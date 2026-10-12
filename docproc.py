"""Single-document processing shared by the WARC and live-crawl paths."""
from __future__ import annotations

import hashlib
import re
import time
from collections import Counter
from urllib.parse import urlparse

import pyarrow as pa

from . import langid
from .config import PipelineConfig
from .extract import decode_html, get_extractor
from .quality import assess

DOC_SCHEMA = pa.schema(
    [
        ("doc_id", pa.string()),
        ("url", pa.string()),
        ("domain", pa.string()),
        ("source", pa.string()),
        ("fetched_at", pa.string()),
        ("http_status", pa.int32()),
        ("extractor", pa.string()),
        ("text", pa.string()),
        ("n_chars", pa.int32()),
        ("n_words", pa.int32()),
        ("mean_word_len", pa.float32()),
        ("lang", pa.string()),
        ("lang_conf", pa.float32()),
        ("content_hash", pa.string()),
        ("shard_id", pa.string()),
    ]
)

_WS = re.compile(r"\s+")


def normalize_for_hash(text: str) -> str:
    return _WS.sub(" ", text.lower()).strip()


def content_hash(text: str) -> str:
    return hashlib.sha256(normalize_for_hash(text).encode("utf-8")).hexdigest()


def doc_id_for(url: str, text_hash: str) -> str:
    """128-bit stable id. Includes the content hash so re-crawls with new content get new ids."""
    return hashlib.blake2b(f"{url}\x00{text_hash}".encode(), digest_size=16).hexdigest()


def domain_of(url: str) -> str:
    host = urlparse(url).netloc.lower().split("@")[-1].split(":")[0]
    return host[4:] if host.startswith("www.") else host


def is_html(content_type: str, body: bytes) -> bool:
    ct = (content_type or "").lower()
    if "html" in ct:  # text/html, application/xhtml+xml
        return True
    if not ct:  # sniff
        head = body[:512].lstrip().lower()
        return head.startswith((b"<!doctype html", b"<html"))
    return False


def process_page(
    *,
    url: str,
    status: int,
    content_type: str,
    fetched_at: str,
    body: bytes,
    source: str,
    shard_id: str,
    cfg: PipelineConfig,
    counters: Counter,
) -> dict | None:
    """Run one fetched page through extraction + language-ID + quality filters.

    Returns a row dict matching ``DOC_SCHEMA`` or ``None`` (reason recorded in counters).
    Never raises for bad content; unexpected extractor errors are counted and skipped.
    """
    counters["records_seen"] += 1
    counters["bytes_in"] += len(body)

    if status != 200:
        counters["skipped:http_status"] += 1
        return None
    if not is_html(content_type, body):
        counters["skipped:non_html"] += 1
        return None
    if len(body) > cfg.extract.max_html_bytes:
        counters["skipped:too_large"] += 1
        return None

    extractor = get_extractor(cfg.extract.extractor)
    t0 = time.perf_counter()
    try:
        text = extractor(decode_html(body, content_type), url)
    except Exception:  # noqa: BLE001 - extractor bugs must not kill a shard
        counters["skipped:extract_error"] += 1
        return None
    counters["extract_time_us"] += int((time.perf_counter() - t0) * 1e6)

    if not text or len(text) < cfg.extract.min_chars:
        counters["rejected:too_short_after_extract"] += 1
        return None
    counters["docs_extracted"] += 1

    lang, conf = langid.identify(text)
    if cfg.quality.target_langs and (
        lang not in cfg.quality.target_langs or conf < cfg.quality.min_lang_conf
    ):
        counters["rejected:language"] += 1
        return None

    reason, feats = assess(text, cfg.quality, lang)
    if reason:
        counters[f"rejected:{reason}"] += 1
        return None

    h = content_hash(text)
    counters["docs_kept"] += 1
    counters["chars_out"] += len(text)
    return {
        "doc_id": doc_id_for(url, h),
        "url": url,
        "domain": domain_of(url),
        "source": source,
        "fetched_at": fetched_at,
        "http_status": status,
        "extractor": cfg.extract.extractor,
        "text": text,
        "n_chars": len(text),
        "n_words": int(feats["n_words"]),
        "mean_word_len": float(feats.get("mean_word_len", 0.0)),
        "lang": lang,
        "lang_conf": float(conf),
        "content_hash": h,
        "shard_id": shard_id,
    }
