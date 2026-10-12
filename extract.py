"""HTML -> main-content text extractors (pluggable so they can be benchmarked)."""
from __future__ import annotations

import html as html_lib
import re
from typing import Callable

import trafilatura

Extractor = Callable[[str, str | None], str | None]

_META_CHARSET = re.compile(rb"""<meta[^>]+charset\s*=\s*["']?\s*([\w\-]+)""", re.I)
_HDR_CHARSET = re.compile(r"charset\s*=\s*([\w\-]+)", re.I)


def decode_html(body: bytes, content_type: str = "") -> str:
    """Decode bytes using header charset, <meta> charset, then UTF-8 / latin-1 fallbacks."""
    candidates: list[str] = []
    m = _HDR_CHARSET.search(content_type or "")
    if m:
        candidates.append(m.group(1))
    m2 = _META_CHARSET.search(body[:4096])
    if m2:
        candidates.append(m2.group(1).decode("ascii", "ignore"))
    candidates += ["utf-8"]
    for enc in candidates:
        try:
            return body.decode(enc)
        except (LookupError, UnicodeDecodeError):
            continue
    return body.decode("latin-1", errors="replace")


def trafilatura_precision(html: str, url: str | None = None) -> str | None:
    return trafilatura.extract(
        html, url=url, favor_precision=True, include_comments=False, include_tables=False
    )


def trafilatura_recall(html: str, url: str | None = None) -> str | None:
    return trafilatura.extract(
        html, url=url, favor_recall=True, include_comments=False, include_tables=True
    )


def trafilatura_baseline(html: str, url: str | None = None) -> str | None:
    _, text, _ = trafilatura.baseline(html)
    return text or None


_SCRIPT_STYLE = re.compile(r"<(script|style|noscript)\b.*?</\1>", re.I | re.S)
_TAGS = re.compile(r"<[^>]+>")
_WS = re.compile(r"[ \t\r\f\v]+")


def naive_strip(html: str, url: str | None = None) -> str | None:
    """Strip tags only (no boilerplate removal) - the lower bound for benchmarks."""
    text = _SCRIPT_STYLE.sub(" ", html)
    text = re.sub(r"</(p|div|li|h\d|br|tr)>", "\n", text, flags=re.I)
    text = html_lib.unescape(_TAGS.sub(" ", text))
    lines = [_WS.sub(" ", ln).strip() for ln in text.splitlines()]
    out = "\n".join(ln for ln in lines if ln)
    return out or None


EXTRACTORS: dict[str, Extractor] = {
    "trafilatura_precision": trafilatura_precision,
    "trafilatura_recall": trafilatura_recall,
    "trafilatura_baseline": trafilatura_baseline,
    "naive_strip": naive_strip,
}


def get_extractor(name: str) -> Extractor:
    try:
        return EXTRACTORS[name]
    except KeyError:
        raise ValueError(f"Unknown extractor {name!r}; options: {sorted(EXTRACTORS)}") from None
