"""WARC reading for local files and Common Crawl (HTTP) sources."""
from __future__ import annotations

import gzip
import io
from dataclasses import dataclass
from typing import Iterator
from urllib.parse import urlparse

CC_BASE = "https://data.commoncrawl.org/"


@dataclass
class WarcPage:
    url: str
    status: int
    content_type: str
    fetched_at: str
    body: bytes
    source: str


def is_remote(src: str) -> bool:
    return src.startswith(("http://", "https://"))


def resolve_cc_paths(crawl_id: str, limit: int | None = None, timeout: float = 60.0) -> list[str]:
    """Return full URLs of WARC files for a Common Crawl snapshot (e.g. CC-MAIN-2024-10)."""
    import requests

    url = f"{CC_BASE}crawl-data/{crawl_id}/warc.paths.gz"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    lines = gzip.decompress(resp.content).decode().split()
    if limit:
        lines = lines[:limit]
    return [CC_BASE + p for p in lines]


def _open_stream(src: str, timeout: float = 120.0):
    if is_remote(src):
        import requests

        resp = requests.get(src, stream=True, timeout=timeout)
        resp.raise_for_status()
        resp.raw.decode_content = False  # keep gzip bytes; warcio handles member decompression
        return resp.raw, resp
    return open(src, "rb"), None


def iter_warc_pages(
    src: str,
    max_records: int | None = None,
    max_body_bytes: int = 5_000_000,
) -> Iterator[WarcPage]:
    """Yield HTTP *response* records from a WARC(.gz). Bad records are skipped, not fatal."""
    from warcio.archiveiterator import ArchiveIterator

    stream, resp = _open_stream(src)
    n = 0
    try:
        for rec in ArchiveIterator(stream):
            if rec.rec_type != "response" or rec.http_headers is None:
                continue
            url = rec.rec_headers.get_header("WARC-Target-URI") or ""
            if not urlparse(url).netloc:
                continue
            try:
                status = int(rec.http_headers.get_statuscode())
            except (TypeError, ValueError):
                status = 0
            ctype = rec.http_headers.get_header("Content-Type") or ""
            body = rec.content_stream().read(max_body_bytes + 1)
            yield WarcPage(
                url=url,
                status=status,
                content_type=ctype,
                fetched_at=rec.rec_headers.get_header("WARC-Date") or "",
                body=body[:max_body_bytes],
                source=src,
            )
            n += 1
            if max_records and n >= max_records:
                break
    finally:
        if resp is not None:
            resp.close()
        else:
            stream.close()


def write_warc(path: str, pages: list[tuple[str, int, str, bytes, str]]) -> None:
    """Write (url, status, content_type, body, date_iso) tuples to a gzipped WARC."""
    from warcio.statusandheaders import StatusAndHeaders
    from warcio.warcwriter import WARCWriter

    with open(path, "wb") as fh:
        writer = WARCWriter(fh, gzip=True)
        for url, status, ctype, body, date in pages:
            http_headers = StatusAndHeaders(
                f"{status} {'OK' if status == 200 else 'ERR'}",
                [("Content-Type", ctype), ("Content-Length", str(len(body)))],
                protocol="HTTP/1.1",
            )
            rec = writer.create_warc_record(
                url,
                "response",
                payload=io.BytesIO(body),
                http_headers=http_headers,
                warc_headers_dict={"WARC-Date": date} if date else None,
            )
            writer.write_record(rec)
