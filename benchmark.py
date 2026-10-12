"""Extractor benchmark: yield, throughput, and (with gold labels) token-F1 + boilerplate leakage."""
from __future__ import annotations

import json
import re
import time
from collections import Counter
from pathlib import Path

from .config import PipelineConfig
from .extract import EXTRACTORS, decode_html
from .docproc import is_html
from .warc_io import iter_warc_pages

_TOK = re.compile(r"\w+", re.U)


def _tokens(text: str) -> Counter:
    return Counter(_TOK.findall(text.lower()))


def token_f1(pred: str, gold: str) -> tuple[float, float, float]:
    p, g = _tokens(pred), _tokens(gold)
    overlap = sum((p & g).values())
    if not p or not g or not overlap:
        return 0.0, 0.0, 0.0
    prec, rec = overlap / sum(p.values()), overlap / sum(g.values())
    return prec, rec, 2 * prec * rec / (prec + rec)


def load_gold(path: str | Path | None) -> dict[str, dict]:
    if not path or not Path(path).exists():
        return {}
    out = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            out[rec["url"]] = rec
    return out


def benchmark_extractors(
    warc_paths: list[str],
    cfg: PipelineConfig,
    gold_path: str | Path | None = None,
    names: list[str] | None = None,
    max_pages: int | None = 2000,
) -> dict:
    names = names or list(EXTRACTORS)
    gold = load_gold(gold_path)
    pages = []
    for p in warc_paths:
        for page in iter_warc_pages(p, max_body_bytes=cfg.extract.max_html_bytes):
            if page.status == 200 and is_html(page.content_type, page.body):
                pages.append((page.url, decode_html(page.body, page.content_type)))
            if max_pages and len(pages) >= max_pages:
                break
    results: dict[str, dict] = {}
    for name in names:
        fn = EXTRACTORS[name]
        t0 = time.perf_counter()
        outs = []
        for url, html in pages:
            try:
                outs.append(fn(html, url) or "")
            except Exception:  # noqa: BLE001
                outs.append("")
        elapsed = time.perf_counter() - t0
        yielded = [o for o in outs if len(o) >= cfg.extract.min_chars]
        row = {
            "pages": len(pages),
            "yield": round(len(yielded) / max(1, len(pages)), 4),
            "mean_chars": round(sum(map(len, outs)) / max(1, len(outs)), 1),
            "pages_per_second": round(len(pages) / elapsed, 1) if elapsed else 0.0,
        }
        if gold:
            precs, recs, f1s, leaks = [], [], [], []
            for (url, _), out in zip(pages, outs):
                g = gold.get(url)
                if not g or g["label"] == "non_html" or not g["body"]:
                    continue
                pr, rc, f1 = token_f1(out, g["body"])
                precs.append(pr), recs.append(rc), f1s.append(f1)
                phrases = g.get("boilerplate", [])
                leaks.append(sum(1 for ph in phrases if ph in out) / max(1, len(phrases)))
            n = max(1, len(f1s))
            row.update(
                precision=round(sum(precs) / n, 4),
                recall=round(sum(recs) / n, 4),
                f1=round(sum(f1s) / n, 4),
                boilerplate_leakage=round(sum(leaks) / max(1, len(leaks)), 4),
                scored_pages=len(f1s),
            )
        results[name] = row
    best = max(results, key=lambda k: results[k].get("f1", results[k]["yield"]))
    return {"results": results, "best_by_f1": best if gold else None, "pages": len(pages)}


def to_markdown(report: dict) -> str:
    rows = report["results"]
    cols = ["yield", "precision", "recall", "f1", "boilerplate_leakage", "pages_per_second", "mean_chars"]
    cols = [c for c in cols if any(c in r for r in rows.values())]
    lines = ["| extractor | " + " | ".join(cols) + " |", "|---|" + "---|" * len(cols)]
    for name, r in rows.items():
        lines.append(f"| {name} | " + " | ".join(str(r.get(c, "")) for c in cols) + " |")
    return "\n".join(lines)
