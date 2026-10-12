"""Corpus analysis: coverage gaps, redundancy, concentration, and yield by domain."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pyarrow.compute as pc

from .publish import open_dataset


def analyze_corpus(
    root: str | Path,
    version: str | None = None,
    crawl_report: str | Path | None = None,
    dedup_report: str | Path | None = None,
) -> dict:
    table = open_dataset(root, version).to_table(columns=["domain", "lang", "n_words", "n_chars"])
    n_docs = table.num_rows
    words = table["n_words"].to_pylist()
    total_words = sum(words)
    dom_counts = Counter(table["domain"].to_pylist())
    top = dom_counts.most_common(10)
    shares = [c / n_docs for c in dom_counts.values()]
    hhi = sum(s * s for s in shares)  # 1/N (uniform) .. 1 (single domain)
    q = pc.quantile(table["n_words"], q=[0.1, 0.5, 0.9, 0.99]).to_pylist()

    report: dict = {
        "docs": n_docs,
        "total_words": total_words,
        "approx_tokens": int(total_words * 1.3),
        "domains": len(dom_counts),
        "top10_domain_share": round(sum(c for _, c in top) / n_docs, 4) if n_docs else 0,
        "domain_hhi": round(hhi, 4),
        "top_domains": [{"domain": d, "docs": c} for d, c in top],
        "language_mix": dict(Counter(table["lang"].to_pylist()).most_common()),
        "words_per_doc_quantiles": dict(zip(["p10", "p50", "p90", "p99"], q)),
        "gaps": [],
    }

    if report["top10_domain_share"] > 0.5:
        report["gaps"].append(
            f"Top-10 domains hold {report['top10_domain_share']:.0%} of docs: add sources / cap per-domain."
        )
    if len(report["language_mix"]) == 1:
        report["gaps"].append("Single-language corpus: multilingual coverage is zero.")

    if crawl_report and Path(crawl_report).exists():
        cr = json.loads(Path(crawl_report).read_text())
        report["crawl"] = {
            "records_seen": cr["records_seen"],
            "yield_ratio": cr["yield_ratio"],
            "top_rejections": dict(list(cr["rejected_by_reason"].items())[:5]),
            "top_skips": dict(list(cr["skipped_by_reason"].items())[:5]),
        }
        # Domains with plenty of fetches but poor yield: candidates for extractor tuning
        # or a specialised crawler.
        low = [
            {"domain": d, "seen": s, "kept": k, "yield": round(k / s, 3)}
            for d, (s, k) in cr.get("domain_stats", {}).items()
            if s >= 10 and k / s < 0.25
        ]
        report["low_yield_domains"] = sorted(low, key=lambda r: -r["seen"])[:20]
        if report["low_yield_domains"]:
            report["gaps"].append(
                f"{len(report['low_yield_domains'])} high-volume domains yield <25%: inspect extraction."
            )

    if dedup_report and Path(dedup_report).exists():
        dr = json.loads(Path(dedup_report).read_text())
        report["redundancy"] = {
            "exact_removed": dr["exact_duplicates_removed"],
            "near_removed": dr["near_duplicates_removed"],
            "removed_fraction": dr["removed_fraction"],
        }
    return report
