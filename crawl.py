"""Ray-distributed crawl driver.

Two kinds of shard work, both idempotent and resumable (a ``_done/<shard>.json`` marker is
written after the Parquet part is atomically published):

* ``warc``  - extract documents from Common Crawl / local WARC files
* ``live``  - polite domain-partitioned fetching of specialized sources

The driver bounds in-flight tasks (backpressure), retries failed shards via Ray task
retries, keeps going when a shard permanently fails, and aggregates metrics + a report.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable

from .config import PipelineConfig, from_dict
from .docproc import domain_of, process_page
from .metrics import MetricsRegistry
from .shardio import ShardWriter, read_marker, write_marker
from .warc_io import iter_warc_pages

log = logging.getLogger("crawlpipe.crawl")


def shard_id_for(index: int, source: str) -> str:
    return f"{index:05d}-{hashlib.blake2b(source.encode(), digest_size=4).hexdigest()}"


def _cap_domain_stats(stats: dict[str, list[int]], cap: int) -> dict[str, list[int]]:
    if len(stats) <= cap:
        return stats
    top = sorted(stats.items(), key=lambda kv: kv[1][0], reverse=True)[:cap]
    return dict(top)


def process_warc_shard_impl(shard_id: str, source: str, cfg_dict: dict, out_dir: str) -> dict:
    """Pure-Python shard worker (also runnable without Ray, which keeps it unit-testable)."""
    t0 = time.time()
    done = read_marker(out_dir, shard_id)
    if done:
        return {**done, "status": "skipped"}

    cfg = from_dict(cfg_dict)
    counters: Counter = Counter()
    domain_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    writer = ShardWriter(out_dir, shard_id, cfg.crawl.batch_rows)
    try:
        for page in iter_warc_pages(
            source,
            max_records=cfg.crawl.max_records_per_shard,
            max_body_bytes=cfg.extract.max_html_bytes,
        ):
            dom = domain_of(page.url)
            before = counters["docs_kept"]
            row = process_page(
                url=page.url,
                status=page.status,
                content_type=page.content_type,
                fetched_at=page.fetched_at,
                body=page.body,
                source=source,
                shard_id=shard_id,
                cfg=cfg,
                counters=counters,
            )
            domain_stats[dom][0] += 1
            if row is not None:
                writer.add(row)
                domain_stats[dom][1] += counters["docs_kept"] - before
        out = writer.close()
    except BaseException:
        writer.abort()
        raise

    result = {
        "shard_id": shard_id,
        "source": source,
        "status": "ok",
        "rows": writer.rows,
        "counters": dict(counters),
        "domain_stats": _cap_domain_stats(dict(domain_stats), cfg.crawl.max_domain_stats),
        "out_file": str(out) if out else None,
        "elapsed_s": round(time.time() - t0, 3),
    }
    write_marker(out_dir, shard_id, result)
    return result


def _dispatch(kind: str, shard_id: str, payload, cfg_dict: dict, out_dir: str) -> dict:
    if kind == "warc":
        return process_warc_shard_impl(shard_id, payload, cfg_dict, out_dir)
    if kind == "live":
        from .live import fetch_urls_impl

        return fetch_urls_impl(shard_id, payload, cfg_dict, out_dir)
    raise ValueError(f"unknown job kind {kind!r}")


def _ensure_ray(cfg: PipelineConfig) -> bool:
    """Init Ray if needed. Returns True when this call initialised it (so we should shut down)."""
    import ray

    if ray.is_initialized():
        return False
    ray.init(
        num_cpus=cfg.crawl.num_cpus,
        include_dashboard=False,
        ignore_reinit_error=True,
        logging_level=logging.WARNING,
    )
    return True


def _run_jobs(
    jobs: list[tuple[str, str, object]],
    out_dir: str,
    cfg: PipelineConfig,
    metrics: MetricsRegistry | None,
) -> dict:
    import ray

    metrics = metrics or MetricsRegistry()
    if cfg.crawl.metrics_port:
        metrics.start_http(cfg.crawl.metrics_port)
    owned = _ensure_ray(cfg)
    remote_fn = ray.remote(
        max_retries=cfg.crawl.max_task_retries, retry_exceptions=True, num_cpus=1
    )(_dispatch)
    cfg_dict = cfg.to_dict()

    t0 = time.time()
    pending: dict = {}
    results: list[dict] = []
    failed: list[dict] = []
    todo = iter(jobs)
    exhausted = False
    domain_totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])

    try:
        while pending or not exhausted:
            while not exhausted and len(pending) < cfg.crawl.max_in_flight:
                try:
                    kind, sid, payload = next(todo)
                except StopIteration:
                    exhausted = True
                    break
                ref = remote_fn.remote(kind, sid, payload, cfg_dict, out_dir)
                pending[ref] = (sid, kind)
            if not pending:
                break
            ready, _ = ray.wait(list(pending), num_returns=1)
            for ref in ready:
                sid, _kind = pending.pop(ref)
                try:
                    res = ray.get(ref)
                except Exception as exc:  # noqa: BLE001 - a shard failing for good is data, not a crash
                    log.error("shard %s failed permanently: %s", sid, exc)
                    failed.append({"shard_id": sid, "error": repr(exc)[:500]})
                    metrics.merge({"shards_failed": 1})
                    continue
                results.append(res)
                if res["status"] == "skipped":
                    metrics.merge({"shards_skipped": 1})
                else:
                    metrics.merge(res["counters"])
                    metrics.merge({"shards_done": 1})
                for dom, (seen, kept) in res.get("domain_stats", {}).items():
                    domain_totals[dom][0] += seen
                    domain_totals[dom][1] += kept
                elapsed = max(1e-9, time.time() - t0)
                snap = metrics.snapshot()
                metrics.set_gauge("docs_per_second", snap.get("docs_kept", 0) / elapsed)
                metrics.set_gauge("shards_in_flight", len(pending))
                log.info(
                    "shard %s %s rows=%s | done=%d failed=%d",
                    sid, res["status"], res["rows"], len(results), len(failed),
                )
    finally:
        if owned:
            ray.shutdown()

    elapsed = time.time() - t0
    # Totals cover every shard (including ones resumed from a previous run); throughput is
    # computed from this run's work only so resumed shards do not inflate it.
    totals: Counter = Counter()
    fresh: Counter = Counter()
    for r in results:
        totals.update(r["counters"])
        if r["status"] != "skipped":
            fresh.update(r["counters"])
    rejected = {k.split(":", 1)[1]: int(v) for k, v in totals.items() if k.startswith("rejected:")}
    skipped = {k.split(":", 1)[1]: int(v) for k, v in totals.items() if k.startswith("skipped:")}
    report = {
        "out_dir": str(out_dir),
        "config_fingerprint": cfg.fingerprint(),
        "shards_total": len(jobs),
        "shards_ok": sum(1 for r in results if r["status"] == "ok"),
        "shards_resumed": sum(1 for r in results if r["status"] == "skipped"),
        "shards_failed": failed,
        "records_seen": int(totals["records_seen"]),
        "docs_extracted": int(totals["docs_extracted"]),
        "docs_kept": int(totals["docs_kept"]),
        "yield_ratio": round(totals["docs_kept"] / totals["records_seen"], 4)
        if totals["records_seen"]
        else 0.0,
        "rejected_by_reason": dict(sorted(rejected.items(), key=lambda kv: -kv[1])),
        "skipped_by_reason": dict(sorted(skipped.items(), key=lambda kv: -kv[1])),
        "bytes_in": int(totals["bytes_in"]),
        "chars_out": int(totals["chars_out"]),
        "elapsed_s": round(elapsed, 2),
        "docs_per_second": round(fresh["docs_kept"] / elapsed, 2) if elapsed else 0.0,
        "mb_per_second": round(fresh["bytes_in"] / 1e6 / elapsed, 3) if elapsed else 0.0,
        "domain_stats": dict(
            sorted(domain_totals.items(), key=lambda kv: kv[1][0], reverse=True)[:5000]
        ),
        "shards": [
            {k: r[k] for k in ("shard_id", "source", "status", "rows", "elapsed_s")}
            for r in sorted(results, key=lambda r: r["shard_id"])
        ],
    }
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "crawl_report.json").write_text(json.dumps(report, indent=2))
    metrics.to_json(out / "metrics.json")
    metrics.write_textfile(out / "metrics.prom")
    return report


def run_warc_crawl(
    sources: Iterable[str],
    out_dir: str,
    cfg: PipelineConfig,
    metrics: MetricsRegistry | None = None,
) -> dict:
    jobs = [("warc", shard_id_for(i, s), s) for i, s in enumerate(sources)]
    if not jobs:
        raise ValueError("no WARC sources given")
    return _run_jobs(jobs, str(out_dir), cfg, metrics)


def run_live_crawl(
    urls: Iterable[str],
    out_dir: str,
    cfg: PipelineConfig,
    metrics: MetricsRegistry | None = None,
) -> dict:
    """Partition URLs by domain, then pack whole domains into tasks of <= max_urls_per_task."""
    by_domain: dict[str, list[str]] = defaultdict(list)
    for u in dict.fromkeys(urls):  # de-dup, keep order
        by_domain[domain_of(u)].append(u)
    jobs: list[tuple[str, str, object]] = []
    batch: list[str] = []
    cap = cfg.crawl.max_urls_per_task

    def flush() -> None:
        if batch:
            jobs.append(("live", shard_id_for(len(jobs), "|".join(batch[:3]) + str(len(batch))), list(batch)))
            batch.clear()

    for dom in sorted(by_domain):
        group = by_domain[dom]
        if batch and len(batch) + len(group) > cap:
            flush()
        batch.extend(group)
        if len(batch) >= cap:
            flush()
    flush()
    if not jobs:
        raise ValueError("no URLs given")
    return _run_jobs(jobs, str(out_dir), cfg, metrics)
