"""Command-line interface: ``crawlpipe <command> ...`` (also ``python -m crawlpipe``)."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from .config import PipelineConfig, load_config


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def _cfg(args) -> PipelineConfig:
    cfg = load_config(getattr(args, "config", None))
    if getattr(args, "num_cpus", None):
        cfg.crawl.num_cpus = args.num_cpus
    if getattr(args, "metrics_port", None):
        cfg.crawl.metrics_port = args.metrics_port
    if getattr(args, "iceberg", False):
        cfg.publish.iceberg = True
    return cfg


def _collect_sources(args) -> list[str]:
    from .warc_io import resolve_cc_paths

    srcs: list[str] = list(args.warc or [])
    if args.warc_list:
        srcs += [ln.strip() for ln in Path(args.warc_list).read_text().splitlines() if ln.strip()]
    if args.cc_crawl:
        srcs += resolve_cc_paths(args.cc_crawl, args.cc_limit)
    if not srcs:
        raise SystemExit("provide --warc, --warc-list or --cc-crawl")
    return srcs


def cmd_gen_sample(args) -> int:
    from .sample_data import generate

    _print(generate(args.out, n_pages=args.pages, n_warcs=args.warcs, seed=args.seed))
    return 0


def cmd_crawl(args) -> int:
    from .crawl import run_warc_crawl

    report = run_warc_crawl(_collect_sources(args), args.out, _cfg(args))
    _print({k: v for k, v in report.items() if k not in ("domain_stats", "shards")})
    return 1 if report["shards_failed"] else 0


def cmd_live_crawl(args) -> int:
    from .crawl import run_live_crawl

    urls = [ln.strip() for ln in Path(args.urls).read_text().splitlines() if ln.strip() and not ln.startswith("#")]
    report = run_live_crawl(urls, args.out, _cfg(args))
    _print({k: v for k, v in report.items() if k not in ("domain_stats", "shards")})
    return 1 if report["shards_failed"] else 0


def cmd_dedup(args) -> int:
    from .dedup import build_spark, dedup

    cfg = _cfg(args)
    run = Path(args.run)
    spark = build_spark(cfg.dedup)
    try:
        _print(dedup(spark, str(run / "raw"), str(run / "dedup" / "docs"), cfg.dedup))
    finally:
        spark.stop()
    return 0


def cmd_publish(args) -> int:
    from .publish import publish_snapshot

    cfg = _cfg(args)
    run = Path(args.run)
    upstream = {}
    for name in ("crawl_report", "dedup/dedup_report"):
        p = run / f"{name}.json"
        if p.exists():
            upstream[Path(name).name] = json.loads(p.read_text())
    upstream.get("crawl_report", {}).pop("domain_stats", None)
    root = args.root or str(run / "corpus")
    manifest = publish_snapshot(run / "dedup" / "docs", root, cfg, upstream)
    _print({k: v for k, v in manifest.items() if k not in ("files", "upstream")})
    return 0


def cmd_analyze(args) -> int:
    from .analyze import analyze_corpus

    run = Path(args.run)
    rep = analyze_corpus(
        args.root or run / "corpus", args.version, run / "crawl_report.json", run / "dedup" / "dedup_report.json"
    )
    (run / "analysis.json").write_text(json.dumps(rep, indent=2))
    _print(rep)
    return 0


def cmd_bench_extract(args) -> int:
    from .benchmark import benchmark_extractors, to_markdown

    rep = benchmark_extractors(args.warc, _cfg(args), args.gold, max_pages=args.max_pages)
    if args.out:
        Path(args.out).mkdir(parents=True, exist_ok=True)
        (Path(args.out) / "extractor_benchmark.json").write_text(json.dumps(rep, indent=2))
        (Path(args.out) / "extractor_benchmark.md").write_text(to_markdown(rep))
    print(to_markdown(rep))
    print(f"\nbest by F1: {rep['best_by_f1']}")
    return 0


def cmd_bench_strategy(args) -> int:
    from .strategies import compare

    rep = compare(budget=args.budget, seeds=args.webs)
    if args.out:
        Path(args.out).mkdir(parents=True, exist_ok=True)
        (Path(args.out) / "strategy_benchmark.json").write_text(json.dumps(rep, indent=2))
    _print(rep)
    return 0


def cmd_versions(args) -> int:
    from .publish import diff_versions, latest_version, list_versions, read_manifest, verify_snapshot

    if args.action == "list":
        _print({"versions": list_versions(args.root), "latest": latest_version(args.root)})
    elif args.action == "show":
        m = read_manifest(args.root, args.version)
        _print({k: v for k, v in m.items() if k != "files"})
    elif args.action == "verify":
        ok = verify_snapshot(args.root, args.version)
        _print({"version": args.version or latest_version(args.root), "intact": ok})
        return 0 if ok else 1
    elif args.action == "diff":
        _print(diff_versions(args.root, args.version, args.other))
    return 0


def cmd_run_all(args) -> int:
    from .analyze import analyze_corpus
    from .crawl import run_warc_crawl
    from .dedup import build_spark, dedup
    from .publish import publish_snapshot
    from .sample_data import generate

    cfg = _cfg(args)
    run = Path(args.out)
    run.mkdir(parents=True, exist_ok=True)
    plan = None
    if args.warc or args.warc_list or args.cc_crawl:
        sources = _collect_sources(args)
    else:
        plan = generate(run / "sample", n_pages=args.pages, n_warcs=args.warcs, seed=args.seed)
        sources = plan["warcs"]

    crawl_rep = run_warc_crawl(sources, str(run), cfg)
    if crawl_rep["shards_failed"]:
        print("crawl had permanently failed shards; aborting", file=sys.stderr)
        return 1
    spark = build_spark(cfg.dedup)
    try:
        dd = dedup(spark, str(run / "raw"), str(run / "dedup" / "docs"), cfg.dedup)
    finally:
        spark.stop()
    upstream = {"crawl_report": {k: v for k, v in crawl_rep.items() if k not in ("domain_stats", "shards")}, "dedup_report": dd}
    manifest = publish_snapshot(run / "dedup" / "docs", run / "corpus", cfg, upstream)
    analysis = analyze_corpus(run / "corpus", None, run / "crawl_report.json", run / "dedup" / "dedup_report.json")
    (run / "analysis.json").write_text(json.dumps(analysis, indent=2))
    summary = {
        "records_seen": crawl_rep["records_seen"],
        "docs_after_filters": crawl_rep["docs_kept"],
        "docs_after_dedup": dd["output_docs"],
        "dedup_removed_fraction": dd["removed_fraction"],
        "published_version": manifest["version"],
        "published_rows": manifest["rows"],
        "approx_tokens": analysis["approx_tokens"],
        "crawl_docs_per_second": crawl_rep["docs_per_second"],
        "planted_duplicates": plan["planted_duplicates"] if plan else None,
    }
    (run / "summary.json").write_text(json.dumps(summary, indent=2))
    _print(summary)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="crawlpipe", description=__doc__)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp, config=True):
        if config:
            sp.add_argument("--config", help="YAML config (see configs/default.yaml)")

    def sources(sp):
        sp.add_argument("--warc", nargs="*", help="local paths or https URLs of WARC(.gz) files")
        sp.add_argument("--warc-list", help="text file with one WARC path/URL per line")
        sp.add_argument("--cc-crawl", help="Common Crawl id, e.g. CC-MAIN-2024-10")
        sp.add_argument("--cc-limit", type=int, default=10, help="max WARC files from --cc-crawl")

    sp = sub.add_parser("gen-sample", help="generate synthetic WARC files + gold labels")
    sp.add_argument("--out", required=True)
    sp.add_argument("--pages", type=int, default=600)
    sp.add_argument("--warcs", type=int, default=3)
    sp.add_argument("--seed", type=int, default=7)
    sp.set_defaults(fn=cmd_gen_sample)

    sp = sub.add_parser("crawl", help="Ray-distributed WARC extraction")
    common(sp); sources(sp)
    sp.add_argument("--out", required=True, help="run directory")
    sp.add_argument("--num-cpus", type=int)
    sp.add_argument("--metrics-port", type=int, help="serve Prometheus /metrics on this port")
    sp.set_defaults(fn=cmd_crawl)

    sp = sub.add_parser("live-crawl", help="polite live fetch of a URL list (robots.txt aware)")
    common(sp)
    sp.add_argument("--urls", required=True, help="text file, one URL per line")
    sp.add_argument("--out", required=True)
    sp.add_argument("--num-cpus", type=int)
    sp.add_argument("--metrics-port", type=int)
    sp.set_defaults(fn=cmd_live_crawl)

    sp = sub.add_parser("dedup", help="exact + MinHash-LSH near-dup removal on Spark")
    common(sp)
    sp.add_argument("--run", required=True)
    sp.set_defaults(fn=cmd_dedup)

    sp = sub.add_parser("publish", help="publish a versioned corpus snapshot")
    common(sp)
    sp.add_argument("--run", required=True)
    sp.add_argument("--root", help="corpus root (default: <run>/corpus)")
    sp.add_argument("--iceberg", action="store_true", help="also append to an Iceberg table")
    sp.set_defaults(fn=cmd_publish)

    sp = sub.add_parser("analyze", help="coverage / redundancy / yield report")
    sp.add_argument("--run", required=True)
    sp.add_argument("--root")
    sp.add_argument("--version")
    sp.set_defaults(fn=cmd_analyze)

    sp = sub.add_parser("bench-extract", help="benchmark extractors (yield, F1, leakage, speed)")
    common(sp)
    sp.add_argument("--warc", nargs="+", required=True)
    sp.add_argument("--gold")
    sp.add_argument("--out")
    sp.add_argument("--max-pages", type=int, default=2000)
    sp.set_defaults(fn=cmd_bench_extract)

    sp = sub.add_parser("bench-strategy", help="compare crawl frontier strategies (simulated)")
    sp.add_argument("--budget", type=int, default=3000)
    sp.add_argument("--webs", type=int, default=5)
    sp.add_argument("--out")
    sp.set_defaults(fn=cmd_bench_strategy)

    sp = sub.add_parser("versions", help="inspect published corpus versions")
    sp.add_argument("action", choices=["list", "show", "verify", "diff"])
    sp.add_argument("--root", required=True)
    sp.add_argument("--version")
    sp.add_argument("--other", help="second version for diff (use --version for the first)")
    sp.set_defaults(fn=cmd_versions)

    sp = sub.add_parser("run-all", help="crawl -> dedup -> publish -> analyze (synthetic data by default)")
    common(sp); sources(sp)
    sp.add_argument("--out", required=True)
    sp.add_argument("--pages", type=int, default=600)
    sp.add_argument("--warcs", type=int, default=3)
    sp.add_argument("--seed", type=int, default=7)
    sp.add_argument("--num-cpus", type=int)
    sp.add_argument("--metrics-port", type=int)
    sp.add_argument("--iceberg", action="store_true")
    sp.set_defaults(fn=cmd_run_all)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
