import json
from pathlib import Path

import pyarrow.dataset as ds
import pytest

from crawlpipe.crawl import process_warc_shard_impl, run_warc_crawl
from crawlpipe.dedup import dedup
from crawlpipe.publish import (
    diff_versions,
    latest_version,
    list_versions,
    open_dataset,
    publish_snapshot,
    read_manifest,
    verify_snapshot,
)
from crawlpipe.warc_io import iter_warc_pages


def test_warc_roundtrip(sample):
    pages = [p for w in sample["warcs"] for p in iter_warc_pages(w)]
    assert len(pages) == sample["pages"]
    assert any(p.status == 404 or p.status >= 500 for p in pages)


def test_shard_impl_is_idempotent_and_atomic(sample, cfg, tmp_path):
    first = process_warc_shard_impl("s0", sample["warcs"][0], cfg.to_dict(), str(tmp_path))
    assert first["status"] == "ok" and first["rows"] > 0
    assert not list((tmp_path / "raw").glob(".part-*.tmp"))
    again = process_warc_shard_impl("s0", sample["warcs"][0], cfg.to_dict(), str(tmp_path))
    assert again["status"] == "skipped" and again["rows"] == first["rows"]


def test_shard_failure_leaves_no_partial_output(cfg, tmp_path):
    with pytest.raises(Exception):
        process_warc_shard_impl("bad", str(tmp_path / "missing.warc.gz"), cfg.to_dict(), str(tmp_path))
    raw = tmp_path / "raw"
    assert not list(raw.glob("part-bad*")) and not list(raw.glob(".part-bad*"))


@pytest.mark.slow
def test_ray_crawl_filters_exactly_what_was_planted(sample, cfg, tmp_path):
    rep = run_warc_crawl(sample["warcs"], str(tmp_path), cfg)
    p = sample["planted"]
    assert rep["shards_failed"] == [] and rep["shards_ok"] == 3
    assert rep["records_seen"] == sample["pages"]
    # every good page and every planted duplicate passes the filters; everything else is rejected
    assert rep["docs_kept"] == p["good"] + sample["planted_duplicates"]
    assert rep["skipped_by_reason"]["non_html"] == p["non_html"]
    assert rep["skipped_by_reason"]["http_status"] == p["http_error"]
    assert rep["rejected_by_reason"]["language"] >= p["foreign"] * 0.9
    assert (tmp_path / "metrics.prom").read_text().count("crawlpipe_docs_kept_total") >= 1

    # resume: nothing is recomputed, totals preserved
    rep2 = run_warc_crawl(sample["warcs"], str(tmp_path), cfg)
    assert rep2["shards_resumed"] == 3 and rep2["docs_kept"] == rep["docs_kept"]


@pytest.mark.slow
def test_dedup_removes_planted_duplicates(sample, cfg, spark, tmp_path):
    run_warc_crawl(sample["warcs"], str(tmp_path), cfg)
    stats = dedup(spark, str(tmp_path / "raw"), str(tmp_path / "dedup" / "docs"), cfg.dedup)
    planted = sample["planted_duplicates"]
    removed = stats["total_removed"]
    # recall of planted duplicates (LSH is probabilistic) and no over-removal of distinct docs
    assert removed >= 0.9 * planted
    assert removed <= planted + 2
    assert stats["output_docs"] + removed == stats["input_docs"]
    out = ds.dataset(str(tmp_path / "dedup" / "docs"), format="parquet").to_table()
    assert len(set(out["content_hash"].to_pylist())) == out.num_rows  # no exact dups remain

    # publish two versions and check manifest / verify / diff / pinning
    root = tmp_path / "corpus"
    m1 = publish_snapshot(tmp_path / "dedup" / "docs", root, cfg, {"dedup_report": stats})
    m2 = publish_snapshot(tmp_path / "dedup" / "docs", root, cfg)
    assert list_versions(root) == ["v0001", "v0002"] and latest_version(root) == "v0002"
    assert m1["rows"] == stats["output_docs"] and m2["parent"] == "v0001"
    assert verify_snapshot(root, "v0001")
    assert open_dataset(root, "v0001").count_rows() == m1["rows"]
    assert diff_versions(root, "v0001", "v0002") == {
        "from": "v0001", "to": "v0002", "added": 0, "removed": 0, "unchanged": m1["rows"],
    }
    # corrupting a data file must be detected
    victim = next((root / "snapshots" / "v0001" / "data").rglob("*.parquet"))
    victim.write_bytes(victim.read_bytes()[:-8] + b"corrupt!")
    assert not verify_snapshot(root, "v0001")
    assert json.loads((root / "snapshots" / "v0002" / "manifest.json").read_text())["version"] == "v0002"
    assert read_manifest(root)["version"] == "v0002"


@pytest.mark.slow
def test_iceberg_publish_and_time_travel(sample, cfg, tmp_path):
    pytest.importorskip("pyiceberg")
    from crawlpipe.publish import iceberg_time_travel

    cfg.publish.iceberg = True
    run_warc_crawl(sample["warcs"][:1], str(tmp_path), cfg)
    docs = tmp_path / "raw"
    m1 = publish_snapshot(docs, tmp_path / "corpus", cfg)
    m2 = publish_snapshot(docs, tmp_path / "corpus", cfg)
    assert m1["iceberg"]["rows"] == m1["rows"]
    assert iceberg_time_travel(tmp_path / "corpus", "v0001").num_rows == m1["rows"]
    assert iceberg_time_travel(tmp_path / "corpus", "v0002").num_rows == m1["rows"] + m2["rows"]
