"""Corpus deduplication on Spark: exact (content hash) then near-duplicate (MinHash LSH).

Near-duplicate clustering:
  1. mapInPandas computes MinHash signatures and emits one row per (doc, band).
  2. Rows sharing (band, band_hash) form candidate buckets; each member links to the bucket's
     smallest doc_id (a star, so edge count stays linear in docs, not quadratic in bucket size).
  3. Connected components via iterative min-label propagation (DataFrame joins, checkpointed
     each round to keep lineage short).
  4. One survivor per component: the longest document (ties broken by doc_id).
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import DedupConfig
from .minhash import MinHasher, optimal_bands


def build_spark(cfg: DedupConfig, app: str = "crawlpipe-dedup") -> SparkSession:
    # Python workers must run the same interpreter (and thus site-packages, incl. crawlpipe)
    # as the driver; otherwise mapInPandas fails with ModuleNotFoundError.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder.master(cfg.spark_master)
        .appName(app)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _exact_dedup(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("content_hash").orderBy("doc_id")
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _band_rows(df: DataFrame, cfg: DedupConfig, bands: int, rows: int) -> DataFrame:
    num_perm, ngram, seed = cfg.num_perm, cfg.ngram, cfg.seed

    def fn(batches):
        import pandas as pd

        hasher = MinHasher(num_perm, ngram, seed)  # built once per task
        for pdf in batches:
            out_id, out_band, out_hash = [], [], []
            for doc_id, text in zip(pdf["doc_id"], pdf["text"]):
                sig = hasher.signature(text)
                for b, h in enumerate(MinHasher.band_hashes(sig, bands, rows)):
                    out_id.append(doc_id)
                    out_band.append(b)
                    out_hash.append(h)
            yield pd.DataFrame({"doc_id": out_id, "band": out_band, "bhash": out_hash})

    return df.select("doc_id", "text").mapInPandas(fn, "doc_id string, band int, bhash long")


def _components(edges: DataFrame, max_iters: int) -> tuple[DataFrame, int]:
    """Min-label propagation. Returns (DataFrame[doc_id, label], iterations used)."""
    und = edges.select(F.col("src").alias("a"), F.col("dst").alias("b")).union(
        edges.select(F.col("dst").alias("a"), F.col("src").alias("b"))
    ).distinct()
    labels = und.select(F.col("a").alias("doc_id")).distinct().withColumn("label", F.col("doc_id"))
    labels = labels.localCheckpoint(eager=True)
    iters = 0
    for iters in range(1, max_iters + 1):
        msgs = und.join(labels.withColumnRenamed("doc_id", "b").withColumnRenamed("label", "nl"), "b").select(
            F.col("a").alias("doc_id"), F.col("nl").alias("label")
        )
        new = labels.unionByName(msgs).groupBy("doc_id").agg(F.min("label").alias("label"))
        new = new.localCheckpoint(eager=True)
        changed = (
            new.alias("n").join(labels.alias("o"), "doc_id").filter(F.col("n.label") != F.col("o.label")).count()
        )
        labels = new
        if changed == 0:
            break
    return labels, iters


def dedup(
    spark: SparkSession,
    in_path: str,
    out_path: str,
    cfg: DedupConfig,
) -> dict:
    """Deduplicate Parquet docs at ``in_path`` into ``out_path``; return a stats dict."""
    t0 = time.time()
    docs = spark.read.parquet(str(Path(in_path) / "raw") if (Path(in_path) / "raw").exists() else in_path)
    n_in = docs.count()
    if n_in == 0:
        raise ValueError(f"no input documents under {in_path}")

    exact = _exact_dedup(docs).persist()
    n_exact = exact.count()

    bands, rows = optimal_bands(cfg.threshold, cfg.num_perm)
    sig = _band_rows(exact, cfg, bands, rows).persist()
    buckets = (
        sig.groupBy("band", "bhash")
        .agg(F.min("doc_id").alias("rep"), F.count("*").alias("n"))
        .filter("n > 1")
    )
    edges = (
        sig.join(buckets, ["band", "bhash"])
        .select(F.col("doc_id").alias("src"), F.col("rep").alias("dst"))
        .filter("src != dst")
        .distinct()
        .persist()
    )
    n_edges = edges.count()

    if n_edges:
        labels, iters = _components(edges, cfg.max_cc_iters)
        with_label = exact.join(labels, "doc_id", "left").withColumn(
            "_cluster", F.coalesce(F.col("label"), F.col("doc_id"))
        )
        w = Window.partitionBy("_cluster").orderBy(F.col("n_chars").desc(), F.col("doc_id"))
        kept = with_label.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_cluster", "label")
        n_clusters = labels.select("label").distinct().count()
    else:
        kept, iters, n_clusters = exact, 0, 0

    out = Path(out_path)
    if out.exists():
        shutil.rmtree(out)
    kept.write.mode("overwrite").option("compression", "zstd").parquet(str(out))
    n_out = spark.read.parquet(str(out)).count()

    stats = {
        "input_docs": n_in,
        "exact_duplicates_removed": n_in - n_exact,
        "near_duplicates_removed": n_exact - n_out,
        "output_docs": n_out,
        "total_removed": n_in - n_out,
        "removed_fraction": round((n_in - n_out) / n_in, 4),
        "lsh_bands": bands,
        "lsh_rows_per_band": rows,
        "jaccard_threshold": cfg.threshold,
        "candidate_edges": n_edges,
        "near_dup_clusters": n_clusters,
        "cc_iterations": iters,
        "elapsed_s": round(time.time() - t0, 2),
    }
    (out.parent / "dedup_report.json").write_text(json.dumps(stats, indent=2))
    exact.unpersist()
    sig.unpersist()
    edges.unpersist()
    return stats
