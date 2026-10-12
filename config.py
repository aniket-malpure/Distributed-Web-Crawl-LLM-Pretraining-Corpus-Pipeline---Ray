"""Typed pipeline configuration, loadable from YAML with partial overrides."""
from __future__ import annotations

import dataclasses
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ExtractConfig:
    extractor: str = "trafilatura_precision"  # see extract.EXTRACTORS
    min_chars: int = 200
    max_html_bytes: int = 5_000_000


@dataclass
class QualityConfig:
    target_langs: list[str] = field(default_factory=lambda: ["en"])
    min_lang_conf: float = 0.5
    min_words: int = 50
    max_words: int = 100_000
    min_mean_word_len: float = 3.0
    max_mean_word_len: float = 10.0
    max_symbol_word_ratio: float = 0.1
    min_alpha_word_frac: float = 0.8
    min_stopwords: int = 2
    max_bullet_line_frac: float = 0.9
    max_ellipsis_line_frac: float = 0.3
    max_dup_line_frac: float = 0.30
    max_dup_line_char_frac: float = 0.30
    max_dup_para_frac: float = 0.30
    max_dup_para_char_frac: float = 0.30


@dataclass
class CrawlConfig:
    num_cpus: int | None = None  # None -> all local CPUs / existing Ray cluster
    max_in_flight: int = 8  # concurrent shard tasks (backpressure)
    max_task_retries: int = 3
    batch_rows: int = 2000  # rows per Parquet row group
    max_records_per_shard: int | None = None
    max_domain_stats: int = 500  # per-shard cap on per-domain yield stats
    metrics_port: int | None = None  # expose Prometheus /metrics on the driver
    # live-crawl politeness
    user_agent: str = "crawlpipe/0.1 (+mailto:crawler-admin@example.com)"
    min_domain_delay_s: float = 1.0
    request_timeout_s: float = 15.0
    max_urls_per_task: int = 200
    respect_robots: bool = True


@dataclass
class DedupConfig:
    num_perm: int = 128
    threshold: float = 0.8  # target Jaccard similarity for near-duplicates
    ngram: int = 5  # word shingle size
    seed: int = 1
    max_cc_iters: int = 30  # connected-components iteration cap
    spark_master: str = "local[*]"
    shuffle_partitions: int = 16
    driver_memory: str = "3g"


@dataclass
class PublishConfig:
    root: str = "corpus"
    iceberg: bool = False
    max_rows_per_file: int = 250_000
    max_rows_per_group: int = 50_000
    partition_by_lang: bool = True


@dataclass
class PipelineConfig:
    extract: ExtractConfig = field(default_factory=ExtractConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)
    crawl: CrawlConfig = field(default_factory=CrawlConfig)
    dedup: DedupConfig = field(default_factory=DedupConfig)
    publish: PublishConfig = field(default_factory=PublishConfig)

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    def fingerprint(self) -> str:
        blob = json.dumps(self.to_dict(), sort_keys=True).encode()
        return hashlib.sha256(blob).hexdigest()[:16]


def _build(cls: type, data: dict[str, Any] | None):
    data = data or {}
    valid = {f.name for f in dataclasses.fields(cls)}
    unknown = set(data) - valid
    if unknown:
        raise ValueError(f"Unknown keys for {cls.__name__}: {sorted(unknown)}")
    return cls(**data)


def from_dict(data: dict[str, Any] | None) -> PipelineConfig:
    data = data or {}
    unknown = set(data) - {"extract", "quality", "crawl", "dedup", "publish"}
    if unknown:
        raise ValueError(f"Unknown top-level config sections: {sorted(unknown)}")
    return PipelineConfig(
        extract=_build(ExtractConfig, data.get("extract")),
        quality=_build(QualityConfig, data.get("quality")),
        crawl=_build(CrawlConfig, data.get("crawl")),
        dedup=_build(DedupConfig, data.get("dedup")),
        publish=_build(PublishConfig, data.get("publish")),
    )


def load_config(path: str | Path | None) -> PipelineConfig:
    if path is None:
        return PipelineConfig()
    with open(path, "r", encoding="utf-8") as fh:
        return from_dict(yaml.safe_load(fh))
