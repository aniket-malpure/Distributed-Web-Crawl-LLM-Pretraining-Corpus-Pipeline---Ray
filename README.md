# crawlpipe: Distributed Web Crawl & LLM Pretraining Corpus Pipeline (Ray)

End-to-end ingestion for pre-training corpora: **acquire → extract → filter → deduplicate → version → analyze**, with experiments to decide *how* to acquire.

```
 Common Crawl WARCs ─┐                                              ┌─► Parquet snapshots (hive by lang)
 local WARCs ────────┼─► Ray tasks ─► Trafilatura ─► langid ─► quality ─► Parquet shards ─► Spark MinHash-LSH ─┤   vNNNN/ + manifest.json + LATEST
 live URL lists ─────┘  (retries,     extraction     filter    heuristics   (atomic, resumable)   exact + near dedup    └─► Iceberg table (time travel)
 (robots.txt, polite)   backpressure)                                                                         ▼
                                              Prometheus /metrics · crawl_report.json · analysis.json (gaps, redundancy, concentration)
```

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[iceberg,dev]"          # Java 17+ is required for Spark

# 1. Everything on synthetic data (no network needed): ~40 s on a laptop
crawlpipe run-all --out runs/demo --pages 600 --warcs 3 --iceberg

# 2. Tests (17 tests: unit + Ray + Spark + Iceberg + live crawl)
pytest
```

`run-all` prints a summary like (synthetic data, seed 7):

```
records_seen 600 → docs_after_filters 451 → docs_after_dedup 310  (141 of 144 planted duplicates removed)
```

### Run on real Common Crawl

```bash
# 10 WARC files (~1 GB each) from a snapshot; needs outbound HTTPS to data.commoncrawl.org
crawlpipe crawl --cc-crawl CC-MAIN-2024-10 --cc-limit 10 --out runs/cc --config configs/default.yaml --metrics-port 9100
crawlpipe dedup   --run runs/cc --config configs/default.yaml
crawlpipe publish --run runs/cc --iceberg
crawlpipe analyze --run runs/cc
```

Multi-node: start a Ray cluster (`ray start --head` / `ray start --address=...`) and run the same commands; `ray.init()` attaches to the existing cluster. Point `--run` at shared storage (S3 via a mounted path / FUSE, or any path all workers see) and set `dedup.spark_master` to your Spark master.

## What each stage does

| Stage | Module | Notes |
|---|---|---|
| WARC I/O | `warc_io.py` | Streams local or HTTPS WARC(.gz) with `warcio`; `--cc-crawl` resolves `warc.paths.gz`. Bad records are skipped, not fatal. |
| Ray crawl | `crawl.py` | One task per shard, bounded in-flight tasks (backpressure), Ray task retries, permanently failed shards recorded without killing the run, **atomic Parquet publish + `_done` markers so reruns resume**. |
| Extraction | `extract.py` | Trafilatura (precision / recall / baseline) and a naive tag-stripper; charset sniffing; pluggable via `extract.extractor`. |
| Filtering | `langid.py`, `quality.py` | Offline stopword language ID; Gopher/C4-style rules (length, word length, symbol ratio, bullet/ellipsis lines, duplicate lines/paragraphs/n-grams). Every rejection is counted by reason. |
| Live crawler | `live.py` | For specialized, high-priority sources. Domain-partitioned tasks, robots.txt (incl. `Crawl-delay`), per-domain rate limit, size cap, 5xx-robots ⇒ skip. |
| Dedup | `dedup.py`, `minhash.py` | Spark: exact content hash, then MinHash (128 perms, 5-word shingles) + LSH banding auto-tuned to the Jaccard threshold, connected components by iterative label propagation, keep the longest doc per cluster. Hashing is deterministic across processes (no salted `hash()`). |
| Delivery | `publish.py` | Immutable `snapshots/vNNNN/` with `manifest.json` (rows, per-file SHA-256, schema + config fingerprints, parent, upstream reports), `LATEST` pointer, `verify`, `diff`, optional Iceberg append with version→snapshot mapping for time travel. |
| Observability | `metrics.py` | Prometheus `/metrics` on the driver (`--metrics-port`), `metrics.prom` textfile, `metrics.json`, `crawl_report.json`. |
| Experiments | `benchmark.py`, `strategies.py` | Extractor benchmark (yield, token precision/recall/F1, boilerplate leakage, pages/s). Crawl-frontier experiment (BFS vs random vs UCB-over-domains) on a simulated web. |
| Analysis | `analyze.py` | Domain concentration (top-10 share, HHI), language mix, length quantiles, low-yield high-volume domains, dedup redundancy, auto-generated "gaps". |

## Experiments

```bash
crawlpipe gen-sample --out data/sample --pages 2000 --warcs 4
crawlpipe bench-extract --warc data/sample/*.warc.gz --gold data/sample/gold.jsonl --out reports/
crawlpipe bench-strategy --budget 3000 --webs 5 --out reports/
```

Measured in this repo (synthetic data / simulation, so treat as a harness check, not a result about the real web):

* **Extractors** (gold labels known): naive tag-stripping leaks 100% of the planted boilerplate; Trafilatura leaks under 1%. On the synthetic pages the simple `trafilatura_baseline` scores the highest F1 because the markup is clean `<article><p>` HTML; run the same benchmark on real Common Crawl WARCs (omit `--gold` to compare yield and speed, or hand-label a sample) before choosing an extractor.
* **Crawl frontier** (budget 3000 fetches, 5 simulated webs): UCB-over-domains harvest rate 0.447 vs 0.274 for BFS (+63%) and 0.287 for random.
* **Dedup** (600-page synthetic run, 144 planted duplicates): 141 removed (97.9% recall); the misses are near-duplicates whose true Jaccard sits just above the threshold, where LSH is probabilistic. Lower `dedup.threshold` or raise `num_perm` for higher recall at the cost of more candidate pairs.

## Scaling notes (multi-TB → PB)

* Shards are independent, so crawl throughput scales with Ray workers; `crawl.max_in_flight` bounds memory and request pressure.
* Parquet shards are zstd-compressed with bounded row groups; reads/writes are streaming, never whole-shard in memory.
* Dedup cost is dominated by signature computation (CPU, embarrassingly parallel via `mapInPandas`) and one shuffle on `(band, hash)`; the star-shaped edge list keeps connected components linear in documents. At PB scale: raise `dedup.shuffle_partitions`, give Spark a real cluster, and consider 128-bit document ids (already used) to avoid collisions.
* `doc_id` is a 128-bit BLAKE2b of `url + content hash`; the same URL with new content gets a new id, so snapshot diffs show real content changes.
* Language ID here is a deliberately simple offline heuristic. For production swap `langid.identify` for fastText `lid.176` (same `(lang, confidence)` contract).
* The quality filters are conventional heuristics. Treat thresholds as hypotheses and tune them against downstream model evals.

## Responsible crawling

The live crawler honours robots.txt and `Crawl-delay`, identifies itself with a configurable `crawl.user_agent` (set a real contact address in your config), rate-limits per domain, and caps response size. Check each site's terms and the licenses of data you collect before using it for training.

## Layout

```
src/crawlpipe/   package (cli.py is the entry point)
tests/           pytest suite (slow-marked tests start Ray / Spark)
configs/         default.yaml with every tunable
scripts/         run_e2e.sh, run_common_crawl.sh
Dockerfile · Makefile · .github/workflows/ci.yml
```
