#!/usr/bin/env bash
# Run the pipeline on real Common Crawl WARC files (needs outbound HTTPS to data.commoncrawl.org).
#   usage: scripts/run_common_crawl.sh CC-MAIN-2024-10 10 runs/cc
set -euo pipefail
CRAWL="${1:?crawl id, e.g. CC-MAIN-2024-10}"
LIMIT="${2:-5}"
OUT="${3:-runs/cc}"
CFG="${CFG:-configs/default.yaml}"

crawlpipe crawl   --cc-crawl "$CRAWL" --cc-limit "$LIMIT" --out "$OUT" --config "$CFG" --metrics-port "${METRICS_PORT:-9100}"
crawlpipe dedup   --run "$OUT" --config "$CFG"
crawlpipe publish --run "$OUT" --config "$CFG" --iceberg
crawlpipe analyze --run "$OUT"
