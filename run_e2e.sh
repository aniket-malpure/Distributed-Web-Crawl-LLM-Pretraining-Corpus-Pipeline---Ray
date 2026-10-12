#!/usr/bin/env bash
# End-to-end smoke run on synthetic data: crawl -> dedup -> publish (+Iceberg) -> analyze -> verify.
set -euo pipefail
OUT="${1:-runs/e2e}"
rm -rf "$OUT"

crawlpipe run-all --out "$OUT" --pages "${PAGES:-600}" --warcs "${WARCS:-3}" --iceberg
crawlpipe versions list   --root "$OUT/corpus"
crawlpipe versions verify --root "$OUT/corpus"
crawlpipe bench-extract --warc "$OUT"/sample/*.warc.gz --gold "$OUT/sample/gold.jsonl" --out "$OUT/reports"
crawlpipe bench-strategy --budget 3000 --webs 5 --out "$OUT/reports"
echo "done -> $OUT (summary.json, analysis.json, crawl_report.json, metrics.prom, corpus/)"
