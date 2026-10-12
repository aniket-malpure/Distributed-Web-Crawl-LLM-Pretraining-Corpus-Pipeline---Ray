PY ?= python

.PHONY: install test demo bench docker clean

install:
	$(PY) -m pip install -e ".[iceberg,dev]"

test:
	$(PY) -m pytest

demo:
	crawlpipe run-all --out runs/demo --pages 600 --warcs 3 --iceberg

bench:
	crawlpipe gen-sample --out data/sample --pages 2000 --warcs 4
	crawlpipe bench-extract --warc data/sample/*.warc.gz --gold data/sample/gold.jsonl --out reports
	crawlpipe bench-strategy --budget 3000 --webs 5 --out reports

docker:
	docker build -t crawlpipe .

clean:
	rm -rf runs data reports .pytest_cache spark-warehouse metastore_db derby.log
