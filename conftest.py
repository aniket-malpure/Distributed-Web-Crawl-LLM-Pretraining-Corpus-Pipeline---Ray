import os
import sys

import pytest

from crawlpipe.config import PipelineConfig
from crawlpipe.sample_data import generate

# Spark python workers must use this interpreter (so they can import crawlpipe).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def sample(tmp_path_factory):
    d = tmp_path_factory.mktemp("sample")
    return generate(d, n_pages=300, n_warcs=3, seed=3)


@pytest.fixture()
def cfg():
    c = PipelineConfig()
    c.crawl.num_cpus = 2
    c.crawl.max_in_flight = 2
    c.dedup.shuffle_partitions = 4
    c.dedup.driver_memory = "1g"
    return c


@pytest.fixture(scope="session")
def spark():
    from crawlpipe.config import DedupConfig
    from crawlpipe.dedup import build_spark

    s = build_spark(DedupConfig(shuffle_partitions=4, driver_memory="1g"), app="tests")
    yield s
    s.stop()
