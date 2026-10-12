import functools
import http.server
import random
import threading

import pytest

from crawlpipe.config import PipelineConfig
from crawlpipe.crawl import run_live_crawl
from crawlpipe.sample_data import BOILER, make_article, to_html
from crawlpipe.strategies import SimWeb, compare, run_strategy


@pytest.fixture()
def local_site(tmp_path):
    rng = random.Random(9)
    site = tmp_path / "site"
    (site / "public").mkdir(parents=True)
    (site / "private").mkdir()
    for i in range(3):
        (site / "public" / f"p{i}.html").write_text(to_html(f"Public {i}", make_article(rng), BOILER))
        (site / "private" / f"s{i}.html").write_text(to_html(f"Secret {i}", make_article(rng), BOILER))
    (site / "robots.txt").write_text("User-agent: *\nDisallow: /private/\n")
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(site))
    handler.log_message = lambda *a, **k: None  # type: ignore[attr-defined]
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


@pytest.mark.slow
def test_live_crawl_honours_robots_and_rate_limit(local_site, tmp_path):
    cfg = PipelineConfig()
    cfg.crawl.num_cpus = 2
    cfg.crawl.min_domain_delay_s = 0.05
    urls = [f"{local_site}/public/p{i}.html" for i in range(3)] + [
        f"{local_site}/private/s{i}.html" for i in range(3)
    ]
    rep = run_live_crawl(urls, str(tmp_path), cfg)
    assert rep["docs_kept"] == 3
    assert rep["skipped_by_reason"]["robots_disallowed"] == 3
    import pyarrow.dataset as ds

    got = ds.dataset(str(tmp_path / "raw"), format="parquet").to_table()["url"].to_pylist()
    assert all("/public/" in u for u in got)


def test_ucb_beats_bfs_and_random_on_harvest_rate():
    rep = compare(budget=2000, seeds=4)
    res = rep["results"]
    assert res["ucb"]["mean_harvest_rate"] > res["bfs"]["mean_harvest_rate"]
    assert res["ucb"]["mean_harvest_rate"] > res["random"]["mean_harvest_rate"]


def test_strategy_runs_are_deterministic_and_respect_budget():
    web = SimWeb(seed=1)
    a = run_strategy(web, "ucb", 500, seed=0)
    b = run_strategy(web, "ucb", 500, seed=0)
    assert a == b and a["fetched"] <= 500
