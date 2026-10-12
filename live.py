"""Polite live fetcher for specialized, high-priority sources.

Work is partitioned *by domain* in the driver, so per-domain rate limiting and robots.txt
caching are local to a task (no cross-task coordination needed).
"""
from __future__ import annotations

import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

from .config import from_dict
from .docproc import domain_of, process_page
from .shardio import ShardWriter, read_marker, write_marker


class _Robots:
    def __init__(self, session: requests.Session, cfg, counters: Counter):
        self.session, self.cfg, self.counters = session, cfg, counters
        self._cache: dict[str, RobotFileParser | None] = {}

    def parser(self, scheme: str, host: str) -> RobotFileParser | None:
        key = f"{scheme}://{host}"
        if key in self._cache:
            return self._cache[key]
        rp: RobotFileParser | None = RobotFileParser()
        try:
            r = self.session.get(f"{key}/robots.txt", timeout=self.cfg.request_timeout_s)
            if r.status_code == 200:
                rp.parse(r.text.splitlines())  # type: ignore[union-attr]
            elif 400 <= r.status_code < 500:
                rp.parse([])  # no robots -> allow all
            else:
                rp = None  # 5xx: treat host as off-limits for this run
        except requests.RequestException:
            rp = None
        self._cache[key] = rp
        return rp

    def allowed(self, url: str) -> tuple[bool, float | None]:
        u = urlparse(url)
        rp = self.parser(u.scheme, u.netloc)
        if rp is None:
            self.counters["skipped:robots_unreachable"] += 1
            return False, None
        ok = rp.can_fetch(self.cfg.user_agent, url)
        delay = rp.crawl_delay(self.cfg.user_agent)
        return ok, float(delay) if delay else None


def fetch_urls_impl(task_id: str, urls: list[str], cfg_dict: dict, out_dir: str) -> dict:
    t0 = time.time()
    done = read_marker(out_dir, task_id)
    if done:
        return {**done, "status": "skipped"}

    cfg = from_dict(cfg_dict)
    counters: Counter = Counter()
    domain_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    writer = ShardWriter(out_dir, task_id, cfg.crawl.batch_rows)
    session = requests.Session()
    session.headers["User-Agent"] = cfg.crawl.user_agent
    robots = _Robots(session, cfg.crawl, counters)
    last_hit: dict[str, float] = {}

    try:
        for url in urls:
            dom = domain_of(url)
            delay = cfg.crawl.min_domain_delay_s
            if cfg.crawl.respect_robots:
                ok, crawl_delay = robots.allowed(url)
                if not ok:
                    counters["records_seen"] += 1
                    counters["skipped:robots_disallowed"] += 1
                    domain_stats[dom][0] += 1
                    continue
                if crawl_delay:
                    delay = max(delay, crawl_delay)
            wait = last_hit.get(dom, 0.0) + delay - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            last_hit[dom] = time.monotonic()
            try:
                resp = session.get(url, timeout=cfg.crawl.request_timeout_s, stream=True)
                body = resp.raw.read(cfg.extract.max_html_bytes + 1, decode_content=True)
                status, ctype = resp.status_code, resp.headers.get("Content-Type", "")
                resp.close()
            except requests.RequestException:
                counters["records_seen"] += 1
                counters["skipped:fetch_error"] += 1
                domain_stats[dom][0] += 1
                continue
            before = counters["docs_kept"]
            row = process_page(
                url=url,
                status=status,
                content_type=ctype,
                fetched_at=datetime.now(timezone.utc).isoformat(),
                body=body,
                source="live",
                shard_id=task_id,
                cfg=cfg,
                counters=counters,
            )
            domain_stats[dom][0] += 1
            if row is not None:
                writer.add(row)
                domain_stats[dom][1] += counters["docs_kept"] - before
        out = writer.close()
    except BaseException:
        writer.abort()
        raise

    result = {
        "shard_id": task_id,
        "source": f"live:{len(urls)}urls",
        "status": "ok",
        "rows": writer.rows,
        "counters": dict(counters),
        "domain_stats": dict(domain_stats),
        "out_file": str(out) if out else None,
        "elapsed_s": round(time.time() - t0, 3),
    }
    write_marker(out_dir, task_id, result)
    return result
