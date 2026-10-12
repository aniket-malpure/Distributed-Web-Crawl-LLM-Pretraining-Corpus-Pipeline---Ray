"""Lightweight observability.

Workers accumulate plain ``Counter`` dicts (cheap, picklable) and return them with
their shard result. The driver merges them into a ``MetricsRegistry`` which can:

* serve Prometheus text format on ``/metrics`` (``start_http``)
* dump a node-exporter-style textfile (``write_textfile``)
* dump JSON for the benchmark / analysis reports

Counter keys use ``name`` or ``name:label`` (e.g. ``rejected:min_words``); the label part
is exposed as the ``reason`` Prometheus label.
"""
from __future__ import annotations

import json
import threading
import time
from collections import Counter
from pathlib import Path

from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

PREFIX = "crawlpipe_"


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Counter = Counter()
        self._gauges: dict[str, float] = {}
        self._started = time.time()
        self.registry = CollectorRegistry()
        self.registry.register(self)  # type: ignore[arg-type]

    # -- updates ---------------------------------------------------------------
    def merge(self, counters: dict[str, float]) -> None:
        with self._lock:
            self._counters.update(counters)

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = float(value)

    # -- reads -----------------------------------------------------------------
    def snapshot(self) -> dict[str, float]:
        with self._lock:
            return dict(self._counters)

    def yield_ratio(self) -> float:
        snap = self.snapshot()
        seen = snap.get("records_seen", 0)
        return snap.get("docs_kept", 0) / seen if seen else 0.0

    def to_json(self, path: str | Path) -> None:
        snap = self.snapshot()
        payload = {
            "counters": snap,
            "gauges": dict(self._gauges),
            "elapsed_s": round(time.time() - self._started, 2),
            "yield_ratio": round(self.yield_ratio(), 4),
        }
        Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True))

    # -- Prometheus collector protocol -------------------------------------------
    def collect(self):
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
        fams: dict[str, CounterMetricFamily] = {}
        labelled = {k.partition(":")[0] for k in counters if ":" in k}
        for key, val in sorted(counters.items()):
            base, _, label = key.partition(":")
            name = PREFIX + base
            if base not in fams:
                fams[base] = CounterMetricFamily(
                    name,
                    f"crawlpipe counter {base}",
                    labels=["reason"] if base in labelled else [],
                )
            fams[base].add_metric([label] if base in labelled else [], val)
        yield from fams.values()
        for gname, gval in sorted(gauges.items()):
            g = GaugeMetricFamily(PREFIX + gname, f"crawlpipe gauge {gname}")
            g.add_metric([], gval)
            yield g
        up = GaugeMetricFamily(PREFIX + "elapsed_seconds", "seconds since start")
        up.add_metric([], time.time() - self._started)
        yield up
        seen = counters.get("records_seen", 0)
        y = GaugeMetricFamily(PREFIX + "yield_ratio", "kept docs / records seen")
        y.add_metric([], counters.get("docs_kept", 0) / seen if seen else 0.0)
        yield y

    # -- exposition ------------------------------------------------------------
    def start_http(self, port: int) -> None:
        start_http_server(port, registry=self.registry)

    def write_textfile(self, path: str | Path) -> None:
        from prometheus_client import generate_latest

        Path(path).write_bytes(generate_latest(self.registry))
