"""Atomic, resumable per-shard Parquet output."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .docproc import DOC_SCHEMA


class ShardWriter:
    """Buffers rows, writes Parquet row groups, and publishes atomically on close()."""

    def __init__(self, out_dir: str | Path, shard_id: str, batch_rows: int = 2000):
        self.raw_dir = Path(out_dir) / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.final = self.raw_dir / f"part-{shard_id}.parquet"
        self.tmp = self.raw_dir / f".part-{shard_id}.parquet.tmp"
        self.batch_rows = batch_rows
        self._buf: list[dict] = []
        self._writer: pq.ParquetWriter | None = None
        self.rows = 0

    def add(self, row: dict) -> None:
        self._buf.append(row)
        if len(self._buf) >= self.batch_rows:
            self._flush()

    def _flush(self) -> None:
        if not self._buf:
            return
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.tmp, DOC_SCHEMA, compression="zstd")
        cols = {f.name: [r[f.name] for r in self._buf] for f in DOC_SCHEMA}
        self._writer.write_table(pa.table(cols, schema=DOC_SCHEMA))
        self.rows += len(self._buf)
        self._buf.clear()

    def close(self) -> Path | None:
        self._flush()
        if self._writer is None:
            return None
        self._writer.close()
        os.replace(self.tmp, self.final)
        return self.final

    def abort(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:  # noqa: BLE001
                pass
        self.tmp.unlink(missing_ok=True)


def marker_path(out_dir: str | Path, shard_id: str) -> Path:
    return Path(out_dir) / "raw" / "_done" / f"{shard_id}.json"


def read_marker(out_dir: str | Path, shard_id: str) -> dict | None:
    p = marker_path(out_dir, shard_id)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except json.JSONDecodeError:
            return None  # torn marker -> redo shard
    return None


def write_marker(out_dir: str | Path, shard_id: str, result: dict) -> None:
    p = marker_path(out_dir, shard_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps({**result, "completed_at": time.time()}))
    os.replace(tmp, p)
