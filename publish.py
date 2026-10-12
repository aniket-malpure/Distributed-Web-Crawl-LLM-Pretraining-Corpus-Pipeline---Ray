"""Versioned corpus delivery.

Each publish creates an immutable snapshot ``<root>/snapshots/vNNNN/`` containing Parquet
data files plus a ``manifest.json`` (row counts, file checksums, schema fingerprint, config
fingerprint, upstream crawl/dedup reports, parent version). ``<root>/LATEST`` points at the
newest version, so pre-training jobs can pin an exact version or follow LATEST.

Optionally every snapshot is also appended to an Apache Iceberg table (SQL/SQLite catalog),
which gives time travel, schema evolution and ACID appends for the same data.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .config import PipelineConfig


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _schema_fingerprint(schema: pa.Schema) -> str:
    return hashlib.sha256(str(schema).encode()).hexdigest()[:16]


def list_versions(root: str | Path) -> list[str]:
    snaps = Path(root) / "snapshots"
    return sorted(p.name for p in snaps.iterdir() if p.is_dir()) if snaps.exists() else []


def latest_version(root: str | Path) -> str | None:
    p = Path(root) / "LATEST"
    return p.read_text().strip() if p.exists() else None


def snapshot_dir(root: str | Path, version: str | None = None) -> Path:
    version = version or latest_version(root)
    if not version:
        raise FileNotFoundError(f"no published versions under {root}")
    return Path(root) / "snapshots" / version


def read_manifest(root: str | Path, version: str | None = None) -> dict:
    return json.loads((snapshot_dir(root, version) / "manifest.json").read_text())


def open_dataset(root: str | Path, version: str | None = None) -> ds.Dataset:
    return ds.dataset(snapshot_dir(root, version) / "data", format="parquet", partitioning="hive")


def publish_snapshot(
    docs_path: str | Path,
    root: str | Path,
    cfg: PipelineConfig,
    upstream: dict | None = None,
) -> dict:
    root = Path(root)
    versions = list_versions(root)
    parent = versions[-1] if versions else None
    version = f"v{len(versions) + 1:04d}"
    final = root / "snapshots" / version
    staging = root / "snapshots" / f".{version}.staging"
    if staging.exists():
        shutil.rmtree(staging)
    (staging / "data").mkdir(parents=True)

    src = ds.dataset(str(docs_path), format="parquet")
    pcfg = cfg.publish
    ds.write_dataset(
        src,
        base_dir=str(staging / "data"),
        format="parquet",
        partitioning=ds.partitioning(pa.schema([("lang", pa.string())]), flavor="hive")
        if pcfg.partition_by_lang
        else None,
        file_options=ds.ParquetFileFormat().make_write_options(compression="zstd"),
        max_rows_per_file=pcfg.max_rows_per_file,
        max_rows_per_group=min(pcfg.max_rows_per_group, pcfg.max_rows_per_file),
        basename_template="part-{i}.parquet",
        existing_data_behavior="error",
    )

    files = []
    total_rows = 0
    for f in sorted((staging / "data").rglob("*.parquet")):
        meta = pq.ParquetFile(f).metadata
        total_rows += meta.num_rows
        files.append(
            {
                "path": str(f.relative_to(staging)),
                "rows": meta.num_rows,
                "bytes": f.stat().st_size,
                "sha256": _sha256(f),
            }
        )
    expected = src.count_rows()
    if total_rows != expected:
        shutil.rmtree(staging)
        raise RuntimeError(f"row count mismatch after write: wrote {total_rows}, expected {expected}")

    manifest = {
        "version": version,
        "parent": parent,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "rows": total_rows,
        "bytes": sum(f["bytes"] for f in files),
        "files": files,
        "schema_fingerprint": _schema_fingerprint(src.schema),
        "config_fingerprint": cfg.fingerprint(),
        "upstream": upstream or {},
    }
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2))
    staging.rename(final)  # atomic publish of the whole version
    (root / "LATEST").write_text(version)

    if pcfg.iceberg:
        manifest["iceberg"] = _append_iceberg(root, final / "data", version)
        (final / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def verify_snapshot(root: str | Path, version: str | None = None) -> bool:
    """Recompute checksums for every data file; True when the snapshot is intact."""
    d = snapshot_dir(root, version)
    manifest = json.loads((d / "manifest.json").read_text())
    return all(
        (d / f["path"]).exists() and _sha256(d / f["path"]) == f["sha256"] for f in manifest["files"]
    )


def diff_versions(root: str | Path, a: str, b: str) -> dict:
    """Which documents were added / removed between two published versions."""
    ids_a = set(open_dataset(root, a).to_table(columns=["doc_id"])["doc_id"].to_pylist())
    ids_b = set(open_dataset(root, b).to_table(columns=["doc_id"])["doc_id"].to_pylist())
    return {
        "from": a,
        "to": b,
        "added": len(ids_b - ids_a),
        "removed": len(ids_a - ids_b),
        "unchanged": len(ids_a & ids_b),
    }


# --------------------------------------------------------------------------- Iceberg


def _iceberg_catalog(root: Path):
    from pyiceberg.catalog.sql import SqlCatalog

    base = (root / "iceberg").resolve()
    (base / "warehouse").mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "crawlpipe",
        uri=f"sqlite:///{base / 'catalog.db'}",
        warehouse=f"file://{base / 'warehouse'}",
    )


def _append_iceberg(root: Path, data_dir: Path, version: str) -> dict:
    """Append a snapshot to the Iceberg table ``corpus.docs`` (created on first use)."""
    catalog = _iceberg_catalog(root)
    catalog.create_namespace_if_not_exists("corpus")
    table_name = "corpus.docs"
    arrow = ds.dataset(str(data_dir), format="parquet", partitioning="hive").to_table()
    # Iceberg wants a plain (non-dictionary) schema; hive partition columns come back as dictionary.
    cols = {}
    for name in arrow.schema.names:
        col = arrow[name]
        if pa.types.is_dictionary(col.type):
            col = col.cast(col.type.value_type)
        cols[name] = col
    arrow = pa.table(cols)
    if catalog.table_exists(table_name):
        table = catalog.load_table(table_name)
    else:
        table = catalog.create_table(table_name, schema=arrow.schema)
    table.append(arrow)
    table.refresh()
    snap = table.current_snapshot()
    # Persist version -> snapshot_id for time travel.
    mapping_path = root / "iceberg" / "versions.json"
    mapping = json.loads(mapping_path.read_text()) if mapping_path.exists() else {}
    mapping[version] = snap.snapshot_id
    mapping_path.write_text(json.dumps(mapping, indent=2))
    return {"table": table_name, "snapshot_id": snap.snapshot_id, "rows": arrow.num_rows}


def iceberg_time_travel(root: str | Path, version: str):
    """Scan the Iceberg table as of a published corpus version -> pyarrow.Table."""
    root = Path(root)
    mapping = json.loads((root / "iceberg" / "versions.json").read_text())
    table = _iceberg_catalog(root).load_table("corpus.docs")
    return table.scan(snapshot_id=mapping[version]).to_arrow()
