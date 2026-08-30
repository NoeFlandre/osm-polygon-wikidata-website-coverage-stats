"""Bounded, partition-first overlap aggregation for the two coverage sets."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.domain.coverage import OVERLAP_CATEGORIES
from osm_polygon_wikidata_website_coverage.io.atomic import atomic_path
from osm_polygon_wikidata_website_coverage.io.duckdb import (
    DUCKDB_THREADS,
    MEMORY_LIMIT,
    configure_connection,
    export_query,
)
from osm_polygon_wikidata_website_coverage.io.parquet import (
    MEMBERSHIP_SCHEMA,
    OVERLAP_SCHEMA,
    SUMMARY_SCHEMA,
)
from osm_polygon_wikidata_website_coverage.pipeline.join import MembershipResult

OVERLAP_SHARD_COUNT = 64
_SHARD_EXPRESSION = f"hash(osm_type || ':' || CAST(osm_id AS VARCHAR)) % {OVERLAP_SHARD_COUNT}"
_PARTITION_QUERY = f"""
SELECT osm_type, osm_id, {_SHARD_EXPRESSION} AS shard_id
FROM read_parquet(?, union_by_name = true)
"""
_SHARD_QUERY = """
WITH raw AS (
    SELECT DISTINCT osm_type, osm_id
    FROM read_parquet(?)
), classified AS (
    SELECT
        raw.osm_type,
        raw.osm_id,
        website.osm_id IS NOT NULL AS website,
        wikidata.osm_id IS NOT NULL AS wikidata
    FROM raw
    LEFT JOIN website_keys AS website USING (osm_type, osm_id)
    LEFT JOIN wikidata_keys AS wikidata USING (osm_type, osm_id)
)
SELECT
    osm_type,
    osm_id,
    website,
    wikidata,
    CASE
        WHEN website AND wikidata THEN 'both'
        WHEN website THEN 'website_only'
        WHEN wikidata THEN 'wikidata_only'
        ELSE 'neither'
    END AS overlap_category
FROM classified
"""


class OverlapError(RuntimeError):
    """Raised when overlap inputs or generated outputs are unsafe."""


@dataclass(frozen=True, slots=True)
class OverlapResult:
    """Generated overlap shards and the four-category counts."""

    paths: tuple[Path, ...]
    summary_path: Path
    row_count: int
    summary: dict[str, int]


_write_query = export_query


def _identity_files(root: Path) -> tuple[Path, ...]:
    if not root.is_dir():
        raise OverlapError(f"raw identity directory is missing: {root}")
    files = tuple(sorted(root.glob("*.parquet"), key=lambda path: path.name))
    if not files:
        raise OverlapError(f"raw identity directory contains no Parquet files: {root}")
    return files


def _validate_membership_path(path: Path) -> None:
    if not path.is_file():
        raise OverlapError(f"membership Parquet is missing: {path}")
    try:
        schema = pq.read_schema(path)
    except (OSError, ValueError, pa.ArrowException) as exc:
        raise OverlapError(f"membership Parquet cannot be read: {path}") from exc
    if schema != MEMBERSHIP_SCHEMA:
        raise OverlapError(f"membership schema mismatch: {path}")


def _validate_memberships(memberships: MembershipResult) -> tuple[Path, Path]:
    if len(memberships.paths) != 2:
        raise ValueError("exactly two membership paths are required")
    website, wikidata = memberships.paths
    _validate_membership_path(website)
    _validate_membership_path(wikidata)
    return website, wikidata


def _inventory(paths: tuple[Path, ...], root: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": str(path.relative_to(root)),
            "size_bytes": path.stat().st_size,
            "mtime_ns": path.stat().st_mtime_ns,
        }
        for path in paths
    ]


def _coverage_root(output_root: Path) -> Path:
    return output_root / "coverage"


def _overlap_root(output_root: Path) -> Path:
    return _coverage_root(output_root) / "overlap"


def _summary_path(output_root: Path) -> Path:
    return _coverage_root(output_root) / "overlap-summary.parquet"


def _manifest_path(output_root: Path) -> Path:
    return _coverage_root(output_root) / "manifest.json"


def _output_is_valid(path: Path) -> bool:
    try:
        metadata = pq.ParquetFile(path).metadata
        return metadata is not None and pq.read_schema(path) == OVERLAP_SCHEMA
    except (OSError, ValueError, pa.ArrowException):
        return False


def _summary_is_valid(path: Path) -> bool:
    try:
        metadata = pq.ParquetFile(path).metadata
        return metadata is not None and pq.read_schema(path) == SUMMARY_SCHEMA
    except (OSError, ValueError, pa.ArrowException):
        return False


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _expected_shard_names() -> tuple[str, ...]:
    return tuple(f"shard-{index:02d}.parquet" for index in range(OVERLAP_SHARD_COUNT))


def _shard_outputs(overlap_root: Path) -> tuple[Path, ...]:
    if not overlap_root.is_dir():
        return ()
    return tuple(sorted(overlap_root.glob("shard-*.parquet")))


def _stage_manifest_matches(
    manifest: dict[str, Any] | None,
    raw_inventory: list[dict[str, Any]],
    membership_inventory: list[dict[str, Any]],
) -> bool:
    if manifest is None:
        return False
    return (
        manifest.get("schema_version"),
        manifest.get("raw_inventory"),
        manifest.get("membership_inventory"),
    ) == ("1", raw_inventory, membership_inventory)


def _all_shards_valid(outputs: tuple[Path, ...]) -> bool:
    return all(_output_is_valid(path) for path in outputs)


def _stage_is_reusable(
    output_root: Path,
    raw_inventory: list[dict[str, Any]],
    membership_inventory: list[dict[str, Any]],
) -> bool:
    manifest = _read_json(_manifest_path(output_root))
    overlap_root = _overlap_root(output_root)
    outputs = _shard_outputs(overlap_root)
    if not _stage_manifest_matches(manifest, raw_inventory, membership_inventory):
        return False
    if tuple(path.name for path in outputs) != _expected_shard_names():
        return False
    return _all_shards_valid(outputs) and _summary_is_valid(_summary_path(output_root))


def _write_empty(path: Path) -> None:
    with atomic_path(path) as temporary:
        pq.write_table(
            pa.Table.from_pylist([], schema=OVERLAP_SCHEMA), temporary, compression="zstd"
        )


def _scratch_root(output_root: Path) -> Path:
    return output_root / "scratch"


def _load_membership_tables(
    connection: duckdb.DuckDBPyConnection, memberships: tuple[Path, Path]
) -> None:
    for name, path in zip(("website_keys", "wikidata_keys"), memberships, strict=True):
        connection.execute(
            f"CREATE TEMP TABLE {name} AS SELECT DISTINCT osm_type, osm_id FROM read_parquet(?)",
            [str(path)],
        )


def _partition_directory(output_root: Path) -> Path:
    return _scratch_root(output_root) / "overlap-parts.tmp"


def _temporary_overlap_root(output_root: Path) -> Path:
    return _coverage_root(output_root) / ".overlap.tmp"


def _write_raw_partitions(
    connection: duckdb.DuckDBPyConnection, raw_root: Path, partitions: Path
) -> None:
    """Hash-partition raw identity occurrences before deduplicating or joining."""
    destination = str(partitions).replace("'", "''")
    connection.execute(
        f"COPY ({_PARTITION_QUERY}) TO '{destination}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (shard_id))",
        [str(raw_root / "*.parquet")],
    )


def _write_shard(connection: duckdb.DuckDBPyConnection, bucket: Path, output: Path) -> None:
    """Deduplicate one hash bucket and write its sorted final shard."""
    files = tuple(sorted(bucket.glob("*.parquet"))) if bucket.is_dir() else ()
    if not files:
        _write_empty(output)
        return
    _write_query(
        connection,
        _SHARD_QUERY,
        [str(bucket / "*.parquet")],
        output,
    )


def _promote_overlap(temporary_root: Path, output_root: Path) -> tuple[Path, ...]:
    final_root = _overlap_root(output_root)
    shutil.rmtree(final_root, ignore_errors=True)
    temporary_root.replace(final_root)
    return tuple(final_root / name for name in _expected_shard_names())


def _write_partitioned_overlap(
    connection: duckdb.DuckDBPyConnection,
    raw_root: Path,
    output_root: Path,
) -> tuple[Path, ...]:
    partitions = _partition_directory(output_root)
    temporary_root = _temporary_overlap_root(output_root)
    shutil.rmtree(partitions, ignore_errors=True)
    shutil.rmtree(temporary_root, ignore_errors=True)
    partitions.mkdir(parents=True, exist_ok=True)
    temporary_root.mkdir(parents=True, exist_ok=True)
    try:
        _write_raw_partitions(connection, raw_root, partitions)
        for shard_id, name in enumerate(_expected_shard_names()):
            _write_shard(connection, partitions / f"shard_id={shard_id}", temporary_root / name)
        return _promote_overlap(temporary_root, output_root)
    except BaseException:
        shutil.rmtree(temporary_root, ignore_errors=True)
        raise
    finally:
        shutil.rmtree(partitions, ignore_errors=True)


def _summary_counts(rows: list[tuple[Any, Any]]) -> dict[str, int]:
    counts = {category: 0 for category in OVERLAP_CATEGORIES}
    for category, count in rows:
        if category not in counts:
            raise OverlapError(f"unexpected overlap category: {category}")
        counts[str(category)] = int(count)
    return counts


def _summary_records(counts: dict[str, int]) -> list[dict[str, Any]]:
    total = sum(counts.values())
    return [
        {
            "overlap_category": category,
            "count": counts[category],
            "percentage": counts[category] / total * 100 if total else 0.0,
        }
        for category in OVERLAP_CATEGORIES
    ]


def _summary_rows(
    connection: duckdb.DuckDBPyConnection, overlap_root: Path
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows = connection.execute(
        """
        SELECT overlap_category, COUNT(*)
        FROM read_parquet(?)
        GROUP BY overlap_category
        """,
        [str(overlap_root / "*.parquet")],
    ).fetchall()
    counts = _summary_counts(rows)
    return _summary_records(counts), counts


def _write_summary(output_root: Path, rows: list[dict[str, Any]]) -> Path:
    output = _summary_path(output_root)
    with atomic_path(output) as temporary:
        pq.write_table(
            pa.Table.from_pylist(rows, schema=SUMMARY_SCHEMA), temporary, compression="zstd"
        )
    return output


def _write_manifest(
    output_root: Path,
    raw_inventory: list[dict[str, Any]],
    membership_inventory: list[dict[str, Any]],
    row_count: int,
    summary: dict[str, int],
) -> Path:
    output = _manifest_path(output_root)
    with atomic_path(output) as temporary:
        temporary.write_text(
            json.dumps(
                {
                    "schema_version": "1",
                    "memory_limit": MEMORY_LIMIT,
                    "duckdb_threads": DUCKDB_THREADS,
                    "raw_inventory": raw_inventory,
                    "membership_inventory": membership_inventory,
                    "row_count": row_count,
                    "summary": summary,
                },
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    return output


def _existing_result(output_root: Path) -> OverlapResult:
    paths = tuple(
        _overlap_root(output_root) / f"shard-{index:02d}.parquet"
        for index in range(OVERLAP_SHARD_COUNT)
    )
    rows = pq.read_table(_summary_path(output_root)).to_pylist()
    summary = {str(row["overlap_category"]): int(row["count"]) for row in rows}
    return OverlapResult(paths, _summary_path(output_root), sum(summary.values()), summary)


def _cleanup_scratch(output_root: Path) -> None:
    shutil.rmtree(_scratch_root(output_root), ignore_errors=True)


def _run_overlap_query(
    connection: duckdb.DuckDBPyConnection, raw_root: Path, output_root: Path
) -> tuple[Path, ...]:
    """Partition first, then deduplicate each shard and atomically promote it."""

    return _write_partitioned_overlap(connection, raw_root, output_root)


def compute_overlap(
    raw_identity_root: Path,
    memberships: MembershipResult,
    output_root: Path,
    *,
    resume: bool = False,
) -> OverlapResult:
    """Compute website/Wikidata overlap without geometry or extra reports."""

    raw_files = _identity_files(raw_identity_root)
    membership_paths = _validate_memberships(memberships)
    raw_inventory = _inventory(raw_files, raw_identity_root)
    membership_inventory = _inventory(membership_paths, membership_paths[0].parent)
    if resume and _stage_is_reusable(output_root, raw_inventory, membership_inventory):
        return _existing_result(output_root)
    overlap_root = _overlap_root(output_root)
    if overlap_root.exists() and not resume:
        raise FileExistsError(f"refusing to overwrite overlap output: {overlap_root}")
    output_root.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(database=":memory:")
    try:
        configure_connection(connection, output_root)
        _load_membership_tables(connection, membership_paths)
        paths = _run_overlap_query(connection, raw_identity_root, output_root)
        summary_rows, summary = _summary_rows(connection, _overlap_root(output_root))
        summary_path = _write_summary(output_root, summary_rows)
        row_count = sum(summary.values())
        _write_manifest(output_root, raw_inventory, membership_inventory, row_count, summary)
        return OverlapResult(paths, summary_path, row_count, summary)
    finally:
        try:
            connection.close()
        finally:
            _cleanup_scratch(output_root)
