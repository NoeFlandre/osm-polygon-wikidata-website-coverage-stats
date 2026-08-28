"""Global polygon deduplication, coverage joins, and descriptive summaries."""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.domain.coverage import EXPECTED_OVERLAP_CATEGORIES
from osm_polygon_wikidata_website_coverage.io.parquet import OCCURRENCE_SCHEMA

EXPECTED_GLOBAL_COLUMNS = (
    "osm_type",
    "osm_id",
    "source_pbf",
    "contributing_pbf_count",
    "source_pbfs",
    "region",
    "osm_version",
    "osm_timestamp",
    "relation_kind",
    "geometry_type",
    "centroid_lon",
    "centroid_lat",
    "bbox_min_lon",
    "bbox_min_lat",
    "bbox_max_lon",
    "bbox_max_lat",
    "area_m2",
    "area_bucket",
    "geometry_hash",
    "website",
    "wikipedia",
    "wikivoyage",
    "covered_by_any_text",
    "overlap_category",
)

COMPACT_GLOBAL_SCHEMA = pa.schema(
    [
        pa.field("osm_type", pa.string()),
        pa.field("osm_id", pa.int64()),
        pa.field("source_pbf", pa.string()),
        pa.field("contributing_pbf_count", pa.int64()),
        pa.field("source_pbfs", pa.string()),
        pa.field("region", pa.string()),
        pa.field("osm_version", pa.int64()),
        pa.field("osm_timestamp", pa.string()),
        pa.field("relation_kind", pa.string()),
        pa.field("geometry_type", pa.string()),
        pa.field("centroid_lon", pa.float64()),
        pa.field("centroid_lat", pa.float64()),
        pa.field("bbox_min_lon", pa.float64()),
        pa.field("bbox_min_lat", pa.float64()),
        pa.field("bbox_max_lon", pa.float64()),
        pa.field("bbox_max_lat", pa.float64()),
        pa.field("area_m2", pa.float64()),
        pa.field("area_bucket", pa.string()),
        pa.field("geometry_hash", pa.string()),
        pa.field("website", pa.bool_()),
        pa.field("wikipedia", pa.bool_()),
        pa.field("wikivoyage", pa.bool_()),
        pa.field("covered_by_any_text", pa.bool_()),
        pa.field("overlap_category", pa.string()),
    ]
)
BY_PBF_SCHEMA = pa.schema(
    [
        pa.field("source_pbf", pa.string()),
        pa.field("osm_type", pa.string()),
        pa.field("osm_id", pa.int64()),
        pa.field("region", pa.string()),
        pa.field("geometry_type", pa.string()),
        pa.field("area_m2", pa.float64()),
        pa.field("website", pa.bool_()),
        pa.field("wikipedia", pa.bool_()),
        pa.field("wikivoyage", pa.bool_()),
        pa.field("covered_by_any_text", pa.bool_()),
        pa.field("overlap_category", pa.string()),
    ]
)

GLOBAL_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("scope", pa.string()),
        pa.field("group_name", pa.string()),
        pa.field("metric", pa.string()),
        pa.field("value", pa.float64()),
    ]
)
OVERLAP_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("overlap_category", pa.string()),
        pa.field("count", pa.int64()),
        pa.field("percentage", pa.float64()),
    ]
)
GROUP_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("group_name", pa.string()),
        pa.field("valid_polygon_count", pa.int64()),
        pa.field("website_count", pa.int64()),
        pa.field("wikipedia_count", pa.int64()),
        pa.field("wikivoyage_count", pa.int64()),
        pa.field("covered_by_any_text_count", pa.int64()),
        pa.field("website_rate", pa.float64()),
        pa.field("wikipedia_rate", pa.float64()),
        pa.field("wikivoyage_rate", pa.float64()),
        pa.field("covered_by_any_text_rate", pa.float64()),
    ]
)
GROUP_METRIC_SCHEMA = GLOBAL_SUMMARY_SCHEMA
FAILURE_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("candidate_kind", pa.string()),
        pa.field("failure_kind", pa.string()),
        pa.field("count", pa.int64()),
    ]
)
CONFLICT_SCHEMA = pa.schema(
    [
        pa.field("osm_type", pa.string()),
        pa.field("osm_id", pa.int64()),
        pa.field("occurrence_count", pa.int64()),
        pa.field("distinct_geometry_count", pa.int64()),
        pa.field("source_pbfs", pa.string()),
    ]
)

_SHARD_COUNT = 64
_SHARD_EXPRESSION = "hash(osm_type || ':' || CAST(osm_id AS VARCHAR)) % 64"

_GLOBAL_SQL = """
WITH occurrence_rows AS (
    SELECT * FROM read_parquet(?, union_by_name = true)
), ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY osm_type, osm_id
        ORDER BY osm_version DESC NULLS LAST, osm_timestamp DESC NULLS LAST, source_pbf ASC
    ) AS occurrence_rank
    FROM occurrence_rows
), canonical AS (
    SELECT * FROM ranked WHERE occurrence_rank = 1
), source_lists AS (
    SELECT osm_type, osm_id,
           COUNT(DISTINCT source_pbf) AS contributing_pbf_count,
           CAST(to_json(list(DISTINCT source_pbf ORDER BY source_pbf)) AS VARCHAR) AS source_pbfs
    FROM occurrence_rows
    GROUP BY osm_type, osm_id
), website AS (
    SELECT DISTINCT osm_type, osm_id FROM read_parquet(?, union_by_name = true)
), wikipedia AS (
    SELECT DISTINCT osm_type, osm_id FROM read_parquet(?, union_by_name = true)
), wikivoyage AS (
    SELECT DISTINCT osm_type, osm_id FROM read_parquet(?, union_by_name = true)
)
SELECT
    canonical.osm_type,
    canonical.osm_id,
    canonical.source_pbf,
    source_lists.contributing_pbf_count,
    source_lists.source_pbfs,
    canonical.region,
    canonical.osm_version,
    canonical.osm_timestamp,
    canonical.relation_kind,
    canonical.geometry_type,
    canonical.centroid_lon,
    canonical.centroid_lat,
    canonical.bbox_min_lon,
    canonical.bbox_min_lat,
    canonical.bbox_max_lon,
    canonical.bbox_max_lat,
    canonical.area_m2,
    canonical.area_bucket,
    canonical.geometry_hash,
    website.osm_id IS NOT NULL AS website,
    wikipedia.osm_id IS NOT NULL AS wikipedia,
    wikivoyage.osm_id IS NOT NULL AS wikivoyage,
    (
        website.osm_id IS NOT NULL
        OR wikipedia.osm_id IS NOT NULL
        OR wikivoyage.osm_id IS NOT NULL
    ) AS covered_by_any_text,
    CASE
        WHEN website.osm_id IS NULL
         AND wikipedia.osm_id IS NULL
         AND wikivoyage.osm_id IS NULL THEN 'neither'
        WHEN website.osm_id IS NOT NULL
         AND wikipedia.osm_id IS NULL
         AND wikivoyage.osm_id IS NULL THEN 'website_only'
        WHEN website.osm_id IS NULL
         AND wikipedia.osm_id IS NOT NULL
         AND wikivoyage.osm_id IS NULL THEN 'wikipedia_only'
        WHEN website.osm_id IS NULL
         AND wikipedia.osm_id IS NULL
         AND wikivoyage.osm_id IS NOT NULL THEN 'wikivoyage_only'
        WHEN website.osm_id IS NOT NULL
         AND wikipedia.osm_id IS NOT NULL
         AND wikivoyage.osm_id IS NULL THEN 'website_wikipedia_only'
        WHEN website.osm_id IS NOT NULL
         AND wikipedia.osm_id IS NULL
         AND wikivoyage.osm_id IS NOT NULL THEN 'website_wikivoyage_only'
        WHEN website.osm_id IS NULL
         AND wikipedia.osm_id IS NOT NULL
         AND wikivoyage.osm_id IS NOT NULL THEN 'wikipedia_wikivoyage_only'
        ELSE 'all_three'
    END AS overlap_category,
    hash(canonical.osm_type || ':' || CAST(canonical.osm_id AS VARCHAR)) % 64 AS shard_id
FROM canonical
JOIN source_lists USING (osm_type, osm_id)
LEFT JOIN website USING (osm_type, osm_id)
LEFT JOIN wikipedia USING (osm_type, osm_id)
LEFT JOIN wikivoyage USING (osm_type, osm_id)
"""

_BY_PBF_SQL = """
WITH source_rows AS (
    SELECT *, row_number() OVER (
        PARTITION BY source_pbf, osm_type, osm_id
        ORDER BY osm_version DESC NULLS LAST, osm_timestamp DESC NULLS LAST
    ) AS occurrence_rank
    FROM read_parquet(?, union_by_name = true)
), website AS (
    SELECT DISTINCT osm_type, osm_id FROM read_parquet(?, union_by_name = true)
), wikipedia AS (
    SELECT DISTINCT osm_type, osm_id FROM read_parquet(?, union_by_name = true)
), wikivoyage AS (
    SELECT DISTINCT osm_type, osm_id FROM read_parquet(?, union_by_name = true)
)
SELECT
    source_rows.source_pbf,
    source_rows.osm_type,
    source_rows.osm_id,
    source_rows.region,
    source_rows.geometry_type,
    source_rows.area_m2,
    website.osm_id IS NOT NULL AS website,
    wikipedia.osm_id IS NOT NULL AS wikipedia,
    wikivoyage.osm_id IS NOT NULL AS wikivoyage,
    (
        website.osm_id IS NOT NULL
        OR wikipedia.osm_id IS NOT NULL
        OR wikivoyage.osm_id IS NOT NULL
    ) AS covered_by_any_text,
    CASE
        WHEN website.osm_id IS NULL
         AND wikipedia.osm_id IS NULL
         AND wikivoyage.osm_id IS NULL THEN 'neither'
        WHEN website.osm_id IS NOT NULL
         AND wikipedia.osm_id IS NULL
         AND wikivoyage.osm_id IS NULL THEN 'website_only'
        WHEN website.osm_id IS NULL
         AND wikipedia.osm_id IS NOT NULL
         AND wikivoyage.osm_id IS NULL THEN 'wikipedia_only'
        WHEN website.osm_id IS NULL
         AND wikipedia.osm_id IS NULL
         AND wikivoyage.osm_id IS NOT NULL THEN 'wikivoyage_only'
        WHEN website.osm_id IS NOT NULL
         AND wikipedia.osm_id IS NOT NULL
         AND wikivoyage.osm_id IS NULL THEN 'website_wikipedia_only'
        WHEN website.osm_id IS NOT NULL
         AND wikipedia.osm_id IS NULL
         AND wikivoyage.osm_id IS NOT NULL THEN 'website_wikivoyage_only'
        WHEN website.osm_id IS NULL
         AND wikipedia.osm_id IS NOT NULL
         AND wikivoyage.osm_id IS NOT NULL THEN 'wikipedia_wikivoyage_only'
        ELSE 'all_three'
    END AS overlap_category
FROM source_rows
LEFT JOIN website USING (osm_type, osm_id)
LEFT JOIN wikipedia USING (osm_type, osm_id)
LEFT JOIN wikivoyage USING (osm_type, osm_id)
WHERE source_rows.occurrence_rank = 1
"""

_COMPACT_FROM_GLOBAL_SQL = f"""
SELECT *, {_SHARD_EXPRESSION} AS shard_id
FROM read_parquet(?, union_by_name = true)
"""

_GROUP_METRICS = (
    "website_count",
    "wikipedia_count",
    "wikivoyage_count",
    "covered_by_any_text_count",
)


class AggregationError(RuntimeError):
    """Raised when aggregation inputs or outputs are not safe."""


@dataclass(frozen=True, slots=True)
class AggregationResult:
    output_root: Path
    global_paths: tuple[Path, ...]
    by_pbf_path: Path
    summary_paths: tuple[Path, ...]
    global_row_count: int
    summary: dict[str, Any]


def _parquet_files(root: Path, description: str) -> tuple[Path, ...]:
    if not root.is_dir():
        raise AggregationError(f"{description} directory is missing: {root}")
    files = tuple(sorted(root.glob("*.parquet"), key=lambda path: path.name))
    if not files:
        raise AggregationError(f"{description} directory contains no Parquet files: {root}")
    return files


def _sql_literal(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _copy_query(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[Any],
    output_path: Path,
    *,
    replace_existing: bool = False,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.tmp")
    if not replace_existing and (output_path.exists() or temporary.exists()):
        raise FileExistsError(f"refusing to overwrite aggregate output: {output_path}")
    if replace_existing:
        temporary.unlink(missing_ok=True)
    connection.execute(
        f"COPY ({query}) TO {_sql_literal(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD)",
        parameters,
    )
    os.replace(temporary, output_path)


def _fetch_count(connection: duckdb.DuckDBPyConnection, query: str, parameters: list[Any]) -> int:
    row = connection.execute(query, parameters).fetchone()
    if row is None:
        raise AggregationError("DuckDB returned no count row")
    return int(row[0])


def _source_counts(connection: duckdb.DuckDBPyConnection, compact: Path) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            SUM(CASE WHEN website THEN 1 ELSE 0 END),
            SUM(CASE WHEN wikipedia THEN 1 ELSE 0 END),
            SUM(CASE WHEN wikivoyage THEN 1 ELSE 0 END),
            SUM(CASE WHEN covered_by_any_text THEN 1 ELSE 0 END)
        FROM read_parquet(?)
        """,
        [str(compact)],
    ).fetchone()
    if row is None:
        raise AggregationError("DuckDB returned no source-count row")
    return {name: int(value or 0) for name, value in zip(_GROUP_METRICS, row, strict=True)}


def _overlap_summary(
    connection: duckdb.DuckDBPyConnection, compact: Path, total: int
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT overlap_category, COUNT(*)
        FROM read_parquet(?)
        GROUP BY overlap_category
        """,
        [str(compact)],
    ).fetchall()
    overlap_counts = {str(category): int(count) for category, count in rows}
    return [
        {
            "category": category,
            "count": overlap_counts.get(category, 0),
            "percentage": _rate(overlap_counts.get(category, 0), total),
        }
        for category in EXPECTED_OVERLAP_CATEGORIES
    ]


def _area_statistics(
    connection: duckdb.DuckDBPyConnection, compact: Path
) -> dict[str, float | None]:
    area_row = connection.execute(
        """
        SELECT
            SUM(area_m2), MIN(area_m2), MAX(area_m2), AVG(area_m2),
            MEDIAN(area_m2), QUANTILE_CONT(area_m2, 0.25),
            QUANTILE_CONT(area_m2, 0.75), QUANTILE_CONT(area_m2, 0.95)
        FROM read_parquet(?)
        """,
        [str(compact)],
    ).fetchone()
    if area_row is None:
        raise AggregationError("DuckDB returned no area-statistics row")
    return {
        name: None if value is None else float(value)
        for name, value in zip(
            (
                "total_m2",
                "min_m2",
                "max_m2",
                "mean_m2",
                "median_m2",
                "p25_m2",
                "p75_m2",
                "p95_m2",
            ),
            area_row,
            strict=True,
        )
    }


def _type_counts(
    connection: duckdb.DuckDBPyConnection, compact: Path, column: str
) -> dict[str, int]:
    rows = connection.execute(
        f"SELECT {column}, COUNT(*) FROM read_parquet(?) GROUP BY {column} ORDER BY {column}",
        [str(compact)],
    ).fetchall()
    return {str(kind): int(count) for kind, count in rows}


def _pairwise_intersections(connection: duckdb.DuckDBPyConnection, compact: Path) -> dict[str, int]:
    queries = {
        "website_wikipedia": "website AND wikipedia",
        "website_wikivoyage": "website AND wikivoyage",
        "wikipedia_wikivoyage": "wikipedia AND wikivoyage",
        "all_three": "website AND wikipedia AND wikivoyage",
    }
    return {
        name: _fetch_count(
            connection,
            f"SELECT COUNT(*) FROM read_parquet(?) WHERE {predicate}",
            [str(compact)],
        )
        for name, predicate in queries.items()
    }


def _source_audit(
    connection: duckdb.DuckDBPyConnection,
    compact: Path,
    members: tuple[Path, Path, Path],
) -> dict[str, int]:
    return {
        source: _fetch_count(
            connection,
            """
            SELECT COUNT(*)
            FROM read_parquet(?) AS membership
            LEFT JOIN read_parquet(?) AS raw
              USING (osm_type, osm_id)
            WHERE raw.osm_id IS NULL
            """,
            [str(member), str(compact)],
        )
        for source, member in zip(("website", "wikipedia", "wikivoyage"), members, strict=True)
    }


def _failure_count(connection: duckdb.DuckDBPyConnection, failure_root: Path) -> int:
    failure_files = tuple(sorted(failure_root.glob("*.parquet"))) if failure_root.is_dir() else ()
    if not failure_files:
        return 0
    return _fetch_count(
        connection,
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(failure_root / "*.parquet")],
    )


def _membership_paths(root: Path) -> tuple[Path, Path, Path]:
    website = root / "website.parquet"
    wikipedia = root / "wikipedia.parquet"
    wikivoyage = root / "wikivoyage.parquet"
    paths = (website, wikipedia, wikivoyage)
    missing = tuple(path for path in paths if not path.is_file())
    if missing:
        names = ", ".join(str(path) for path in missing)
        raise AggregationError(f"membership Parquets are missing: {names}")
    return paths


def _remove_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _duckdb_spill_directory(target: Path) -> Path:
    return target / "scratch" / "duckdb-temp"


def _aggregation_bucket_directory(target: Path) -> Path:
    return target / "scratch" / "occurrence-buckets"


def _aggregation_bucket_temporary_directory(target: Path) -> Path:
    return target / "scratch" / ".occurrence-buckets.tmp"


def _aggregation_global_part(target: Path) -> Path:
    return target / "scratch" / "global_shard.parquet"


def _cleanup_duckdb_spill(target: Path) -> None:
    _remove_directory(_duckdb_spill_directory(target))


def _cleanup_aggregation_temporary(target: Path) -> None:
    _cleanup_duckdb_spill(target)
    _remove_directory(_aggregation_bucket_directory(target))
    _remove_directory(_aggregation_bucket_temporary_directory(target))
    global_part = _aggregation_global_part(target)
    global_part.unlink(missing_ok=True)
    global_part.with_name(f".{global_part.name}.tmp").unlink(missing_ok=True)
    (target / "scratch" / ".global_compact.parquet.tmp").unlink(missing_ok=True)


def _prepare_bucket_directories(output_root: Path, *, replace_existing: bool) -> tuple[Path, Path]:
    scratch = _aggregation_bucket_directory(output_root).parent
    scratch.mkdir(parents=True, exist_ok=True)
    bucket_root = _aggregation_bucket_directory(output_root)
    temporary = _aggregation_bucket_temporary_directory(output_root)
    if replace_existing:
        _remove_directory(bucket_root)
        _remove_directory(temporary)
    elif bucket_root.exists() or temporary.exists():
        raise FileExistsError(f"refusing to overwrite aggregate buckets: {bucket_root}")
    return bucket_root, temporary


def _write_partitioned_occurrences(
    connection: duckdb.DuckDBPyConnection, occurrence_root: Path, temporary: Path
) -> None:
    connection.execute(
        f"COPY (SELECT *, {_SHARD_EXPRESSION} AS shard_id "
        f"FROM read_parquet(?, union_by_name = true)) "
        f"TO {_sql_literal(temporary)} "
        "(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (shard_id))",
        [str(occurrence_root / "*.parquet")],
    )


def _ensure_empty_buckets(temporary: Path) -> None:
    for shard_id in range(_SHARD_COUNT):
        bucket = temporary / f"shard_id={shard_id}"
        bucket.mkdir(parents=True, exist_ok=True)
        if not any(bucket.glob("*.parquet")):
            _write_rows(bucket / "empty.parquet", OCCURRENCE_SCHEMA, [])


def _partition_occurrences(
    connection: duckdb.DuckDBPyConnection,
    occurrence_root: Path,
    output_root: Path,
    *,
    replace_existing: bool = False,
) -> Path:
    """Partition raw occurrences once so each global shard has a bounded join."""

    bucket_root, temporary = _prepare_bucket_directories(
        output_root, replace_existing=replace_existing
    )
    try:
        _write_partitioned_occurrences(connection, occurrence_root, temporary)
        _ensure_empty_buckets(temporary)
        temporary.replace(bucket_root)
    except BaseException:
        _remove_directory(temporary)
        raise
    return bucket_root


def _configure_connection(connection: duckdb.DuckDBPyConnection, target: Path) -> None:
    """Keep DuckDB spill files with the run and bound resource fan-out."""

    _cleanup_aggregation_temporary(target)
    temp_directory = _duckdb_spill_directory(target)
    temp_directory.mkdir(parents=True, exist_ok=True)
    connection.execute(f"SET temp_directory = {_sql_literal(temp_directory)}")
    connection.execute("SET preserve_insertion_order = false")
    connection.execute("SET threads = 4")
    connection.execute("SET max_temp_directory_size = '100GB'")


def _global_parameters(occurrence_root: Path, members: tuple[Path, Path, Path]) -> list[str]:
    return [str(occurrence_root / "*.parquet"), *(str(path) for path in members)]


def _materialize_global(
    connection: duckdb.DuckDBPyConnection,
    occurrence_root: Path,
    members: tuple[Path, Path, Path],
    output_root: Path,
    *,
    replace_existing: bool = False,
) -> tuple[tuple[Path, ...], Path, int]:
    scratch = output_root / "scratch"
    compact = scratch / "global_compact.parquet"
    global_root = output_root / "coverage" / "global"
    bucket_root = _partition_occurrences(
        connection,
        occurrence_root,
        output_root,
        replace_existing=replace_existing,
    )
    global_part = _aggregation_global_part(output_root)
    global_paths: list[Path] = []
    columns = ", ".join(EXPECTED_GLOBAL_COLUMNS)
    shard_query = "SELECT " + columns
    shard_query += " FROM read_parquet(?) WHERE shard_id = ?"
    shard_query += " ORDER BY osm_type, osm_id"
    try:
        for shard_id in range(_SHARD_COUNT):
            bucket = bucket_root / f"shard_id={shard_id}"
            _copy_query(
                connection,
                _GLOBAL_SQL,
                _global_parameters(bucket, members),
                global_part,
                replace_existing=True,
            )
            output = global_root / f"shard-{shard_id:02d}.parquet"
            _copy_query(
                connection,
                shard_query,
                [str(global_part), shard_id],
                output,
                replace_existing=replace_existing,
            )
            global_paths.append(output)
        _copy_query(
            connection,
            _COMPACT_FROM_GLOBAL_SQL,
            [str(global_root / "*.parquet")],
            compact,
            replace_existing=replace_existing,
        )
        row_count = _fetch_count(connection, "SELECT COUNT(*) FROM read_parquet(?)", [str(compact)])
        return tuple(global_paths), compact, row_count
    finally:
        global_part.unlink(missing_ok=True)
        global_part.with_name(f".{global_part.name}.tmp").unlink(missing_ok=True)
        _remove_directory(bucket_root)
        _remove_directory(_aggregation_bucket_temporary_directory(output_root))


def _materialize_by_pbf(
    connection: duckdb.DuckDBPyConnection,
    occurrence_root: Path,
    members: tuple[Path, Path, Path],
    output_root: Path,
    *,
    replace_existing: bool = False,
) -> Path:
    output = output_root / "coverage" / "by-pbf" / "by-pbf.parquet"
    if replace_existing:
        _copy_query(
            connection,
            _BY_PBF_SQL,
            _global_parameters(occurrence_root, members),
            output,
            replace_existing=True,
        )
    else:
        _copy_query(
            connection,
            _BY_PBF_SQL,
            _global_parameters(occurrence_root, members),
            output,
        )
    return output


def _fetch_summary(
    connection: duckdb.DuckDBPyConnection,
    compact: Path,
    members: tuple[Path, Path, Path],
    failure_root: Path,
) -> dict[str, Any]:
    total = _fetch_count(connection, "SELECT COUNT(*) FROM read_parquet(?)", [str(compact)])
    summary = {
        "valid_universe_count": total,
        **_source_counts(connection, compact),
        "geometry_failure_count": _failure_count(connection, failure_root),
        "source_keys_not_in_raw": _source_audit(connection, compact, members),
        "overlap_categories": _overlap_summary(connection, compact, total),
        "pairwise_intersections": _pairwise_intersections(connection, compact),
        "osm_type_counts": _type_counts(connection, compact, "osm_type"),
        "geometry_type_counts": _type_counts(connection, compact, "geometry_type"),
        "area_statistics": _area_statistics(connection, compact),
    }
    return summary


def _rate(count: int, denominator: int) -> float:
    return count / denominator * 100 if denominator else 0.0


def _group_rows(
    connection: duckdb.DuckDBPyConnection,
    table: Path,
    group_column: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""
        SELECT
            {group_column} AS group_name,
            COUNT(*) AS valid_polygon_count,
            SUM(CASE WHEN website THEN 1 ELSE 0 END) AS website_count,
            SUM(CASE WHEN wikipedia THEN 1 ELSE 0 END) AS wikipedia_count,
            SUM(CASE WHEN wikivoyage THEN 1 ELSE 0 END) AS wikivoyage_count,
            SUM(CASE WHEN covered_by_any_text THEN 1 ELSE 0 END) AS covered_by_any_text_count
        FROM read_parquet(?)
        GROUP BY {group_column}
        ORDER BY group_name
        """,
        [str(table)],
    ).fetchall()
    return [
        {
            "group_name": str(group),
            "valid_polygon_count": int(total),
            "website_count": int(website),
            "wikipedia_count": int(wikipedia),
            "wikivoyage_count": int(wikivoyage),
            "covered_by_any_text_count": int(covered),
            "website_rate": _rate(int(website), int(total)),
            "wikipedia_rate": _rate(int(wikipedia), int(total)),
            "wikivoyage_rate": _rate(int(wikivoyage), int(total)),
            "covered_by_any_text_rate": _rate(int(covered), int(total)),
        }
        for group, total, website, wikipedia, wikivoyage, covered in rows
    ]


def _group_base_rows(
    connection: duckdb.DuckDBPyConnection, table: Path, group_column: str
) -> list[tuple[Any, ...]]:
    return connection.execute(
        f"""
        SELECT
            {group_column} AS group_name,
            COUNT(*) AS valid_polygon_count,
            SUM(CASE WHEN website THEN 1 ELSE 0 END) AS website_count,
            SUM(CASE WHEN wikipedia THEN 1 ELSE 0 END) AS wikipedia_count,
            SUM(CASE WHEN wikivoyage THEN 1 ELSE 0 END) AS wikivoyage_count,
            SUM(CASE WHEN covered_by_any_text THEN 1 ELSE 0 END) AS covered_by_any_text_count
        FROM read_parquet(?)
        GROUP BY {group_column}
        ORDER BY group_name
        """,
        [str(table)],
    ).fetchall()


def _group_fixed_metric_rows(base_rows: list[tuple[Any, ...]], scope: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for group, total, website, wikipedia, wikivoyage, covered in base_rows:
        name = str(group)
        total_count = int(total)
        counts = {
            "valid_polygon_count": total_count,
            "website_count": int(website),
            "wikipedia_count": int(wikipedia),
            "wikivoyage_count": int(wikivoyage),
            "covered_by_any_text_count": int(covered),
        }
        result.extend(
            {
                "scope": scope,
                "group_name": name,
                "metric": metric,
                "value": float(value),
            }
            for metric, value in counts.items()
        )
        result.extend(
            {
                "scope": scope,
                "group_name": name,
                "metric": f"{metric.removesuffix('_count')}_rate",
                "value": _rate(value, total_count),
            }
            for metric, value in counts.items()
            if metric != "valid_polygon_count"
        )
    return result


def _group_overlap_metric_rows(
    connection: duckdb.DuckDBPyConnection,
    table: Path,
    group_column: str,
    scope: str,
    totals: Mapping[str, int],
) -> list[dict[str, Any]]:
    overlap_rows = connection.execute(
        f"""
        SELECT {group_column} AS group_name, overlap_category, COUNT(*)
        FROM read_parquet(?)
        GROUP BY {group_column}, overlap_category
        ORDER BY group_name, overlap_category
        """,
        [str(table)],
    ).fetchall()
    counts = {(str(group), str(category)): int(count) for group, category, count in overlap_rows}
    result: list[dict[str, Any]] = []
    for group_name in totals:
        for category_name in EXPECTED_OVERLAP_CATEGORIES:
            count = counts.get((group_name, category_name), 0)
            result.extend(
                (
                    {
                        "scope": scope,
                        "group_name": group_name,
                        "metric": f"overlap_count:{category_name}",
                        "value": float(count),
                    },
                    {
                        "scope": scope,
                        "group_name": group_name,
                        "metric": f"overlap_rate:{category_name}",
                        "value": _rate(count, totals[group_name]),
                    },
                )
            )
    return result


def _group_area_metric_rows(
    connection: duckdb.DuckDBPyConnection,
    table: Path,
    group_column: str,
    scope: str,
) -> list[dict[str, Any]]:
    area_rows = connection.execute(
        f"""
        SELECT {group_column} AS group_name,
               SUM(area_m2), MIN(area_m2), MAX(area_m2), AVG(area_m2),
               MEDIAN(area_m2), QUANTILE_CONT(area_m2, 0.25),
               QUANTILE_CONT(area_m2, 0.75), QUANTILE_CONT(area_m2, 0.95)
        FROM read_parquet(?)
        GROUP BY {group_column}
        ORDER BY group_name
        """,
        [str(table)],
    ).fetchall()
    area_names = (
        "area_total_m2",
        "area_min_m2",
        "area_max_m2",
        "area_mean_m2",
        "area_median_m2",
        "area_p25_m2",
        "area_p75_m2",
        "area_p95_m2",
    )
    result: list[dict[str, Any]] = []
    for row in area_rows:
        result.extend(
            {
                "scope": scope,
                "group_name": str(row[0]),
                "metric": metric,
                "value": float(value),
            }
            for metric, value in zip(area_names, row[1:], strict=True)
            if value is not None
        )
    return result


def _group_type_metric_rows(
    connection: duckdb.DuckDBPyConnection,
    table: Path,
    group_column: str,
    scope: str,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for column, prefix in (
        ("osm_type", "osm_type_count"),
        ("geometry_type", "geometry_type_count"),
    ):
        type_rows = connection.execute(
            f"""
            SELECT {group_column} AS group_name, {column}, COUNT(*)
            FROM read_parquet(?)
            GROUP BY {group_column}, {column}
            ORDER BY group_name, {column}
            """,
            [str(table)],
        ).fetchall()
        result.extend(
            {
                "scope": scope,
                "group_name": str(group),
                "metric": f"{prefix}:{kind}",
                "value": float(count),
            }
            for group, kind, count in type_rows
        )
    return result


def _group_metric_rows(
    connection: duckdb.DuckDBPyConnection,
    table: Path,
    group_column: str,
    scope: str,
) -> list[dict[str, Any]]:
    base_rows = _group_base_rows(connection, table, group_column)
    totals = {str(group): int(total) for group, total, *_ in base_rows}
    return [
        *_group_fixed_metric_rows(base_rows, scope),
        *_group_overlap_metric_rows(connection, table, group_column, scope, totals),
        *_group_area_metric_rows(connection, table, group_column, scope),
        *_group_type_metric_rows(connection, table, group_column, scope),
    ]


def _write_rows(
    path: Path,
    schema: pa.Schema,
    rows: list[dict[str, Any]],
    *,
    replace_existing: bool = False,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    if not replace_existing and (path.exists() or temporary.exists()):
        raise FileExistsError(f"refusing to overwrite summary output: {path}")
    if replace_existing:
        temporary.unlink(missing_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary, compression="zstd")
    os.replace(temporary, path)


def _fixed_metric_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    fixed_metrics = (
        "valid_universe_count",
        "website_count",
        "wikipedia_count",
        "wikivoyage_count",
        "covered_by_any_text_count",
        "geometry_failure_count",
    )
    return [
        {
            "scope": "global",
            "group_name": "all",
            "metric": metric,
            "value": float(summary[metric]),
        }
        for metric in fixed_metrics
    ]


def _area_metric_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "scope": "area",
            "group_name": "all",
            "metric": metric,
            "value": float(value),
        }
        for metric, value in summary["area_statistics"].items()
        if value is not None
    ]


def _source_audit_metric_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "scope": "source_audit",
            "group_name": source,
            "metric": "keys_not_in_raw_universe",
            "value": float(count),
        }
        for source, count in summary["source_keys_not_in_raw"].items()
    ]


def _type_metric_rows(summary: dict[str, Any], key: str) -> list[dict[str, Any]]:
    return [
        {
            "scope": key,
            "group_name": kind,
            "metric": "valid_polygon_count",
            "value": float(count),
        }
        for kind, count in summary[f"{key}_counts"].items()
    ]


def _metric_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        *_fixed_metric_rows(summary),
        *_area_metric_rows(summary),
        *_source_audit_metric_rows(summary),
        *_type_metric_rows(summary, "osm_type"),
        *_type_metric_rows(summary, "geometry_type"),
    ]


def _overlap_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "overlap_category": item["category"],
            "count": item["count"],
            "percentage": item["percentage"],
        }
        for item in summary["overlap_categories"]
    ]


def _write_summary_outputs(
    connection: duckdb.DuckDBPyConnection,
    compact: Path,
    by_pbf: Path,
    output_root: Path,
    summary: dict[str, Any],
    occurrence_root: Path,
    *,
    replace_existing: bool = False,
) -> tuple[Path, ...]:
    def write_rows(path: Path, schema: pa.Schema, rows: list[dict[str, Any]]) -> None:
        if replace_existing:
            _write_rows(path, schema, rows, replace_existing=True)
        else:
            _write_rows(path, schema, rows)

    summaries_root = output_root / "summaries"
    global_summary = summaries_root / "global.parquet"
    write_rows(global_summary, GLOBAL_SUMMARY_SCHEMA, _metric_rows(summary))
    overlap_summary = summaries_root / "by-overlap.parquet"
    write_rows(overlap_summary, OVERLAP_SUMMARY_SCHEMA, _overlap_rows(summary))
    by_pbf_summary = summaries_root / "by-source-pbf.parquet"
    write_rows(
        by_pbf_summary,
        GROUP_SUMMARY_SCHEMA,
        _group_rows(connection, by_pbf, "source_pbf"),
    )
    by_region_summary = summaries_root / "by-region.parquet"
    write_rows(
        by_region_summary,
        GROUP_SUMMARY_SCHEMA,
        _group_rows(connection, compact, "region"),
    )
    by_pbf_metrics = summaries_root / "by-source-pbf-metrics.parquet"
    write_rows(
        by_pbf_metrics,
        GROUP_METRIC_SCHEMA,
        _group_metric_rows(connection, by_pbf, "source_pbf", "source_pbf"),
    )
    by_region_metrics = summaries_root / "by-region-metrics.parquet"
    write_rows(
        by_region_metrics,
        GROUP_METRIC_SCHEMA,
        _group_metric_rows(connection, compact, "region", "region"),
    )
    failure_summary = summaries_root / "geometry-failures.parquet"
    if replace_existing:
        _write_failure_summary(
            connection,
            occurrence_root.parent / "geometry-failures",
            failure_summary,
            replace_existing=True,
        )
    else:
        _write_failure_summary(
            connection, occurrence_root.parent / "geometry-failures", failure_summary
        )
    conflict_summary = summaries_root / "conflicts.parquet"
    if replace_existing:
        _write_conflicts(
            connection,
            occurrence_root,
            conflict_summary,
            replace_existing=True,
        )
    else:
        _write_conflicts(connection, occurrence_root, conflict_summary)
    return (
        global_summary,
        by_pbf_summary,
        by_region_summary,
        by_pbf_metrics,
        by_region_metrics,
        overlap_summary,
        failure_summary,
        conflict_summary,
    )


def _write_failure_summary(
    connection: duckdb.DuckDBPyConnection,
    failure_root: Path,
    output: Path,
    *,
    replace_existing: bool = False,
) -> None:
    files = tuple(sorted(failure_root.glob("*.parquet"))) if failure_root.is_dir() else ()
    rows = []
    if files:
        result = connection.execute(
            """
            SELECT candidate_kind, failure_kind, COUNT(*)
            FROM read_parquet(?)
            GROUP BY candidate_kind, failure_kind
            ORDER BY candidate_kind, failure_kind
            """,
            [str(failure_root / "*.parquet")],
        ).fetchall()
        rows = [
            {"candidate_kind": str(candidate), "failure_kind": str(kind), "count": int(count)}
            for candidate, kind, count in result
        ]
    if replace_existing:
        _write_rows(output, FAILURE_SUMMARY_SCHEMA, rows, replace_existing=True)
    else:
        _write_rows(output, FAILURE_SUMMARY_SCHEMA, rows)


def _write_conflicts(
    connection: duckdb.DuckDBPyConnection,
    occurrence_root: Path,
    output: Path,
    *,
    replace_existing: bool = False,
) -> None:
    result = connection.execute(
        """
        SELECT
            osm_type,
            osm_id,
            COUNT(*) AS occurrence_count,
            COUNT(DISTINCT geometry_hash) AS distinct_geometry_count,
            CAST(to_json(list(DISTINCT source_pbf ORDER BY source_pbf)) AS VARCHAR) AS source_pbfs
        FROM read_parquet(?)
        GROUP BY osm_type, osm_id
        HAVING COUNT(*) > 1
        ORDER BY osm_type, osm_id
        """,
        [str(occurrence_root / "*.parquet")],
    ).fetchall()
    rows = [
        {
            "osm_type": str(osm_type),
            "osm_id": int(osm_id),
            "occurrence_count": int(occurrence_count),
            "distinct_geometry_count": int(distinct_geometry_count),
            "source_pbfs": str(source_pbfs),
        }
        for osm_type, osm_id, occurrence_count, distinct_geometry_count, source_pbfs in result
    ]
    if replace_existing:
        _write_rows(output, CONFLICT_SCHEMA, rows, replace_existing=True)
    else:
        _write_rows(output, CONFLICT_SCHEMA, rows)


def summarize_rows(rows: Iterable[Mapping[str, object]]) -> dict[str, Any]:
    """Summarize compact rows while retaining all eight overlap categories."""

    category_counts = dict.fromkeys(EXPECTED_OVERLAP_CATEGORIES, 0)
    total = 0
    for row in rows:
        category = str(row["overlap_category"])
        if category not in category_counts:
            raise ValueError(f"unknown overlap category: {category}")
        category_counts[category] += 1
        total += 1
    return {
        "valid_universe_count": total,
        "overlap_categories": [
            {"category": category, "count": count} for category, count in category_counts.items()
        ],
    }


def _aggregate_target(occurrence_root: Path, output_root: Path | None) -> Path:
    return occurrence_root.parent if output_root is None else output_root


def _validate_aggregate_inputs(
    occurrence_root: Path, membership_root: Path
) -> tuple[Path, Path, Path]:
    _parquet_files(occurrence_root, "occurrence")
    return _membership_paths(membership_root)


def _existing_output_directories(target: Path) -> tuple[Path, ...]:
    existing: list[Path] = []
    for name in ("coverage", "summaries", "scratch"):
        path = target / name
        if path.exists():
            existing.append(path)
    return tuple(existing)


def _validate_aggregate_target(target: Path, *, resume: bool = False) -> None:
    existing_outputs = _existing_output_directories(target)
    if existing_outputs and not resume:
        names = ", ".join(str(path) for path in existing_outputs)
        raise AggregationError(f"aggregate output directories already exist: {names}")


def aggregate_run(
    *,
    occurrence_root: Path,
    membership_root: Path,
    output_root: Path | None = None,
    resume: bool = False,
) -> AggregationResult:
    """Aggregate occurrence and membership shards into compact coverage outputs."""

    members = _validate_aggregate_inputs(occurrence_root, membership_root)
    target = _aggregate_target(occurrence_root, output_root)
    _validate_aggregate_target(target, resume=resume)
    target.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(database=":memory:")
    try:
        _configure_connection(connection, target)
        if resume:
            global_paths, compact, row_count = _materialize_global(
                connection,
                occurrence_root,
                members,
                target,
                replace_existing=True,
            )
            by_pbf = _materialize_by_pbf(
                connection,
                occurrence_root,
                members,
                target,
                replace_existing=True,
            )
        else:
            global_paths, compact, row_count = _materialize_global(
                connection, occurrence_root, members, target
            )
            by_pbf = _materialize_by_pbf(connection, occurrence_root, members, target)
        summary = _fetch_summary(
            connection,
            compact,
            members,
            occurrence_root.parent / "geometry-failures",
        )
        if resume:
            summary_paths = _write_summary_outputs(
                connection,
                compact,
                by_pbf,
                target,
                summary,
                occurrence_root,
                replace_existing=True,
            )
        else:
            summary_paths = _write_summary_outputs(
                connection, compact, by_pbf, target, summary, occurrence_root
            )
    finally:
        try:
            connection.close()
        finally:
            _cleanup_duckdb_spill(target)
    return AggregationResult(target, global_paths, by_pbf, summary_paths, row_count, summary)
