"""Global polygon deduplication, coverage joins, and descriptive summaries."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.domain.coverage import EXPECTED_OVERLAP_CATEGORIES

EXPECTED_GLOBAL_COLUMNS = (
    "osm_type",
    "osm_id",
    "source_pbf",
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
           CAST(to_json(list(source_pbf ORDER BY source_pbf)) AS VARCHAR) AS source_pbfs
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
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.tmp")
    if output_path.exists() or temporary.exists():
        raise FileExistsError(f"refusing to overwrite aggregate output: {output_path}")
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


def _global_parameters(occurrence_root: Path, members: tuple[Path, Path, Path]) -> list[str]:
    return [str(occurrence_root / "*.parquet"), *(str(path) for path in members)]


def _materialize_global(
    connection: duckdb.DuckDBPyConnection,
    occurrence_root: Path,
    members: tuple[Path, Path, Path],
    output_root: Path,
) -> tuple[tuple[Path, ...], Path, int]:
    scratch = output_root / "scratch"
    compact = scratch / "global_compact.parquet"
    parameters = _global_parameters(occurrence_root, members)
    _copy_query(connection, _GLOBAL_SQL, parameters, compact)
    global_root = output_root / "coverage" / "global"
    shard_rows = connection.execute(
        "SELECT DISTINCT shard_id FROM read_parquet(?) ORDER BY shard_id", [str(compact)]
    ).fetchall()
    global_paths: list[Path] = []
    columns = ", ".join(EXPECTED_GLOBAL_COLUMNS)
    for (shard_id,) in shard_rows:
        output = global_root / f"shard-{int(shard_id):02d}.parquet"
        _copy_query(
            connection,
            f"SELECT {columns} FROM read_parquet(?) WHERE shard_id = ? ORDER BY osm_type, osm_id",
            [str(compact), int(shard_id)],
            output,
        )
        global_paths.append(output)
    row_count = _fetch_count(connection, "SELECT COUNT(*) FROM read_parquet(?)", [str(compact)])
    return tuple(global_paths), compact, row_count


def _materialize_by_pbf(
    connection: duckdb.DuckDBPyConnection,
    occurrence_root: Path,
    members: tuple[Path, Path, Path],
    output_root: Path,
) -> Path:
    output = output_root / "coverage" / "by-pbf" / "by-pbf.parquet"
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
    counts_row = connection.execute(
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
    if counts_row is None:
        raise AggregationError("DuckDB returned no source-count row")
    source_counts = {
        name: int(value or 0) for name, value in zip(_GROUP_METRICS, counts_row, strict=True)
    }
    overlap_rows = connection.execute(
        """
        SELECT overlap_category, COUNT(*)
        FROM read_parquet(?)
        GROUP BY overlap_category
        """,
        [str(compact)],
    ).fetchall()
    overlap_counts = {str(category): int(count) for category, count in overlap_rows}
    overlaps = [
        {
            "category": category,
            "count": overlap_counts.get(category, 0),
            "percentage": (overlap_counts.get(category, 0) / total * 100) if total else 0.0,
        }
        for category in EXPECTED_OVERLAP_CATEGORIES
    ]
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
    type_rows = connection.execute(
        "SELECT osm_type, COUNT(*) FROM read_parquet(?) GROUP BY osm_type ORDER BY osm_type",
        [str(compact)],
    ).fetchall()
    geometry_rows = connection.execute(
        "SELECT geometry_type, COUNT(*) "
        "FROM read_parquet(?) "
        "GROUP BY geometry_type ORDER BY geometry_type",
        [str(compact)],
    ).fetchall()
    pairwise = {
        "website_wikipedia": _fetch_count(
            connection,
            "SELECT COUNT(*) FROM read_parquet(?) WHERE website AND wikipedia",
            [str(compact)],
        ),
        "website_wikivoyage": _fetch_count(
            connection,
            "SELECT COUNT(*) FROM read_parquet(?) WHERE website AND wikivoyage",
            [str(compact)],
        ),
        "wikipedia_wikivoyage": _fetch_count(
            connection,
            "SELECT COUNT(*) FROM read_parquet(?) WHERE wikipedia AND wikivoyage",
            [str(compact)],
        ),
        "all_three": _fetch_count(
            connection,
            "SELECT COUNT(*) FROM read_parquet(?) WHERE website AND wikipedia AND wikivoyage",
            [str(compact)],
        ),
    }
    source_keys_not_in_raw = {
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
    failure_files = tuple(sorted(failure_root.glob("*.parquet"))) if failure_root.is_dir() else ()
    geometry_failure_count = (
        _fetch_count(
            connection,
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(failure_root / "*.parquet")],
        )
        if failure_files
        else 0
    )
    summary = {
        "valid_universe_count": total,
        **source_counts,
        "geometry_failure_count": geometry_failure_count,
        "source_keys_not_in_raw": source_keys_not_in_raw,
        "overlap_categories": overlaps,
        "pairwise_intersections": pairwise,
        "osm_type_counts": {str(kind): int(count) for kind, count in type_rows},
        "geometry_type_counts": {str(kind): int(count) for kind, count in geometry_rows},
        "area_statistics": {
            name: (None if value is None else float(value))
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
        },
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


def _write_rows(path: Path, schema: pa.Schema, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    if path.exists() or temporary.exists():
        raise FileExistsError(f"refusing to overwrite summary output: {path}")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary, compression="zstd")
    os.replace(temporary, path)


def _write_summary_outputs(
    connection: duckdb.DuckDBPyConnection,
    compact: Path,
    by_pbf: Path,
    output_root: Path,
    summary: dict[str, Any],
    occurrence_root: Path,
) -> tuple[Path, ...]:
    summaries_root = output_root / "summaries"
    metric_rows: list[dict[str, Any]] = []
    fixed_metrics = (
        "valid_universe_count",
        "website_count",
        "wikipedia_count",
        "wikivoyage_count",
        "covered_by_any_text_count",
        "geometry_failure_count",
    )
    metric_rows.extend(
        {
            "scope": "global",
            "group_name": "all",
            "metric": metric,
            "value": float(summary[metric]),
        }
        for metric in fixed_metrics
    )
    metric_rows.extend(
        {
            "scope": "area",
            "group_name": "all",
            "metric": metric,
            "value": float(value),
        }
        for metric, value in summary["area_statistics"].items()
        if value is not None
    )
    metric_rows.extend(
        {
            "scope": "source_audit",
            "group_name": source,
            "metric": "keys_not_in_raw_universe",
            "value": float(count),
        }
        for source, count in summary["source_keys_not_in_raw"].items()
    )
    metric_rows.extend(
        {
            "scope": "osm_type",
            "group_name": kind,
            "metric": "valid_polygon_count",
            "value": float(count),
        }
        for kind, count in summary["osm_type_counts"].items()
    )
    metric_rows.extend(
        {
            "scope": "geometry_type",
            "group_name": kind,
            "metric": "valid_polygon_count",
            "value": float(count),
        }
        for kind, count in summary["geometry_type_counts"].items()
    )
    global_summary = summaries_root / "global.parquet"
    _write_rows(global_summary, GLOBAL_SUMMARY_SCHEMA, metric_rows)
    overlap_summary = summaries_root / "by-overlap.parquet"
    _write_rows(
        overlap_summary,
        OVERLAP_SUMMARY_SCHEMA,
        [
            {
                "overlap_category": item["category"],
                "count": item["count"],
                "percentage": item["percentage"],
            }
            for item in summary["overlap_categories"]
        ],
    )
    by_pbf_summary = summaries_root / "by-source-pbf.parquet"
    _write_rows(by_pbf_summary, GROUP_SUMMARY_SCHEMA, _group_rows(connection, by_pbf, "source_pbf"))
    by_region_summary = summaries_root / "by-region.parquet"
    _write_rows(by_region_summary, GROUP_SUMMARY_SCHEMA, _group_rows(connection, compact, "region"))
    failure_summary = summaries_root / "geometry-failures.parquet"
    _write_failure_summary(
        connection, occurrence_root.parent / "geometry-failures", failure_summary
    )
    conflict_summary = summaries_root / "conflicts.parquet"
    _write_conflicts(connection, occurrence_root, conflict_summary)
    return (
        global_summary,
        by_pbf_summary,
        by_region_summary,
        overlap_summary,
        failure_summary,
        conflict_summary,
    )


def _write_failure_summary(
    connection: duckdb.DuckDBPyConnection, failure_root: Path, output: Path
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
    _write_rows(output, FAILURE_SUMMARY_SCHEMA, rows)


def _write_conflicts(
    connection: duckdb.DuckDBPyConnection, occurrence_root: Path, output: Path
) -> None:
    result = connection.execute(
        """
        SELECT
            osm_type,
            osm_id,
            COUNT(*) AS occurrence_count,
            COUNT(DISTINCT geometry_hash) AS distinct_geometry_count,
            CAST(to_json(list(source_pbf ORDER BY source_pbf)) AS VARCHAR) AS source_pbfs
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


def aggregate_run(
    *,
    occurrence_root: Path,
    membership_root: Path,
    output_root: Path | None = None,
) -> AggregationResult:
    """Aggregate occurrence and membership shards into compact coverage outputs."""

    _parquet_files(occurrence_root, "occurrence")
    members = _membership_paths(membership_root)
    target = occurrence_root.parent if output_root is None else output_root
    existing_outputs = tuple(
        target / name for name in ("coverage", "summaries", "scratch") if (target / name).exists()
    )
    if existing_outputs:
        names = ", ".join(str(path) for path in existing_outputs)
        raise AggregationError(f"aggregate output directories already exist: {names}")
    target.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(database=":memory:")
    try:
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
        summary_paths = _write_summary_outputs(
            connection, compact, by_pbf, target, summary, occurrence_root
        )
    finally:
        connection.close()
    return AggregationResult(target, global_paths, by_pbf, summary_paths, row_count, summary)
