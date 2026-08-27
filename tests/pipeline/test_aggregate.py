import json
from pathlib import Path
from typing import cast
from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.pipeline.aggregate as aggregate_module
from osm_polygon_wikidata_website_coverage.io.parquet import FAILURE_SCHEMA, OCCURRENCE_SCHEMA
from osm_polygon_wikidata_website_coverage.pipeline.aggregate import (
    CONFLICT_SCHEMA,
    EXPECTED_GLOBAL_COLUMNS,
    FAILURE_SUMMARY_SCHEMA,
    GLOBAL_SUMMARY_SCHEMA,
    GROUP_SUMMARY_SCHEMA,
    OVERLAP_SUMMARY_SCHEMA,
    AggregationError,
    _area_metric_rows,
    _area_statistics,
    _copy_query,
    _failure_count,
    _fetch_count,
    _fetch_summary,
    _fixed_metric_rows,
    _materialize_global,
    _membership_paths,
    _pairwise_intersections,
    _parquet_files,
    _rate,
    _source_audit,
    _source_audit_metric_rows,
    _source_counts,
    _sql_literal,
    _type_metric_rows,
    _validate_aggregate_target,
    _write_rows,
    aggregate_run,
    summarize_rows,
)
from osm_polygon_wikidata_website_coverage.pipeline.join import MEMBERSHIP_SCHEMA


def _occurrence(osm_id: int, source_pbf: str = "fixture-latest.osm.pbf") -> dict[str, object]:
    return {
        "osm_type": "way",
        "osm_id": osm_id,
        "source_pbf": source_pbf,
        "region": source_pbf.removesuffix("-latest.osm.pbf"),
        "osm_version": 1,
        "osm_timestamp": "2026-01-01T00:00:00Z",
        "relation_kind": None,
        "geometry_type": "Polygon",
        "geometry": '{"coordinates":[],"type":"Polygon"}',
        "centroid_lon": 0.5,
        "centroid_lat": 0.5,
        "bbox_min_lon": 0.0,
        "bbox_min_lat": 0.0,
        "bbox_max_lon": 1.0,
        "bbox_max_lat": 1.0,
        "area_m2": float(osm_id),
        "area_bucket": "under_1e3_m2",
        "geometry_hash": f"{osm_id:064d}",
    }


def _write_occurrences(root: Path) -> None:
    root.mkdir(parents=True)
    rows = [_occurrence(index) for index in range(1, 9)]
    rows.extend(
        [
            _occurrence(9, "b-latest.osm.pbf"),
            _occurrence(9, "a-latest.osm.pbf"),
        ]
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=OCCURRENCE_SCHEMA), root / "rows.parquet")


def _write_memberships(root: Path) -> None:
    root.mkdir(parents=True)
    membership = {
        "website": [2, 5, 6, 9],
        "wikipedia": [3, 5, 7, 9],
        "wikivoyage": [4, 6, 7, 8, 99],
    }
    for source, ids in membership.items():
        rows = [{"osm_type": "way", "osm_id": osm_id} for osm_id in ids]
        pq.write_table(
            pa.Table.from_pylist(rows, schema=MEMBERSHIP_SCHEMA), root / f"{source}.parquet"
        )


def test_aggregate_run_deduplicates_and_keeps_all_source_pbfs(tmp_path: Path) -> None:
    occurrence_root = tmp_path / "occurrences"
    membership_root = tmp_path / "members"
    _write_occurrences(occurrence_root)
    _write_memberships(membership_root)

    result = aggregate_run(
        occurrence_root=occurrence_root,
        membership_root=membership_root,
        output_root=tmp_path / "run",
    )

    global_rows = [row for path in result.global_paths for row in pq.read_table(path).to_pylist()]
    duplicate = next(row for row in global_rows if row["osm_id"] == 9)
    assert result.global_row_count == 9
    assert duplicate["source_pbf"] == "a-latest.osm.pbf"
    assert json.loads(duplicate["source_pbfs"]) == [
        "a-latest.osm.pbf",
        "b-latest.osm.pbf",
    ]
    assert "geometry" not in EXPECTED_GLOBAL_COLUMNS
    assert set(global_rows[0]) == set(EXPECTED_GLOBAL_COLUMNS)
    assert result.summary["valid_universe_count"] == 9
    assert sum(item["count"] for item in result.summary["overlap_categories"]) == 9
    assert (tmp_path / "run" / "summaries" / "conflicts.parquet").is_file()
    assert pq.read_table(tmp_path / "run" / "summaries" / "conflicts.parquet").num_rows == 1

    assert result.summary == {
        "area_statistics": {
            "max_m2": 9.0,
            "mean_m2": 5.0,
            "median_m2": 5.0,
            "min_m2": 1.0,
            "p25_m2": 3.0,
            "p75_m2": 7.0,
            "p95_m2": 8.6,
            "total_m2": 45.0,
        },
        "covered_by_any_text_count": 8,
        "geometry_failure_count": 0,
        "geometry_type_counts": {"Polygon": 9},
        "osm_type_counts": {"way": 9},
        "overlap_categories": [
            {"category": "neither", "count": 1, "percentage": 11.11111111111111},
            {"category": "website_only", "count": 1, "percentage": 11.11111111111111},
            {"category": "wikipedia_only", "count": 1, "percentage": 11.11111111111111},
            {"category": "wikivoyage_only", "count": 2, "percentage": 22.22222222222222},
            {
                "category": "website_wikipedia_only",
                "count": 2,
                "percentage": 22.22222222222222,
            },
            {
                "category": "website_wikivoyage_only",
                "count": 1,
                "percentage": 11.11111111111111,
            },
            {
                "category": "wikipedia_wikivoyage_only",
                "count": 1,
                "percentage": 11.11111111111111,
            },
            {"category": "all_three", "count": 0, "percentage": 0.0},
        ],
        "pairwise_intersections": {
            "all_three": 0,
            "website_wikipedia": 2,
            "website_wikivoyage": 1,
            "wikipedia_wikivoyage": 1,
        },
        "source_keys_not_in_raw": {"website": 0, "wikipedia": 0, "wikivoyage": 1},
        "valid_universe_count": 9,
        "website_count": 4,
        "wikipedia_count": 4,
        "wikivoyage_count": 4,
    }

    summary_rows = {path.name: pq.read_table(path).to_pylist() for path in result.summary_paths}
    assert result.summary_paths == (
        tmp_path / "run" / "summaries" / "global.parquet",
        tmp_path / "run" / "summaries" / "by-source-pbf.parquet",
        tmp_path / "run" / "summaries" / "by-region.parquet",
        tmp_path / "run" / "summaries" / "by-overlap.parquet",
        tmp_path / "run" / "summaries" / "geometry-failures.parquet",
        tmp_path / "run" / "summaries" / "conflicts.parquet",
    )
    assert pq.read_schema(result.summary_paths[0]) == GLOBAL_SUMMARY_SCHEMA
    assert pq.read_schema(result.summary_paths[1]) == GROUP_SUMMARY_SCHEMA
    assert pq.read_schema(result.summary_paths[2]) == GROUP_SUMMARY_SCHEMA
    assert pq.read_schema(result.summary_paths[3]) == OVERLAP_SUMMARY_SCHEMA
    assert pq.read_schema(result.summary_paths[4]) == FAILURE_SUMMARY_SCHEMA
    assert pq.read_schema(result.summary_paths[5]) == CONFLICT_SCHEMA
    assert summary_rows["by-source-pbf.parquet"] == [
        {
            "group_name": "a-latest.osm.pbf",
            "valid_polygon_count": 1,
            "website_count": 1,
            "wikipedia_count": 1,
            "wikivoyage_count": 0,
            "covered_by_any_text_count": 1,
            "website_rate": 100.0,
            "wikipedia_rate": 100.0,
            "wikivoyage_rate": 0.0,
            "covered_by_any_text_rate": 100.0,
        },
        {
            "group_name": "b-latest.osm.pbf",
            "valid_polygon_count": 1,
            "website_count": 1,
            "wikipedia_count": 1,
            "wikivoyage_count": 0,
            "covered_by_any_text_count": 1,
            "website_rate": 100.0,
            "wikipedia_rate": 100.0,
            "wikivoyage_rate": 0.0,
            "covered_by_any_text_rate": 100.0,
        },
        {
            "group_name": "fixture-latest.osm.pbf",
            "valid_polygon_count": 8,
            "website_count": 3,
            "wikipedia_count": 3,
            "wikivoyage_count": 4,
            "covered_by_any_text_count": 7,
            "website_rate": 37.5,
            "wikipedia_rate": 37.5,
            "wikivoyage_rate": 50.0,
            "covered_by_any_text_rate": 87.5,
        },
    ]
    assert summary_rows["by-region.parquet"] == [
        {
            "group_name": "a",
            "valid_polygon_count": 1,
            "website_count": 1,
            "wikipedia_count": 1,
            "wikivoyage_count": 0,
            "covered_by_any_text_count": 1,
            "website_rate": 100.0,
            "wikipedia_rate": 100.0,
            "wikivoyage_rate": 0.0,
            "covered_by_any_text_rate": 100.0,
        },
        {
            "group_name": "fixture",
            "valid_polygon_count": 8,
            "website_count": 3,
            "wikipedia_count": 3,
            "wikivoyage_count": 4,
            "covered_by_any_text_count": 7,
            "website_rate": 37.5,
            "wikipedia_rate": 37.5,
            "wikivoyage_rate": 50.0,
            "covered_by_any_text_rate": 87.5,
        },
    ]
    assert summary_rows["by-overlap.parquet"] == [
        {
            "overlap_category": item["category"],
            "count": item["count"],
            "percentage": item["percentage"],
        }
        for item in result.summary["overlap_categories"]
    ]
    assert summary_rows["geometry-failures.parquet"] == []
    assert summary_rows["conflicts.parquet"] == [
        {
            "osm_type": "way",
            "osm_id": 9,
            "occurrence_count": 2,
            "distinct_geometry_count": 1,
            "source_pbfs": '["a-latest.osm.pbf","b-latest.osm.pbf"]',
        }
    ]
    assert result.by_pbf_path == tmp_path / "run" / "coverage" / "by-pbf" / "by-pbf.parquet"
    assert (tmp_path / "run" / "scratch" / "global_compact.parquet").is_file()
    assert all(
        path.parent == tmp_path / "run" / "coverage" / "global" for path in result.global_paths
    )
    assert all(
        path.name.startswith("shard-") and path.suffix == ".parquet" for path in result.global_paths
    )


def test_summarize_rows_always_returns_all_eight_categories() -> None:
    rows = [
        {
            "website": website,
            "wikipedia": wikipedia,
            "wikivoyage": wikivoyage,
            "overlap_category": category,
            "osm_type": "way",
            "geometry_type": "Polygon",
            "area_m2": 1.0,
        }
        for category, (website, wikipedia, wikivoyage) in zip(
            (
                "neither",
                "website_only",
                "wikipedia_only",
                "wikivoyage_only",
                "website_wikipedia_only",
                "website_wikivoyage_only",
                "wikipedia_wikivoyage_only",
                "all_three",
            ),
            (
                (False, False, False),
                (True, False, False),
                (False, True, False),
                (False, False, True),
                (True, True, False),
                (True, False, True),
                (False, True, True),
                (True, True, True),
            ),
            strict=True,
        )
    ]

    summary = summarize_rows(rows)

    assert summary == {
        "valid_universe_count": 8,
        "overlap_categories": [
            {"category": category, "count": 1}
            for category in (
                "neither",
                "website_only",
                "wikipedia_only",
                "wikivoyage_only",
                "website_wikipedia_only",
                "website_wikivoyage_only",
                "wikipedia_wikivoyage_only",
                "all_three",
            )
        ],
    }
    duplicate_summary = summarize_rows(
        [{"overlap_category": "neither"}, {"overlap_category": "neither"}]
    )
    assert duplicate_summary["overlap_categories"][0] == {"category": "neither", "count": 2}


def test_aggregate_audits_out_of_universe_keys_and_geometry_failures(tmp_path: Path) -> None:
    occurrence_root = tmp_path / "occurrences"
    membership_root = tmp_path / "members"
    _write_occurrences(occurrence_root)
    _write_memberships(membership_root)
    failure_root = tmp_path / "geometry-failures"
    failure_root.mkdir()
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "osm_type": "relation",
                    "osm_id": 88,
                    "source_pbf": "fixture-latest.osm.pbf",
                    "candidate_kind": "boundary_relation",
                    "failure_kind": "invalid_geometry",
                    "message": "fixture",
                }
            ],
            schema=FAILURE_SCHEMA,
        ),
        failure_root / "fixture-00000.parquet",
    )

    result = aggregate_run(
        occurrence_root=occurrence_root,
        membership_root=membership_root,
        output_root=tmp_path / "run",
    )

    assert result.summary["source_keys_not_in_raw"] == {
        "website": 0,
        "wikipedia": 0,
        "wikivoyage": 1,
    }
    assert result.summary["geometry_failure_count"] == 1
    assert pq.read_table(tmp_path / "run" / "summaries" / "by-source-pbf.parquet").num_rows == 3
    assert pq.read_table(tmp_path / "run" / "summaries" / "by-region.parquet").num_rows == 2


def test_aggregate_rejects_missing_and_empty_occurrence_inputs(tmp_path: Path) -> None:
    membership_root = tmp_path / "members"
    _write_memberships(membership_root)

    with pytest.raises(AggregationError, match="directory is missing"):
        aggregate_run(
            occurrence_root=tmp_path / "missing",
            membership_root=membership_root,
            output_root=tmp_path / "run-missing",
        )

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(AggregationError, match="no Parquet files"):
        aggregate_run(
            occurrence_root=empty,
            membership_root=membership_root,
            output_root=tmp_path / "run-empty",
        )


def test_aggregate_rejects_missing_memberships_and_existing_output_directories(
    tmp_path: Path,
) -> None:
    occurrence_root = tmp_path / "occurrences"
    _write_occurrences(occurrence_root)
    missing_members = tmp_path / "missing-members"
    missing_members.mkdir()

    with pytest.raises(AggregationError, match="membership Parquets"):
        aggregate_run(
            occurrence_root=occurrence_root,
            membership_root=missing_members,
            output_root=tmp_path / "run-missing-members",
        )

    members = tmp_path / "members"
    _write_memberships(members)
    output = tmp_path / "existing-output"
    (output / "coverage").mkdir(parents=True)
    with pytest.raises(AggregationError, match="already exist"):
        aggregate_run(occurrence_root=occurrence_root, membership_root=members, output_root=output)


def test_aggregate_low_level_outputs_refuse_overwrites(tmp_path: Path) -> None:
    connection = duckdb.connect(database=":memory:")
    try:
        output = tmp_path / "output.parquet"
        output.write_bytes(b"existing")
        with pytest.raises(FileExistsError, match="aggregate output"):
            _copy_query(connection, "SELECT 1 AS value", [], output)

        temporary = tmp_path / ".temporary.parquet.tmp"
        temporary.write_bytes(b"existing")
        with pytest.raises(FileExistsError, match="aggregate output"):
            _copy_query(connection, "SELECT 1 AS value", [], tmp_path / "temporary.parquet")
    finally:
        connection.close()

    summary_path = tmp_path / "summary.parquet"
    _write_rows(summary_path, pa.schema([pa.field("value", pa.int64())]), [{"value": 1}])
    with pytest.raises(FileExistsError, match="summary output"):
        _write_rows(summary_path, pa.schema([pa.field("value", pa.int64())]), [{"value": 2}])


def test_aggregate_helpers_report_unexpected_duckdb_empty_rows() -> None:
    class EmptyResult:
        def fetchone(self) -> None:
            return None

    class EmptyConnection:
        def execute(self, query: str, parameters: list[object]) -> EmptyResult:
            return EmptyResult()

    with pytest.raises(AggregationError, match=r"\ADuckDB returned no count row\Z"):
        _fetch_count(cast(duckdb.DuckDBPyConnection, EmptyConnection()), "SELECT COUNT(*)", [])


def test_aggregate_summary_helpers_report_missing_summary_rows(tmp_path: Path) -> None:
    class Result:
        def __init__(self, one: object, many: list[object] | None = None) -> None:
            self.one = one
            self.many = [] if many is None else many

        def fetchone(self) -> object:
            return self.one

        def fetchall(self) -> list[object]:
            return self.many

    class MissingRowConnection:
        def execute(self, query: str, parameters: list[object]) -> Result:
            return Result(None)

    with pytest.raises(AggregationError, match=r"\ADuckDB returned no source-count row\Z"):
        _source_counts(
            cast(duckdb.DuckDBPyConnection, MissingRowConnection()),
            tmp_path / "compact.parquet",
        )
    with pytest.raises(AggregationError, match=r"\ADuckDB returned no area-statistics row\Z"):
        _area_statistics(
            cast(duckdb.DuckDBPyConnection, MissingRowConnection()),
            tmp_path / "compact.parquet",
        )


def test_summarize_rows_rejects_unknown_categories() -> None:
    with pytest.raises(ValueError, match="unknown overlap category"):
        summarize_rows([{"overlap_category": "unexpected"}])


def test_area_metric_rows_omit_null_statistics() -> None:
    rows = _area_metric_rows({"area_statistics": {"median_m2": 2.0, "p95_m2": None}})

    assert rows == [{"scope": "area", "group_name": "all", "metric": "median_m2", "value": 2.0}]


def test_aggregate_helpers_have_explicit_scalar_and_metric_contracts() -> None:
    summary = {
        "valid_universe_count": 10,
        "website_count": 20,
        "wikipedia_count": 30,
        "wikivoyage_count": 40,
        "covered_by_any_text_count": 50,
        "geometry_failure_count": 60,
        "area_statistics": {
            "total_m2": 1.0,
            "min_m2": 2.0,
            "max_m2": 3.0,
            "mean_m2": 4.0,
            "median_m2": 5.0,
            "p25_m2": 6.0,
            "p75_m2": 7.0,
            "p95_m2": 8.0,
        },
        "source_keys_not_in_raw": {"website": 9, "wikipedia": 8, "wikivoyage": 7},
        "osm_type_counts": {"relation": 2, "way": 3},
        "geometry_type_counts": {"MultiPolygon": 4, "Polygon": 5},
    }

    assert _rate(1, 0) == 0.0
    assert _rate(1, 4) == 25.0
    assert _sql_literal(Path("a'b")) == "'a''b'"
    assert _fixed_metric_rows(summary) == [
        {"scope": "global", "group_name": "all", "metric": metric, "value": float(value)}
        for metric, value in (
            ("valid_universe_count", 10),
            ("website_count", 20),
            ("wikipedia_count", 30),
            ("wikivoyage_count", 40),
            ("covered_by_any_text_count", 50),
            ("geometry_failure_count", 60),
        )
    ]
    assert _area_metric_rows(summary) == [
        {"scope": "area", "group_name": "all", "metric": metric, "value": value}
        for metric, value in (
            ("total_m2", 1.0),
            ("min_m2", 2.0),
            ("max_m2", 3.0),
            ("mean_m2", 4.0),
            ("median_m2", 5.0),
            ("p25_m2", 6.0),
            ("p75_m2", 7.0),
            ("p95_m2", 8.0),
        )
    ]
    assert _source_audit_metric_rows(summary) == [
        {
            "scope": "source_audit",
            "group_name": source,
            "metric": "keys_not_in_raw_universe",
            "value": float(count),
        }
        for source, count in (("website", 9), ("wikipedia", 8), ("wikivoyage", 7))
    ]
    assert _type_metric_rows(summary, "osm_type") == [
        {
            "scope": "osm_type",
            "group_name": kind,
            "metric": "valid_polygon_count",
            "value": float(count),
        }
        for kind, count in (("relation", 2), ("way", 3))
    ]
    assert _type_metric_rows(summary, "geometry_type") == [
        {
            "scope": "geometry_type",
            "group_name": kind,
            "metric": "valid_polygon_count",
            "value": float(count),
        }
        for kind, count in (("MultiPolygon", 4), ("Polygon", 5))
    ]


def test_aggregate_path_helpers_sort_and_fail_closed(tmp_path: Path) -> None:
    directory = tmp_path / "parquets"
    directory.mkdir()
    (directory / "b.parquet").write_bytes(b"")
    (directory / "a.parquet").write_bytes(b"")

    assert _parquet_files(directory, "fixture") == (
        directory / "a.parquet",
        directory / "b.parquet",
    )
    with pytest.raises(AggregationError, match="fixture directory is missing"):
        _parquet_files(tmp_path / "missing", "fixture")
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(AggregationError, match="contains no Parquet files"):
        _parquet_files(empty, "fixture")

    members = tmp_path / "members"
    members.mkdir()
    with pytest.raises(AggregationError) as missing_error:
        _membership_paths(members)
    assert str(missing_error.value) == (
        "membership Parquets are missing: "
        f"{members / 'website.parquet'}, "
        f"{members / 'wikipedia.parquet'}, "
        f"{members / 'wikivoyage.parquet'}"
    )
    expected = tuple(members / f"{name}.parquet" for name in ("website", "wikipedia", "wikivoyage"))
    for path in expected:
        path.write_bytes(b"")
    assert _membership_paths(members) == expected


def test_aggregate_parquet_sort_is_by_filename() -> None:
    class FakePath:
        def __init__(self, name: str, order: int) -> None:
            self.name = name
            self.order = order

        def __lt__(self, other: object) -> bool:
            assert isinstance(other, FakePath)
            return self.order < other.order

    class FakeDirectory:
        def is_dir(self) -> bool:
            return True

        def glob(self, pattern: str) -> tuple[FakePath, FakePath]:
            assert pattern == "*.parquet"
            return FakePath("b.parquet", 0), FakePath("a.parquet", 1)

    assert [path.name for path in _parquet_files(cast(Path, FakeDirectory()), "fixture")] == [
        "a.parquet",
        "b.parquet",
    ]


def test_aggregate_query_helpers_preserve_predicates_and_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compact = Path("a'b/compact.parquet")
    captured: list[tuple[str, list[object]]] = []

    def fake_fetch_count(
        connection: duckdb.DuckDBPyConnection, query: str, parameters: list[object]
    ) -> int:
        captured.append((query, parameters))
        return len(captured)

    monkeypatch.setattr(aggregate_module, "_fetch_count", fake_fetch_count)
    assert _pairwise_intersections(cast(duckdb.DuckDBPyConnection, object()), compact) == {
        "website_wikipedia": 1,
        "website_wikivoyage": 2,
        "wikipedia_wikivoyage": 3,
        "all_three": 4,
    }
    assert captured == [
        ("SELECT COUNT(*) FROM read_parquet(?) WHERE website AND wikipedia", [str(compact)]),
        ("SELECT COUNT(*) FROM read_parquet(?) WHERE website AND wikivoyage", [str(compact)]),
        ("SELECT COUNT(*) FROM read_parquet(?) WHERE wikipedia AND wikivoyage", [str(compact)]),
        (
            "SELECT COUNT(*) FROM read_parquet(?) WHERE website AND wikipedia AND wikivoyage",
            [str(compact)],
        ),
    ]


def test_aggregate_source_helpers_validate_row_shapes_and_values(tmp_path: Path) -> None:
    class Result:
        def __init__(self, row: object) -> None:
            self.row = row

        def fetchone(self) -> object:
            return self.row

    class Connection:
        def __init__(self, row: object) -> None:
            self.row = row

        def execute(self, query: str, parameters: list[object]) -> Result:
            return Result(self.row)

    path = tmp_path / "compact.parquet"
    assert _source_counts(cast(duckdb.DuckDBPyConnection, Connection((1, None, 3, 4))), path) == {
        "website_count": 1,
        "wikipedia_count": 0,
        "wikivoyage_count": 3,
        "covered_by_any_text_count": 4,
    }
    with pytest.raises(ValueError):
        _source_counts(cast(duckdb.DuckDBPyConnection, Connection((1, 2))), path)

    area = (1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0)
    assert _area_statistics(cast(duckdb.DuckDBPyConnection, Connection(area)), path) == {
        "total_m2": 1.0,
        "min_m2": 2.0,
        "max_m2": 3.0,
        "mean_m2": 4.0,
        "median_m2": 5.0,
        "p25_m2": 6.0,
        "p75_m2": 7.0,
        "p95_m2": 8.0,
    }
    with pytest.raises(ValueError):
        _area_statistics(cast(duckdb.DuckDBPyConnection, Connection((1.0,))), path)


def test_aggregate_source_audit_keeps_source_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, list[object]]] = []

    def fake_fetch_count(
        connection: duckdb.DuckDBPyConnection, query: str, parameters: list[object]
    ) -> int:
        captured.append((query, parameters))
        return len(captured) - 1

    monkeypatch.setattr(aggregate_module, "_fetch_count", fake_fetch_count)
    members = (Path("website.parquet"), Path("wikipedia.parquet"), Path("wikivoyage.parquet"))
    assert _source_audit(
        cast(duckdb.DuckDBPyConnection, object()), Path("compact.parquet"), members
    ) == {"website": 0, "wikipedia": 1, "wikivoyage": 2}
    assert [parameters for _, parameters in captured] == [
        ["website.parquet", "compact.parquet"],
        ["wikipedia.parquet", "compact.parquet"],
        ["wikivoyage.parquet", "compact.parquet"],
    ]
    with pytest.raises(ValueError):
        _source_audit(
            cast(duckdb.DuckDBPyConnection, object()),
            Path("compact.parquet"),
            cast(tuple[Path, Path, Path], members[:2]),
        )


def test_aggregate_target_rejects_each_reserved_output_directory(tmp_path: Path) -> None:
    for name in ("coverage", "summaries", "scratch"):
        target = tmp_path / name
        target.mkdir()
        with pytest.raises(AggregationError) as error:
            _validate_aggregate_target(tmp_path)
        assert str(error.value) == (f"aggregate output directories already exist: {target}")
        target.rmdir()


def test_aggregate_target_lists_multiple_existing_directories_in_order(tmp_path: Path) -> None:
    coverage = tmp_path / "coverage"
    summaries = tmp_path / "summaries"
    coverage.mkdir()
    summaries.mkdir()

    with pytest.raises(AggregationError) as error:
        _validate_aggregate_target(tmp_path)

    assert str(error.value) == (
        f"aggregate output directories already exist: {coverage}, {summaries}"
    )


def test_aggregate_failure_summary_writes_sorted_compact_rows(tmp_path: Path) -> None:
    failure_root = tmp_path / "failures"
    failure_root.mkdir()
    (failure_root / "fixture.parquet").write_bytes(b"placeholder")
    output = tmp_path / "nested" / "failures.parquet"

    class Result:
        def fetchall(self) -> list[tuple[str, str, int]]:
            return [("boundary_relation", "invalid_geometry", 2), ("closed_way", "bad", 1)]

    class Connection:
        def execute(self, query: str, parameters: list[object]) -> Result:
            return Result()

    aggregate_module._write_failure_summary(
        cast(duckdb.DuckDBPyConnection, Connection()), failure_root, output
    )
    assert pq.read_table(output).to_pylist() == [
        {"candidate_kind": "boundary_relation", "failure_kind": "invalid_geometry", "count": 2},
        {"candidate_kind": "closed_way", "failure_kind": "bad", "count": 1},
    ]


def test_aggregate_failure_count_uses_all_failure_shards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    failure_root = tmp_path / "failures"
    failure_root.mkdir()
    (failure_root / "b.parquet").write_bytes(b"")
    (failure_root / "a.parquet").write_bytes(b"")
    captured: list[tuple[str, list[object]]] = []

    def fake_fetch_count(
        connection: duckdb.DuckDBPyConnection, query: str, parameters: list[object]
    ) -> int:
        captured.append((query, parameters))
        return 2

    monkeypatch.setattr(aggregate_module, "_fetch_count", fake_fetch_count)
    assert _failure_count(cast(duckdb.DuckDBPyConnection, object()), failure_root) == 2
    assert captured == [
        (
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(failure_root / "*.parquet")],
        )
    ]


def test_aggregate_materialization_contract_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    copy_calls: list[tuple[str, list[object], Path]] = []
    execute_calls: list[tuple[str, list[object]]] = []
    count_calls: list[tuple[str, list[object]]] = []

    class Result:
        def fetchall(self) -> list[tuple[int]]:
            return [(1,), (3,)]

    class Connection:
        def execute(self, query: str, parameters: list[object]) -> Result:
            execute_calls.append((query, parameters))
            return Result()

    def fake_copy(
        connection: duckdb.DuckDBPyConnection,
        query: str,
        parameters: list[object],
        output_path: Path,
    ) -> None:
        copy_calls.append((query, parameters, output_path))

    def fake_fetch_count(
        connection: duckdb.DuckDBPyConnection, query: str, parameters: list[object]
    ) -> int:
        count_calls.append((query, parameters))
        return 9

    monkeypatch.setattr(aggregate_module, "_copy_query", fake_copy)
    monkeypatch.setattr(aggregate_module, "_fetch_count", fake_fetch_count)
    occurrence_root = Path("occurrences")
    members = (Path("website.parquet"), Path("wikipedia.parquet"), Path("wikivoyage.parquet"))

    result = _materialize_global(
        cast(duckdb.DuckDBPyConnection, Connection()), occurrence_root, members, Path("run")
    )

    compact = Path("run/scratch/global_compact.parquet")
    assert result == (
        (
            Path("run/coverage/global/shard-01.parquet"),
            Path("run/coverage/global/shard-03.parquet"),
        ),
        compact,
        9,
    )
    assert [output for _, _, output in copy_calls] == [
        compact,
        Path("run/coverage/global/shard-01.parquet"),
        Path("run/coverage/global/shard-03.parquet"),
    ]
    assert execute_calls == [
        (
            "SELECT DISTINCT shard_id FROM read_parquet(?) ORDER BY shard_id",
            [str(compact)],
        )
    ]
    assert count_calls == [("SELECT COUNT(*) FROM read_parquet(?)", [str(compact)])]


def test_aggregate_fetch_summary_calls_each_summary_query_with_exact_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    def fake_fetch_count(
        connection: duckdb.DuckDBPyConnection, query: str, parameters: list[object]
    ) -> int:
        calls.append(("count", (query, parameters)))
        return 2

    def fake_source_counts(connection: duckdb.DuckDBPyConnection, compact: Path) -> dict[str, int]:
        calls.append(("source", compact))
        return {
            "website_count": 3,
            "wikipedia_count": 4,
            "wikivoyage_count": 5,
            "covered_by_any_text_count": 6,
        }

    def fake_failure_count(connection: duckdb.DuckDBPyConnection, failure_root: Path) -> int:
        calls.append(("failure", failure_root))
        return 7

    def fake_source_audit(
        connection: duckdb.DuckDBPyConnection, compact: Path, members: tuple[Path, Path, Path]
    ) -> dict[str, int]:
        calls.append(("audit", (compact, members)))
        return {"website": 8, "wikipedia": 9, "wikivoyage": 10}

    def fake_overlap_summary(
        connection: duckdb.DuckDBPyConnection, compact: Path, total: int
    ) -> list[dict[str, object]]:
        calls.append(("overlap", (compact, total)))
        return []

    def fake_pairwise_intersections(
        connection: duckdb.DuckDBPyConnection, compact: Path
    ) -> dict[str, int]:
        calls.append(("pairs", compact))
        return {}

    def fake_type_counts(
        connection: duckdb.DuckDBPyConnection, compact: Path, column: str
    ) -> dict[str, int]:
        calls.append(("types", (compact, column)))
        return {}

    def fake_area_statistics(
        connection: duckdb.DuckDBPyConnection, compact: Path
    ) -> dict[str, float | None]:
        calls.append(("area", compact))
        return {}

    for name, value in (
        ("_fetch_count", fake_fetch_count),
        ("_source_counts", fake_source_counts),
        ("_failure_count", fake_failure_count),
        ("_source_audit", fake_source_audit),
        ("_overlap_summary", fake_overlap_summary),
        ("_pairwise_intersections", fake_pairwise_intersections),
        ("_type_counts", fake_type_counts),
        ("_area_statistics", fake_area_statistics),
    ):
        monkeypatch.setattr(aggregate_module, name, value)

    compact = Path("compact.parquet")
    members = (Path("website.parquet"), Path("wikipedia.parquet"), Path("wikivoyage.parquet"))
    failure_root = Path("failures")
    summary = _fetch_summary(
        cast(duckdb.DuckDBPyConnection, object()), compact, members, failure_root
    )

    assert summary == {
        "valid_universe_count": 2,
        "website_count": 3,
        "wikipedia_count": 4,
        "wikivoyage_count": 5,
        "covered_by_any_text_count": 6,
        "geometry_failure_count": 7,
        "source_keys_not_in_raw": {"website": 8, "wikipedia": 9, "wikivoyage": 10},
        "overlap_categories": [],
        "pairwise_intersections": {},
        "osm_type_counts": {},
        "geometry_type_counts": {},
        "area_statistics": {},
    }
    assert calls == [
        ("count", ("SELECT COUNT(*) FROM read_parquet(?)", [str(compact)])),
        ("source", compact),
        ("failure", failure_root),
        ("audit", (compact, members)),
        ("overlap", (compact, 2)),
        ("pairs", compact),
        ("types", (compact, "osm_type")),
        ("types", (compact, "geometry_type")),
        ("area", compact),
    ]


def test_aggregate_summary_outputs_forward_exact_paths_schemas_and_group_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_calls: list[tuple[Path, object, object]] = []
    group_calls: list[tuple[Path, str]] = []
    failure_calls: list[tuple[Path, Path]] = []
    conflict_calls: list[tuple[Path, Path]] = []

    def fake_write_rows(path: Path, schema: object, rows: object) -> None:
        write_calls.append((path, schema, rows))

    def fake_metric_rows(summary: dict[str, object]) -> list[str]:
        return ["metrics"]

    def fake_overlap_rows(summary: dict[str, object]) -> list[str]:
        return ["overlap"]

    def fake_group_rows(
        connection: duckdb.DuckDBPyConnection, table: Path, column: str
    ) -> list[str]:
        group_calls.append((table, column))
        return [column]

    def fake_failure_summary(
        connection: duckdb.DuckDBPyConnection, failure_root: Path, output: Path
    ) -> None:
        failure_calls.append((failure_root, output))

    def fake_conflicts(
        connection: duckdb.DuckDBPyConnection, occurrence_root: Path, output: Path
    ) -> None:
        conflict_calls.append((occurrence_root, output))

    monkeypatch.setattr(aggregate_module, "_write_rows", fake_write_rows)
    monkeypatch.setattr(aggregate_module, "_metric_rows", fake_metric_rows)
    monkeypatch.setattr(aggregate_module, "_overlap_rows", fake_overlap_rows)
    monkeypatch.setattr(aggregate_module, "_group_rows", fake_group_rows)
    monkeypatch.setattr(aggregate_module, "_write_failure_summary", fake_failure_summary)
    monkeypatch.setattr(aggregate_module, "_write_conflicts", fake_conflicts)

    output_root = Path("run")
    occurrence_root = output_root / "occurrences"
    result = aggregate_module._write_summary_outputs(
        cast(duckdb.DuckDBPyConnection, object()),
        Path("compact.parquet"),
        Path("by-pbf.parquet"),
        output_root,
        {},
        occurrence_root,
    )

    assert result == (
        Path("run/summaries/global.parquet"),
        Path("run/summaries/by-source-pbf.parquet"),
        Path("run/summaries/by-region.parquet"),
        Path("run/summaries/by-overlap.parquet"),
        Path("run/summaries/geometry-failures.parquet"),
        Path("run/summaries/conflicts.parquet"),
    )
    assert write_calls == [
        (Path("run/summaries/global.parquet"), GLOBAL_SUMMARY_SCHEMA, ["metrics"]),
        (Path("run/summaries/by-overlap.parquet"), OVERLAP_SUMMARY_SCHEMA, ["overlap"]),
        (
            Path("run/summaries/by-source-pbf.parquet"),
            GROUP_SUMMARY_SCHEMA,
            ["source_pbf"],
        ),
        (Path("run/summaries/by-region.parquet"), GROUP_SUMMARY_SCHEMA, ["region"]),
    ]
    assert group_calls == [
        (Path("by-pbf.parquet"), "source_pbf"),
        (Path("compact.parquet"), "region"),
    ]
    assert failure_calls == [
        (Path("run/geometry-failures"), Path("run/summaries/geometry-failures.parquet"))
    ]
    assert conflict_calls == [(occurrence_root, Path("run/summaries/conflicts.parquet"))]


def test_aggregate_conflicts_use_the_exact_conflict_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Result:
        def fetchall(self) -> list[tuple[object, ...]]:
            return []

    class Connection:
        def execute(self, query: str, parameters: list[object]) -> Result:
            return Result()

    captured: list[tuple[Path, object, object]] = []

    def fake_write_rows(path: Path, schema: object, rows: object) -> None:
        captured.append((path, schema, rows))

    monkeypatch.setattr(aggregate_module, "_write_rows", fake_write_rows)
    aggregate_module._write_conflicts(
        cast(duckdb.DuckDBPyConnection, Connection()),
        Path("occurrences"),
        Path("summaries/conflicts.parquet"),
    )

    assert captured == [(Path("summaries/conflicts.parquet"), CONFLICT_SCHEMA, [])]


def test_aggregate_run_forwards_exact_setup_and_stage_contracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    occurrence_root = tmp_path / "occurrences"
    membership_root = tmp_path / "members"
    occurrence_root.mkdir()
    membership_root.mkdir()
    (occurrence_root / "one.parquet").write_bytes(b"occurrence")
    for name in ("website", "wikipedia", "wikivoyage"):
        (membership_root / f"{name}.parquet").write_bytes(b"membership")
    output_root = tmp_path / "nested" / "run"
    calls: list[tuple[str, object]] = []

    class Connection:
        def close(self) -> None:
            calls.append(("close", None))

    connection = Connection()

    def fake_connect(*, database: str) -> Connection:
        calls.append(("connect", database))
        return connection

    def fake_global(
        connection_value: duckdb.DuckDBPyConnection,
        occurrence_value: Path,
        members: tuple[Path, Path, Path],
        target: Path,
    ) -> tuple[tuple[Path, ...], Path, int]:
        calls.append(("global", (occurrence_value, members, target)))
        return (target / "global.parquet",), target / "compact.parquet", 3

    def fake_by_pbf(
        connection_value: duckdb.DuckDBPyConnection,
        occurrence_value: Path,
        members: tuple[Path, Path, Path],
        target: Path,
    ) -> Path:
        calls.append(("by-pbf", (occurrence_value, members, target)))
        return target / "by-pbf.parquet"

    def fake_fetch(
        connection_value: duckdb.DuckDBPyConnection,
        compact: Path,
        members: tuple[Path, Path, Path],
        failure_root: Path,
    ) -> dict[str, object]:
        calls.append(("summary", (compact, members, failure_root)))
        return {"valid_universe_count": 3}

    def fake_summaries(
        connection_value: duckdb.DuckDBPyConnection,
        compact: Path,
        by_pbf: Path,
        target: Path,
        summary: dict[str, object],
        occurrence_value: Path,
    ) -> tuple[Path, ...]:
        calls.append(("summaries", (compact, by_pbf, target, summary, occurrence_value)))
        return (target / "summary.parquet",)

    monkeypatch.setattr(aggregate_module.duckdb, "connect", fake_connect)
    monkeypatch.setattr(aggregate_module, "_materialize_global", fake_global)
    monkeypatch.setattr(aggregate_module, "_materialize_by_pbf", fake_by_pbf)
    monkeypatch.setattr(aggregate_module, "_fetch_summary", fake_fetch)
    monkeypatch.setattr(aggregate_module, "_write_summary_outputs", fake_summaries)
    original_mkdir = Path.mkdir
    mkdir_calls: list[tuple[Path, tuple[object, ...], dict[str, object]]] = []

    def tracked_mkdir(
        path: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        if path == output_root and parents:
            mkdir_calls.append((path, (), {"parents": parents, "exist_ok": exist_ok}))
        original_mkdir(path, mode=mode, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", tracked_mkdir)

    result = aggregate_run(
        occurrence_root=occurrence_root,
        membership_root=membership_root,
        output_root=output_root,
    )

    members = tuple(
        membership_root / f"{name}.parquet" for name in ("website", "wikipedia", "wikivoyage")
    )
    assert mkdir_calls == [(output_root, (), {"parents": True, "exist_ok": True})]
    assert calls == [
        ("connect", ":memory:"),
        ("global", (occurrence_root, members, output_root)),
        ("by-pbf", (occurrence_root, members, output_root)),
        (
            "summary",
            (
                output_root / "compact.parquet",
                members,
                occurrence_root.parent / "geometry-failures",
            ),
        ),
        (
            "summaries",
            (
                output_root / "compact.parquet",
                output_root / "by-pbf.parquet",
                output_root,
                {"valid_universe_count": 3},
                occurrence_root,
            ),
        ),
        ("close", None),
    ]
    assert result == aggregate_module.AggregationResult(
        output_root,
        (output_root / "global.parquet",),
        output_root / "by-pbf.parquet",
        (output_root / "summary.parquet",),
        3,
        {"valid_universe_count": 3},
    )


def test_aggregate_write_rows_preserves_nested_schema_and_compression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = Mock(wraps=aggregate_module.pq.write_table)
    monkeypatch.setattr(aggregate_module.pq, "write_table", writer)
    schema = pa.schema([pa.field("value", pa.int64())])
    output = tmp_path / "nested" / "deeper" / "summary.parquet"

    _write_rows(output, schema, [])

    assert output.is_file()
    assert writer.call_args is not None
    assert writer.call_args.args[0].schema == schema
    assert writer.call_args.args[1] == output.with_name(".summary.parquet.tmp")
    assert writer.call_args.kwargs == {"compression": "zstd"}


def test_aggregate_input_validation_uses_the_occurrence_label(tmp_path: Path) -> None:
    with pytest.raises(AggregationError) as error:
        aggregate_module._validate_aggregate_inputs(tmp_path / "missing", tmp_path / "members")
    assert str(error.value) == f"occurrence directory is missing: {tmp_path / 'missing'}"


def test_aggregate_run_defaults_output_to_occurrence_parent(tmp_path: Path) -> None:
    occurrence_root = tmp_path / "occurrences"
    membership_root = tmp_path / "members"
    _write_occurrences(occurrence_root)
    _write_memberships(membership_root)

    result = aggregate_run(occurrence_root=occurrence_root, membership_root=membership_root)

    assert result.output_root == tmp_path
