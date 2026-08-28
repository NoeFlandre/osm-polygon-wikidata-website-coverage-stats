import inspect
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
    COMPACT_GLOBAL_SCHEMA,
    CONFLICT_SCHEMA,
    EXPECTED_GLOBAL_COLUMNS,
    FAILURE_SUMMARY_SCHEMA,
    GLOBAL_SUMMARY_SCHEMA,
    GROUP_METRIC_SCHEMA,
    GROUP_SUMMARY_SCHEMA,
    OVERLAP_SUMMARY_SCHEMA,
    AggregationError,
    _area_metric_rows,
    _area_statistics,
    _copy_query,
    _existing_output_directories,
    _failure_count,
    _fetch_count,
    _fetch_summary,
    _fixed_metric_rows,
    _group_area_metric_rows,
    _group_base_rows,
    _group_fixed_metric_rows,
    _group_metric_rows,
    _group_overlap_metric_rows,
    _group_type_metric_rows,
    _materialize_by_pbf,
    _materialize_global,
    _membership_paths,
    _pairwise_intersections,
    _parquet_files,
    _partition_occurrences,
    _rate,
    _source_audit,
    _source_audit_metric_rows,
    _source_counts,
    _sql_literal,
    _type_metric_rows,
    _validate_aggregate_target,
    _write_conflicts,
    _write_failure_summary,
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
    assert duplicate["contributing_pbf_count"] == 2
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
        tmp_path / "run" / "summaries" / "by-source-pbf-metrics.parquet",
        tmp_path / "run" / "summaries" / "by-region-metrics.parquet",
        tmp_path / "run" / "summaries" / "by-overlap.parquet",
        tmp_path / "run" / "summaries" / "geometry-failures.parquet",
        tmp_path / "run" / "summaries" / "conflicts.parquet",
    )
    assert pq.read_schema(result.summary_paths[0]) == GLOBAL_SUMMARY_SCHEMA
    assert pq.read_schema(result.summary_paths[1]) == GROUP_SUMMARY_SCHEMA
    assert pq.read_schema(result.summary_paths[2]) == GROUP_SUMMARY_SCHEMA
    assert pq.read_schema(result.summary_paths[3]) == GROUP_METRIC_SCHEMA
    assert pq.read_schema(result.summary_paths[4]) == GROUP_METRIC_SCHEMA
    assert pq.read_schema(result.summary_paths[5]) == OVERLAP_SUMMARY_SCHEMA
    assert pq.read_schema(result.summary_paths[6]) == FAILURE_SUMMARY_SCHEMA
    assert pq.read_schema(result.summary_paths[7]) == CONFLICT_SCHEMA
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
    source_metrics = pq.read_table(
        tmp_path / "run" / "summaries" / "by-source-pbf-metrics.parquet"
    ).to_pylist()
    assert {
        (row["group_name"], row["metric"], row["value"])
        for row in source_metrics
        if row["group_name"] == "fixture-latest.osm.pbf"
        and row["metric"] in {"overlap_count:neither", "area_total_m2", "osm_type_count:way"}
    } == {
        ("fixture-latest.osm.pbf", "overlap_count:neither", 1.0),
        ("fixture-latest.osm.pbf", "area_total_m2", 36.0),
        ("fixture-latest.osm.pbf", "osm_type_count:way", 8.0),
    }
    fixture_overlap = {
        row["metric"]: row["value"]
        for row in source_metrics
        if row["group_name"] == "fixture-latest.osm.pbf"
        and str(row["metric"]).startswith("overlap_count:")
    }
    assert set(fixture_overlap) == {
        f"overlap_count:{category}"
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
    }
    assert fixture_overlap["overlap_count:all_three"] == 0.0
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
    assert not (tmp_path / "run" / "scratch" / "duckdb-temp").exists()
    assert all(
        path.parent == tmp_path / "run" / "coverage" / "global" for path in result.global_paths
    )
    assert all(
        path.name.startswith("shard-") and path.suffix == ".parquet" for path in result.global_paths
    )
    assert len(result.global_paths) == 64
    assert [path.name for path in result.global_paths] == [
        f"shard-{index:02d}.parquet" for index in range(64)
    ]


def test_global_source_provenance_is_distinct_and_counted(tmp_path: Path) -> None:
    occurrence_root = tmp_path / "occurrences"
    membership_root = tmp_path / "members"
    occurrence_root.mkdir()
    _write_memberships(membership_root)
    rows = [_occurrence(1), _occurrence(1), _occurrence(2, "other-latest.osm.pbf")]
    pq.write_table(
        pa.Table.from_pylist(rows, schema=OCCURRENCE_SCHEMA), occurrence_root / "rows.parquet"
    )

    result = aggregate_run(
        occurrence_root=occurrence_root,
        membership_root=membership_root,
        output_root=tmp_path / "run",
    )

    row = next(
        row
        for path in result.global_paths
        for row in pq.read_table(path).to_pylist()
        if row["osm_id"] == 1
    )
    assert row["contributing_pbf_count"] == 1
    assert json.loads(row["source_pbfs"]) == ["fixture-latest.osm.pbf"]


def test_aggregate_preserves_a_zero_polygon_universe_with_empty_shards(tmp_path: Path) -> None:
    occurrence_root = tmp_path / "occurrences"
    occurrence_root.mkdir()
    pq.write_table(
        pa.Table.from_pylist([], schema=OCCURRENCE_SCHEMA),
        occurrence_root / "empty-latest.osm.pbf-00000.parquet",
    )
    membership_root = tmp_path / "members"
    membership_root.mkdir()
    for source in ("website", "wikipedia", "wikivoyage"):
        pq.write_table(
            pa.Table.from_pylist([], schema=MEMBERSHIP_SCHEMA),
            membership_root / f"{source}.parquet",
        )

    result = aggregate_run(
        occurrence_root=occurrence_root,
        membership_root=membership_root,
        output_root=tmp_path / "run",
    )

    assert result.global_row_count == 0
    assert len(result.global_paths) == 64
    assert all(pq.read_table(path).num_rows == 0 for path in result.global_paths)
    assert result.summary["valid_universe_count"] == 0
    assert result.summary["area_statistics"] == {
        "total_m2": None,
        "min_m2": None,
        "max_m2": None,
        "mean_m2": None,
        "median_m2": None,
        "p25_m2": None,
        "p75_m2": None,
        "p95_m2": None,
    }


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


def test_aggregate_resume_replaces_an_existing_derived_stage(tmp_path: Path) -> None:
    occurrence_root = tmp_path / "occurrences"
    _write_occurrences(occurrence_root)
    members = tmp_path / "members"
    _write_memberships(members)
    failure_root = tmp_path / "geometry-failures"
    failure_root.mkdir()
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "osm_type": "way",
                    "osm_id": 10,
                    "source_pbf": "fixture-latest.osm.pbf",
                    "candidate_kind": "closed_way",
                    "failure_kind": "invalid_geometry",
                    "message": "fixture",
                }
            ],
            schema=FAILURE_SCHEMA,
        ),
        failure_root / "fixture-00000.parquet",
    )
    output = tmp_path / "run"

    first = aggregate_run(
        occurrence_root=occurrence_root,
        membership_root=members,
        output_root=output,
    )
    (output / "coverage" / "global" / "shard-00.parquet").write_bytes(b"partial")

    resumed = aggregate_run(
        occurrence_root=occurrence_root,
        membership_root=members,
        output_root=output,
        resume=True,
    )

    assert resumed.global_row_count == first.global_row_count
    assert pq.read_table(output / "coverage" / "global" / "shard-00.parquet").schema == (
        COMPACT_GLOBAL_SCHEMA
    )
    assert resumed.by_pbf_path == first.by_pbf_path
    assert resumed.summary_paths == first.summary_paths
    assert resumed.summary == first.summary
    assert pq.read_schema(output / "coverage" / "by-pbf" / "by-pbf.parquet") == pq.read_schema(
        first.by_pbf_path
    )
    expected_summary_schemas = (
        GLOBAL_SUMMARY_SCHEMA,
        GROUP_SUMMARY_SCHEMA,
        GROUP_SUMMARY_SCHEMA,
        GROUP_METRIC_SCHEMA,
        GROUP_METRIC_SCHEMA,
        OVERLAP_SUMMARY_SCHEMA,
        FAILURE_SUMMARY_SCHEMA,
        CONFLICT_SCHEMA,
    )
    for path, expected_schema in zip(resumed.summary_paths, expected_summary_schemas, strict=True):
        assert pq.read_schema(path) == expected_schema
    assert pq.read_table(output / "summaries" / "geometry-failures.parquet").to_pylist() == [
        {"candidate_kind": "closed_way", "failure_kind": "invalid_geometry", "count": 1}
    ]


def test_aggregate_materialization_preserves_the_exact_shard_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[str, list[object], Path, bool]] = []
    partition_calls: list[tuple[Path, Path, bool]] = []

    def copy_query(
        connection: object,
        query: str,
        parameters: list[object],
        output_path: Path,
        *,
        replace_existing: bool = False,
    ) -> None:
        del connection
        calls.append((query, parameters, output_path, replace_existing))

    monkeypatch.setattr(aggregate_module, "_copy_query", copy_query)
    monkeypatch.setattr(aggregate_module, "_fetch_count", lambda connection, query, parameters: 10)

    def partition_occurrences(
        connection: object,
        occurrence_root: Path,
        output_root: Path,
        *,
        replace_existing: bool = False,
    ) -> Path:
        del connection
        partition_calls.append((occurrence_root, output_root, replace_existing))
        return tmp_path / "run" / "scratch" / "occurrence-buckets"

    monkeypatch.setattr(aggregate_module, "_partition_occurrences", partition_occurrences)
    occurrence_root = tmp_path / "occurrences"
    members = (
        tmp_path / "website.parquet",
        tmp_path / "wikipedia.parquet",
        tmp_path / "wikivoyage.parquet",
    )
    result = _materialize_global(
        cast(duckdb.DuckDBPyConnection, object()),
        occurrence_root,
        members,
        tmp_path / "run",
        replace_existing=True,
    )

    expected_shard_query = (
        "SELECT "
        + ", ".join(EXPECTED_GLOBAL_COLUMNS)
        + " FROM read_parquet(?) WHERE shard_id = ? ORDER BY osm_type, osm_id"
    )
    global_part = tmp_path / "run" / "scratch" / "global_shard.parquet"
    for index in range(64):
        global_call, shard_call = calls[index * 2 : index * 2 + 2]
        assert global_call == (
            aggregate_module._GLOBAL_SQL,
            [
                str(
                    tmp_path
                    / "run"
                    / "scratch"
                    / "occurrence-buckets"
                    / f"shard_id={index}"
                    / "*.parquet"
                ),
                *(str(path) for path in members),
            ],
            global_part,
            True,
        )
        assert shard_call == (
            expected_shard_query,
            [str(global_part), index],
            tmp_path / "run" / "coverage" / "global" / f"shard-{index:02d}.parquet",
            True,
        )
    assert calls[-1] == (
        aggregate_module._COMPACT_FROM_GLOBAL_SQL,
        [str(tmp_path / "run" / "coverage" / "global" / "*.parquet")],
        tmp_path / "run" / "scratch" / "global_compact.parquet",
        True,
    )
    assert partition_calls == [(occurrence_root, tmp_path / "run", True)]
    assert result[2] == 10


def test_aggregate_low_level_materializers_refuse_existing_outputs_by_default(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "run"
    output_root.mkdir()
    output = output_root / "coverage" / "by-pbf" / "by-pbf.parquet"
    output.parent.mkdir(parents=True)
    output.write_bytes(b"existing")
    by_pbf_connection = duckdb.connect(database=":memory:")
    try:
        with pytest.raises(FileExistsError, match="aggregate output"):
            _materialize_by_pbf(
                by_pbf_connection,
                tmp_path / "occurrences",
                (tmp_path / "website", tmp_path / "wikipedia", tmp_path / "wikivoyage"),
                output_root,
            )
    finally:
        by_pbf_connection.close()

    failure_output = tmp_path / "failure-summary.parquet"
    failure_output.write_bytes(b"existing")
    failure_connection = duckdb.connect(database=":memory:")
    try:
        with pytest.raises(FileExistsError, match="summary output"):
            _write_failure_summary(
                failure_connection,
                tmp_path / "missing-failures",
                failure_output,
            )
    finally:
        failure_connection.close()

    occurrence_root = tmp_path / "valid-occurrences"
    _write_occurrences(occurrence_root)
    conflict_output = tmp_path / "conflicts.parquet"
    conflict_output.write_bytes(b"existing")
    conflict_connection = duckdb.connect(database=":memory:")
    try:
        with pytest.raises(FileExistsError, match="summary output"):
            _write_conflicts(conflict_connection, occurrence_root, conflict_output)
    finally:
        conflict_connection.close()


def test_aggregate_output_directory_contract_lists_all_reserved_directories(tmp_path: Path) -> None:
    target = tmp_path / "target"
    (target / "coverage").mkdir(parents=True)
    (target / "summaries").mkdir()
    (target / "scratch").mkdir()

    assert _existing_output_directories(target) == (
        target / "coverage",
        target / "summaries",
        target / "scratch",
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


def test_group_fixed_metrics_emit_each_count_and_rate_with_normalized_values() -> None:
    rows = _group_fixed_metric_rows(
        [
            ("region-b", "8", "2", "3", "1", "5"),
            (42, "4", "1", "0", "2", "3"),
        ],
        "region",
    )

    assert rows == [
        {
            "scope": "region",
            "group_name": "region-b",
            "metric": metric,
            "value": value,
        }
        for metric, value in (
            ("valid_polygon_count", 8.0),
            ("website_count", 2.0),
            ("wikipedia_count", 3.0),
            ("wikivoyage_count", 1.0),
            ("covered_by_any_text_count", 5.0),
            ("website_rate", 25.0),
            ("wikipedia_rate", 37.5),
            ("wikivoyage_rate", 12.5),
            ("covered_by_any_text_rate", 62.5),
        )
    ] + [
        {
            "scope": "region",
            "group_name": "42",
            "metric": metric,
            "value": value,
        }
        for metric, value in (
            ("valid_polygon_count", 4.0),
            ("website_count", 1.0),
            ("wikipedia_count", 0.0),
            ("wikivoyage_count", 2.0),
            ("covered_by_any_text_count", 3.0),
            ("website_rate", 25.0),
            ("wikipedia_rate", 0.0),
            ("wikivoyage_rate", 50.0),
            ("covered_by_any_text_rate", 75.0),
        )
    ]


class _FetchAllConnection:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, list[object]]] = []

    def execute(self, query: str, parameters: list[object]) -> "_FetchAllConnection":
        self.calls.append((query, parameters))
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


def test_group_base_and_overlap_metrics_preserve_group_keys_and_zero_categories(
    tmp_path: Path,
) -> None:
    table = tmp_path / "compact.parquet"
    base_connection = _FetchAllConnection([("alpha", 4, 2, 1, 1, 3)])
    assert _group_base_rows(cast(duckdb.DuckDBPyConnection, base_connection), table, "region") == [
        ("alpha", 4, 2, 1, 1, 3)
    ]
    assert base_connection.calls[0][1] == [str(table)]

    overlap_connection = _FetchAllConnection(
        [("alpha", "neither", 2), ("alpha", "website_only", 1), ("beta", "all_three", 1)]
    )
    rows = _group_overlap_metric_rows(
        cast(duckdb.DuckDBPyConnection, overlap_connection),
        table,
        "region",
        "region",
        {"alpha": 4, "beta": 2},
    )

    assert len(rows) == 32
    assert all(row["scope"] == "region" for row in rows)
    values = {(row["group_name"], row["metric"]): row["value"] for row in rows}
    assert values[("alpha", "overlap_count:neither")] == 2.0
    assert values[("alpha", "overlap_count:website_only")] == 1.0
    assert values[("alpha", "overlap_count:all_three")] == 0.0
    assert values[("alpha", "overlap_rate:neither")] == 50.0
    assert values[("beta", "overlap_count:all_three")] == 1.0
    assert values[("beta", "overlap_rate:all_three")] == 50.0
    assert values[("beta", "overlap_count:neither")] == 0.0
    assert overlap_connection.calls[0][1] == [str(table)]


def test_group_area_metrics_keep_all_named_statistics_and_omit_nulls(tmp_path: Path) -> None:
    table = tmp_path / "compact.parquet"
    connection = _FetchAllConnection(
        [
            ("alpha", 1, 2, 3, 4, 5, 6, 7, 8),
            ("beta", 10, None, 12, None, 14, 15, None, 16),
        ]
    )

    assert _group_area_metric_rows(
        cast(duckdb.DuckDBPyConnection, connection), table, "region", "region"
    ) == [
        {
            "scope": "region",
            "group_name": group,
            "metric": metric,
            "value": value,
        }
        for group, values in (
            ("alpha", (1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0)),
            ("beta", (10.0, 12.0, 14.0, 15.0, 16.0)),
        )
        for metric, value in zip(
            (
                "area_total_m2",
                "area_min_m2",
                "area_max_m2",
                "area_mean_m2",
                "area_median_m2",
                "area_p25_m2",
                "area_p75_m2",
                "area_p95_m2",
            )
            if group == "alpha"
            else ("area_total_m2", "area_max_m2", "area_median_m2", "area_p25_m2", "area_p95_m2"),
            values,
            strict=True,
        )
    ]
    assert connection.calls[0][1] == [str(table)]


def test_group_area_metrics_reject_short_database_rows(tmp_path: Path) -> None:
    table = tmp_path / "compact.parquet"
    connection = _FetchAllConnection([("alpha", 1)])

    with pytest.raises(ValueError, match=r"zip\(\) argument 2 is shorter than argument 1"):
        _group_area_metric_rows(
            cast(duckdb.DuckDBPyConnection, connection), table, "region", "region"
        )


def test_group_type_metrics_emit_both_type_dimensions_with_their_prefixes(
    tmp_path: Path,
) -> None:
    table = tmp_path / "compact.parquet"

    class TypeConnection(_FetchAllConnection):
        def execute(self, query: str, parameters: list[object]) -> "TypeConnection":
            self.calls.append((query, parameters))
            self.rows = (
                [("alpha", "way", 3), ("beta", "relation", 1)]
                if "osm_type" in query
                else [("alpha", "Polygon", 4), ("beta", "MultiPolygon", 1)]
            )
            return self

    connection = TypeConnection([])
    assert _group_type_metric_rows(
        cast(duckdb.DuckDBPyConnection, connection), table, "region", "region"
    ) == [
        {"scope": "region", "group_name": "alpha", "metric": "osm_type_count:way", "value": 3.0},
        {
            "scope": "region",
            "group_name": "beta",
            "metric": "osm_type_count:relation",
            "value": 1.0,
        },
        {
            "scope": "region",
            "group_name": "alpha",
            "metric": "geometry_type_count:Polygon",
            "value": 4.0,
        },
        {
            "scope": "region",
            "group_name": "beta",
            "metric": "geometry_type_count:MultiPolygon",
            "value": 1.0,
        },
    ]
    assert [parameters for _, parameters in connection.calls] == [[str(table)], [str(table)]]
    assert "osm_type" in connection.calls[0][0]
    assert "geometry_type" in connection.calls[1][0]


def test_group_metric_rows_composes_each_detail_layer_and_passes_derived_totals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_rows: list[tuple[object, ...]] = [("alpha", 3, 1, 2, 0, 2)]
    calls: list[tuple[str, object]] = []
    fixed: list[dict[str, object]] = [{"metric": "fixed"}]
    overlap: list[dict[str, object]] = [{"metric": "overlap"}]
    area: list[dict[str, object]] = [{"metric": "area"}]
    types: list[dict[str, object]] = [{"metric": "type"}]

    def fake_base(connection: object, table: Path, group_column: str) -> list[tuple[object, ...]]:
        calls.append(("base", (connection, table, group_column)))
        return base_rows

    def fake_fixed(rows: list[tuple[object, ...]], scope: str) -> list[dict[str, object]]:
        calls.append(("fixed", (rows, scope)))
        return fixed

    def fake_overlap(
        connection: object,
        table: Path,
        group_column: str,
        scope: str,
        totals: dict[str, int],
    ) -> list[dict[str, object]]:
        calls.append(("overlap", (connection, table, group_column, scope, totals)))
        return overlap

    def fake_area(
        connection: object, table: Path, group_column: str, scope: str
    ) -> list[dict[str, object]]:
        calls.append(("area", (connection, table, group_column, scope)))
        return area

    def fake_types(
        connection: object, table: Path, group_column: str, scope: str
    ) -> list[dict[str, object]]:
        calls.append(("types", (connection, table, group_column, scope)))
        return types

    monkeypatch.setattr(aggregate_module, "_group_base_rows", fake_base)
    monkeypatch.setattr(aggregate_module, "_group_fixed_metric_rows", fake_fixed)
    monkeypatch.setattr(aggregate_module, "_group_overlap_metric_rows", fake_overlap)
    monkeypatch.setattr(aggregate_module, "_group_area_metric_rows", fake_area)
    monkeypatch.setattr(aggregate_module, "_group_type_metric_rows", fake_types)

    connection = cast(duckdb.DuckDBPyConnection, object())
    table = Path("coverage.parquet")
    assert _group_metric_rows(connection, table, "region", "region") == [
        *fixed,
        *overlap,
        *area,
        *types,
    ]
    assert calls == [
        ("base", (connection, table, "region")),
        ("fixed", (base_rows, "region")),
        ("overlap", (connection, table, "region", "region", {"alpha": 3})),
        ("area", (connection, table, "region", "region")),
        ("types", (connection, table, "region", "region")),
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


def test_aggregate_resume_summary_writers_forward_exact_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[Path, object, object, bool]] = []

    def fake_write_rows(
        path: Path, schema: object, rows: object, *, replace_existing: bool = False
    ) -> None:
        captured.append((path, schema, rows, replace_existing))

    class Result:
        def fetchall(self) -> list[tuple[object, ...]]:
            return []

    class Connection:
        def execute(self, query: str, parameters: list[object]) -> Result:
            del query, parameters
            return Result()

    monkeypatch.setattr(aggregate_module, "_write_rows", fake_write_rows)
    failure_output = Path("failure-summary.parquet")
    aggregate_module._write_failure_summary(
        cast(duckdb.DuckDBPyConnection, Connection()),
        Path("failures"),
        failure_output,
        replace_existing=True,
    )
    conflict_output = Path("conflicts.parquet")
    aggregate_module._write_conflicts(
        cast(duckdb.DuckDBPyConnection, Connection()),
        Path("occurrences"),
        conflict_output,
        replace_existing=True,
    )

    assert captured == [
        (failure_output, FAILURE_SUMMARY_SCHEMA, [], True),
        (conflict_output, CONFLICT_SCHEMA, [], True),
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
    count_calls: list[tuple[str, list[object]]] = []
    partition_calls: list[bool] = []

    def fake_copy(
        connection: duckdb.DuckDBPyConnection,
        query: str,
        parameters: list[object],
        output_path: Path,
        *,
        replace_existing: bool = False,
    ) -> None:
        del connection, replace_existing
        copy_calls.append((query, parameters, output_path))

    def fake_fetch_count(
        connection: duckdb.DuckDBPyConnection, query: str, parameters: list[object]
    ) -> int:
        count_calls.append((query, parameters))
        return 9

    monkeypatch.setattr(aggregate_module, "_copy_query", fake_copy)
    monkeypatch.setattr(aggregate_module, "_fetch_count", fake_fetch_count)

    def fake_partition(
        connection: object,
        occurrence_value: Path,
        output_root: Path,
        *,
        replace_existing: bool = False,
    ) -> Path:
        del connection, occurrence_value, output_root
        partition_calls.append(replace_existing)
        return Path("run/scratch/occurrence-buckets")

    monkeypatch.setattr(
        aggregate_module,
        "_partition_occurrences",
        fake_partition,
    )
    occurrence_root = Path("occurrences")
    members = (Path("website.parquet"), Path("wikipedia.parquet"), Path("wikivoyage.parquet"))

    result = _materialize_global(
        cast(duckdb.DuckDBPyConnection, object()), occurrence_root, members, Path("run")
    )

    compact = Path("run/scratch/global_compact.parquet")
    assert result == (
        tuple(Path(f"run/coverage/global/shard-{index:02d}.parquet") for index in range(64)),
        compact,
        9,
    )
    assert len(copy_calls) == 129
    assert [output for _, _, output in copy_calls[1::2]] == list(result[0])
    assert copy_calls[-1] == (
        aggregate_module._COMPACT_FROM_GLOBAL_SQL,
        ["run/coverage/global/*.parquet"],
        compact,
    )
    assert count_calls == [("SELECT COUNT(*) FROM read_parquet(?)", [str(compact)])]
    assert partition_calls == [False]


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
    write_calls: list[tuple[Path, object, object, bool]] = []
    group_calls: list[tuple[Path, str]] = []
    metric_calls: list[tuple[Path, str, str]] = []
    failure_calls: list[tuple[Path, Path, bool]] = []
    conflict_calls: list[tuple[Path, Path, bool]] = []

    def fake_write_rows(
        path: Path, schema: object, rows: object, *, replace_existing: bool = False
    ) -> None:
        write_calls.append((path, schema, rows, replace_existing))

    def fake_metric_rows(summary: dict[str, object]) -> list[str]:
        return ["metrics"]

    def fake_overlap_rows(summary: dict[str, object]) -> list[str]:
        return ["overlap"]

    def fake_group_rows(
        connection: duckdb.DuckDBPyConnection, table: Path, column: str
    ) -> list[str]:
        group_calls.append((table, column))
        return [column]

    def fake_group_metric_rows(
        connection: duckdb.DuckDBPyConnection, table: Path, column: str, scope: str
    ) -> list[str]:
        metric_calls.append((table, column, scope))
        return [scope]

    def fake_failure_summary(
        connection: duckdb.DuckDBPyConnection,
        failure_root: Path,
        output: Path,
        *,
        replace_existing: bool = False,
    ) -> None:
        failure_calls.append((failure_root, output, replace_existing))

    def fake_conflicts(
        connection: duckdb.DuckDBPyConnection,
        occurrence_root: Path,
        output: Path,
        *,
        replace_existing: bool = False,
    ) -> None:
        conflict_calls.append((occurrence_root, output, replace_existing))

    monkeypatch.setattr(aggregate_module, "_write_rows", fake_write_rows)
    monkeypatch.setattr(aggregate_module, "_metric_rows", fake_metric_rows)
    monkeypatch.setattr(aggregate_module, "_overlap_rows", fake_overlap_rows)
    monkeypatch.setattr(aggregate_module, "_group_rows", fake_group_rows)
    monkeypatch.setattr(aggregate_module, "_group_metric_rows", fake_group_metric_rows)
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
        replace_existing=True,
    )

    assert result == (
        Path("run/summaries/global.parquet"),
        Path("run/summaries/by-source-pbf.parquet"),
        Path("run/summaries/by-region.parquet"),
        Path("run/summaries/by-source-pbf-metrics.parquet"),
        Path("run/summaries/by-region-metrics.parquet"),
        Path("run/summaries/by-overlap.parquet"),
        Path("run/summaries/geometry-failures.parquet"),
        Path("run/summaries/conflicts.parquet"),
    )
    assert write_calls == [
        (Path("run/summaries/global.parquet"), GLOBAL_SUMMARY_SCHEMA, ["metrics"], True),
        (Path("run/summaries/by-overlap.parquet"), OVERLAP_SUMMARY_SCHEMA, ["overlap"], True),
        (
            Path("run/summaries/by-source-pbf.parquet"),
            GROUP_SUMMARY_SCHEMA,
            ["source_pbf"],
            True,
        ),
        (Path("run/summaries/by-region.parquet"), GROUP_SUMMARY_SCHEMA, ["region"], True),
        (
            Path("run/summaries/by-source-pbf-metrics.parquet"),
            GROUP_METRIC_SCHEMA,
            ["source_pbf"],
            True,
        ),
        (
            Path("run/summaries/by-region-metrics.parquet"),
            GROUP_METRIC_SCHEMA,
            ["region"],
            True,
        ),
    ]
    assert group_calls == [
        (Path("by-pbf.parquet"), "source_pbf"),
        (Path("compact.parquet"), "region"),
    ]
    assert metric_calls == [
        (Path("by-pbf.parquet"), "source_pbf", "source_pbf"),
        (Path("compact.parquet"), "region", "region"),
    ]
    assert failure_calls == [
        (
            Path("run/geometry-failures"),
            Path("run/summaries/geometry-failures.parquet"),
            True,
        )
    ]
    assert conflict_calls == [(occurrence_root, Path("run/summaries/conflicts.parquet"), True)]

    aggregate_module._write_summary_outputs(
        cast(duckdb.DuckDBPyConnection, object()),
        Path("compact.parquet"),
        Path("by-pbf.parquet"),
        output_root,
        {},
        occurrence_root,
    )

    assert write_calls == [
        *write_calls[:6],
        (Path("run/summaries/global.parquet"), GLOBAL_SUMMARY_SCHEMA, ["metrics"], False),
        (Path("run/summaries/by-overlap.parquet"), OVERLAP_SUMMARY_SCHEMA, ["overlap"], False),
        (
            Path("run/summaries/by-source-pbf.parquet"),
            GROUP_SUMMARY_SCHEMA,
            ["source_pbf"],
            False,
        ),
        (Path("run/summaries/by-region.parquet"), GROUP_SUMMARY_SCHEMA, ["region"], False),
        (
            Path("run/summaries/by-source-pbf-metrics.parquet"),
            GROUP_METRIC_SCHEMA,
            ["source_pbf"],
            False,
        ),
        (
            Path("run/summaries/by-region-metrics.parquet"),
            GROUP_METRIC_SCHEMA,
            ["region"],
            False,
        ),
    ]
    assert failure_calls == [
        (
            Path("run/geometry-failures"),
            Path("run/summaries/geometry-failures.parquet"),
            True,
        ),
        (
            Path("run/geometry-failures"),
            Path("run/summaries/geometry-failures.parquet"),
            False,
        ),
    ]
    assert conflict_calls == [
        (occurrence_root, Path("run/summaries/conflicts.parquet"), True),
        (occurrence_root, Path("run/summaries/conflicts.parquet"), False),
    ]


def test_aggregate_summary_writer_defaults_to_no_overwrite() -> None:
    assert (
        inspect.signature(aggregate_module._write_summary_outputs)
        .parameters["replace_existing"]
        .default
        is False
    )


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

        def execute(self, query: str) -> None:
            calls.append(("execute", query))

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
        ("execute", f"SET temp_directory = '{output_root / 'scratch' / 'duckdb-temp'}'"),
        ("execute", "SET preserve_insertion_order = false"),
        ("execute", "SET threads = 4"),
        ("execute", "SET max_temp_directory_size = '100GB'"),
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


def test_aggregate_run_resume_forwards_the_rebuilt_derived_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    occurrence_root = tmp_path / "occurrences"
    membership_root = tmp_path / "members"
    occurrence_root.mkdir()
    membership_root.mkdir()
    (occurrence_root / "one.parquet").write_bytes(b"occurrence")
    for name in ("website", "wikipedia", "wikivoyage"):
        (membership_root / f"{name}.parquet").write_bytes(b"membership")
    output_root = tmp_path / "run"
    calls: list[tuple[str, object]] = []

    class Connection:
        def close(self) -> None:
            calls.append(("close", None))

        def execute(self, query: str) -> None:
            calls.append(("execute", query))

    connection = Connection()

    def fake_connect(*, database: str) -> Connection:
        calls.append(("connect", database))
        return connection

    def fake_global(
        connection_value: duckdb.DuckDBPyConnection,
        occurrence_value: Path,
        members: tuple[Path, Path, Path],
        target: Path,
        *,
        replace_existing: bool = False,
    ) -> tuple[tuple[Path, ...], Path, int]:
        calls.append(("global", (occurrence_value, members, target, replace_existing)))
        return (target / "global.parquet",), target / "compact.parquet", 3

    def fake_by_pbf(
        connection_value: duckdb.DuckDBPyConnection,
        occurrence_value: Path,
        members: tuple[Path, Path, Path],
        target: Path,
        *,
        replace_existing: bool = False,
    ) -> Path:
        calls.append(("by-pbf", (occurrence_value, members, target, replace_existing)))
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
        *,
        replace_existing: bool = False,
    ) -> tuple[Path, ...]:
        calls.append(
            (
                "summaries",
                (compact, by_pbf, target, summary, occurrence_value, replace_existing),
            )
        )
        return (target / "summary.parquet",)

    monkeypatch.setattr(aggregate_module.duckdb, "connect", fake_connect)
    monkeypatch.setattr(aggregate_module, "_materialize_global", fake_global)
    monkeypatch.setattr(aggregate_module, "_materialize_by_pbf", fake_by_pbf)
    monkeypatch.setattr(aggregate_module, "_fetch_summary", fake_fetch)
    monkeypatch.setattr(aggregate_module, "_write_summary_outputs", fake_summaries)

    result = aggregate_run(
        occurrence_root=occurrence_root,
        membership_root=membership_root,
        output_root=output_root,
        resume=True,
    )
    members = tuple(
        membership_root / f"{name}.parquet" for name in ("website", "wikipedia", "wikivoyage")
    )

    assert calls == [
        ("connect", ":memory:"),
        ("execute", f"SET temp_directory = '{output_root / 'scratch' / 'duckdb-temp'}'"),
        ("execute", "SET preserve_insertion_order = false"),
        ("execute", "SET threads = 4"),
        ("execute", "SET max_temp_directory_size = '100GB'"),
        ("global", (occurrence_root, members, output_root, True)),
        ("by-pbf", (occurrence_root, members, output_root, True)),
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
                True,
            ),
        ),
        ("close", None),
    ]
    assert result.by_pbf_path == output_root / "by-pbf.parquet"


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


def test_configure_connection_uses_run_local_bounded_spill_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    mkdir_calls: list[tuple[Path, bool, bool]] = []

    class Connection:
        def execute(self, query: str) -> None:
            calls.append(query)

    original_mkdir = Path.mkdir

    def tracked_mkdir(
        path: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        if path == tmp_path / "run" / "scratch" / "duckdb-temp":
            mkdir_calls.append((path, parents, exist_ok))
        original_mkdir(path, mode=mode, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", tracked_mkdir)
    target = tmp_path / "run"

    aggregate_module._configure_connection(cast(duckdb.DuckDBPyConnection, Connection()), target)

    assert (target / "scratch" / "duckdb-temp").is_dir()
    assert mkdir_calls[0] == (target / "scratch" / "duckdb-temp", True, True)
    assert calls == [
        f"SET temp_directory = '{target / 'scratch' / 'duckdb-temp'}'",
        "SET preserve_insertion_order = false",
        "SET threads = 4",
        "SET max_temp_directory_size = '100GB'",
    ]


def test_partition_occurrences_writes_reusable_hive_buckets(tmp_path: Path) -> None:
    occurrence_root = tmp_path / "occurrences"
    _write_occurrences(occurrence_root)
    connection = duckdb.connect(database=":memory:")
    try:
        bucket_root = _partition_occurrences(connection, occurrence_root, tmp_path / "run")
    finally:
        connection.close()

    bucket_files = tuple(bucket_root.glob("shard_id=*/*.parquet"))
    assert len(bucket_files) >= 64
    assert sum(pq.ParquetFile(path).metadata.num_rows for path in bucket_files) == 10
    assert all(pq.read_schema(path) == OCCURRENCE_SCHEMA for path in bucket_files)


def test_partition_occurrences_refuses_existing_buckets(tmp_path: Path) -> None:
    target = tmp_path / "run"
    (target / "scratch" / "occurrence-buckets").mkdir(parents=True)

    with pytest.raises(FileExistsError, match="aggregate buckets"):
        _partition_occurrences(
            cast(duckdb.DuckDBPyConnection, object()),
            tmp_path / "occurrences",
            target,
        )


def test_partition_occurrences_removes_partial_buckets_after_failure(tmp_path: Path) -> None:
    target = tmp_path / "run"
    temporary = target / "scratch" / ".occurrence-buckets.tmp"

    class Connection:
        def execute(self, query: str, parameters: list[object]) -> None:
            del query, parameters
            temporary.mkdir(parents=True)
            (temporary / "partial.parquet").write_bytes(b"partial")
            raise RuntimeError("partition failed")

    with pytest.raises(RuntimeError, match="partition failed"):
        _partition_occurrences(
            cast(duckdb.DuckDBPyConnection, Connection()),
            tmp_path / "occurrences",
            target,
        )

    assert not temporary.exists()


def test_aggregate_bucket_paths_and_replacement_cleanup_are_exact(tmp_path: Path) -> None:
    fresh_target = tmp_path / "fresh-run"
    fresh_bucket_root, fresh_temporary = aggregate_module._prepare_bucket_directories(
        fresh_target, replace_existing=False
    )
    assert fresh_bucket_root == fresh_target / "scratch" / "occurrence-buckets"
    assert fresh_temporary == fresh_target / "scratch" / ".occurrence-buckets.tmp"
    assert (fresh_target / "scratch").is_dir()

    target = tmp_path / "run"
    bucket_root = target / "scratch" / "occurrence-buckets"
    temporary = target / "scratch" / ".occurrence-buckets.tmp"
    (bucket_root / "old").mkdir(parents=True)
    (temporary / "old").mkdir(parents=True)

    assert aggregate_module._aggregation_bucket_directory(target) == bucket_root
    assert aggregate_module._aggregation_bucket_temporary_directory(target) == temporary
    assert aggregate_module._prepare_bucket_directories(target, replace_existing=True) == (
        bucket_root,
        temporary,
    )
    assert (target / "scratch").is_dir()
    assert not bucket_root.exists()
    assert not temporary.exists()


def test_write_partitioned_occurrences_uses_the_exact_copy_contract(tmp_path: Path) -> None:
    calls: list[tuple[str, list[object]]] = []

    class Connection:
        def execute(self, query: str, parameters: list[object]) -> None:
            calls.append((query, parameters))

    occurrence_root = tmp_path / "occurrences"
    temporary = tmp_path / "scratch" / "buckets.tmp"
    aggregate_module._write_partitioned_occurrences(
        cast(duckdb.DuckDBPyConnection, Connection()), occurrence_root, temporary
    )

    assert calls == [
        (
            f"COPY (SELECT *, {aggregate_module._SHARD_EXPRESSION} AS shard_id "
            "FROM read_parquet(?, union_by_name = true)) "
            f"TO '{temporary}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (shard_id))",
            [str(occurrence_root / "*.parquet")],
        )
    ]


def test_ensure_empty_buckets_creates_parents_and_preserves_parquet_buckets(
    tmp_path: Path,
) -> None:
    temporary = tmp_path / "run" / "scratch" / ".occurrence-buckets.tmp"
    existing = temporary / "shard_id=0"
    existing.mkdir(parents=True)
    (existing / "existing.parquet").write_bytes(b"placeholder")

    aggregate_module._ensure_empty_buckets(temporary)

    assert all((temporary / f"shard_id={shard_id}").is_dir() for shard_id in range(64))
    assert not (existing / "empty.parquet").exists()
    assert all(
        (temporary / f"shard_id={shard_id}" / "empty.parquet").is_file()
        for shard_id in range(1, 64)
    )


def test_ensure_empty_buckets_creates_a_missing_temporary_parent(tmp_path: Path) -> None:
    temporary = tmp_path / "run" / "scratch" / ".occurrence-buckets.tmp"

    aggregate_module._ensure_empty_buckets(temporary)

    assert temporary.is_dir()
    assert (temporary / "shard_id=0" / "empty.parquet").is_file()


def test_partition_occurrences_forwards_replacement_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "run"
    bucket_root = target / "scratch" / "occurrence-buckets"
    temporary = target / "scratch" / ".occurrence-buckets.tmp"
    temporary.mkdir(parents=True)
    calls: list[bool] = []

    def prepare(output_root: Path, *, replace_existing: bool) -> tuple[Path, Path]:
        assert output_root == target
        calls.append(replace_existing)
        return bucket_root, temporary

    monkeypatch.setattr(aggregate_module, "_prepare_bucket_directories", prepare)
    monkeypatch.setattr(aggregate_module, "_write_partitioned_occurrences", lambda *args: None)
    monkeypatch.setattr(aggregate_module, "_ensure_empty_buckets", lambda path: None)

    assert (
        _partition_occurrences(
            cast(duckdb.DuckDBPyConnection, object()),
            tmp_path / "occurrences",
            target,
            replace_existing=True,
        )
        == bucket_root
    )
    assert calls == [True]


def test_materialize_global_defaults_to_nonreplacement_partitioning() -> None:
    assert inspect.signature(_materialize_global).parameters["replace_existing"].default is False


def test_aggregate_temporary_artifact_paths_are_exact(tmp_path: Path) -> None:
    target = tmp_path / "run"

    assert aggregate_module._duckdb_spill_directory(target) == (target / "scratch" / "duckdb-temp")
    assert aggregate_module._aggregation_global_part(target) == (
        target / "scratch" / "global_shard.parquet"
    )


def test_cleanup_duckdb_spill_removes_only_the_run_spill_directory(tmp_path: Path) -> None:
    target = tmp_path / "run"
    spill = target / "scratch" / "duckdb-temp"
    spill.mkdir(parents=True)
    (spill / "spill.tmp").write_bytes(b"spill")

    aggregate_module._cleanup_duckdb_spill(target)
    aggregate_module._cleanup_duckdb_spill(target)

    assert not spill.exists()


def test_cleanup_aggregation_temporary_removes_all_stale_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "run"
    spill = target / "scratch" / "duckdb-temp"
    buckets = target / "scratch" / "occurrence-buckets"
    temporary_buckets = target / "scratch" / ".occurrence-buckets.tmp"
    global_part = target / "scratch" / "global_shard.parquet"
    global_part_temporary = target / "scratch" / ".global_shard.parquet.tmp"
    compact_temporary = target / "scratch" / ".global_compact.parquet.tmp"
    for directory in (spill, buckets, temporary_buckets):
        directory.mkdir(parents=True)
    for file in (global_part, global_part_temporary, compact_temporary):
        file.write_bytes(b"stale")

    original_unlink = Path.unlink
    unlink_calls: list[tuple[Path, bool]] = []

    def tracked_unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path.parent == target / "scratch":
            unlink_calls.append((path, missing_ok))
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", tracked_unlink)

    aggregate_module._cleanup_aggregation_temporary(target)
    aggregate_module._cleanup_aggregation_temporary(target)

    assert all(
        not path.exists()
        for path in (
            spill,
            buckets,
            temporary_buckets,
            global_part,
            global_part_temporary,
            compact_temporary,
        )
    )
    assert unlink_calls == [
        (global_part, True),
        (global_part_temporary, True),
        (compact_temporary, True),
        (global_part, True),
        (global_part_temporary, True),
        (compact_temporary, True),
    ]
