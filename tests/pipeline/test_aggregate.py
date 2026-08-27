import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.io.parquet import FAILURE_SCHEMA, OCCURRENCE_SCHEMA
from osm_polygon_wikidata_website_coverage.pipeline.aggregate import (
    EXPECTED_GLOBAL_COLUMNS,
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

    assert len(summary["overlap_categories"]) == 8
    assert sum(item["count"] for item in summary["overlap_categories"]) == 8


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
