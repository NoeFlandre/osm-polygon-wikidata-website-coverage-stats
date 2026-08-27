from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.io.parquet as parquet_module
from osm_polygon_wikidata_website_coverage.domain.identity import (
    GeometryFailure,
    Occurrence,
    OsmIdentity,
)
from osm_polygon_wikidata_website_coverage.io.parquet import (
    FAILURE_SCHEMA,
    OCCURRENCE_SCHEMA,
    FailureShardWriter,
    OccurrenceShardWriter,
    _failure_row,
    _occurrence_row,
    _timestamp_text,
)


def _occurrence(osm_id: int) -> Occurrence:
    return Occurrence(
        identity=OsmIdentity("way", osm_id),
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        osm_version=1,
        osm_timestamp="2026-01-01T00:00:00Z",
        geometry_type="Polygon",
        geometry='{"coordinates":[],"type":"Polygon"}',
        centroid_lon=0.5,
        centroid_lat=0.5,
        bbox_min_lon=0.0,
        bbox_min_lat=0.0,
        bbox_max_lon=1.0,
        bbox_max_lat=1.0,
        area_m2=1.0,
        area_bucket="under_1e3_m2",
        geometry_hash="a" * 64,
    )


def test_occurrence_writer_flushes_bounded_atomic_shards(tmp_path: Path) -> None:
    writer = OccurrenceShardWriter(tmp_path, source_stem="fixture", batch_rows=2)
    writer.write(_occurrence(1))
    writer.write(_occurrence(2))
    writer.write(_occurrence(3))
    writer.write(_occurrence(4))
    writer.write(_occurrence(5))
    writer.close()

    shards = sorted(tmp_path.glob("fixture-*.parquet"))
    assert [path.name for path in shards] == [
        "fixture-00000.parquet",
        "fixture-00001.parquet",
        "fixture-00002.parquet",
    ]
    assert all(not path.with_suffix(".parquet.tmp").exists() for path in shards)
    assert pq.read_table(shards[0]).num_rows == 2
    assert pq.read_table(shards[1]).num_rows == 2
    assert pq.read_table(shards[2]).num_rows == 1
    assert pq.read_schema(shards[0]) == OCCURRENCE_SCHEMA


def test_failure_writer_preserves_nullable_identity_and_diagnostic(tmp_path: Path) -> None:
    writer = FailureShardWriter(tmp_path, source_stem="fixture", batch_rows=10)
    writer.write(
        GeometryFailure(
            identity=OsmIdentity("relation", 9),
            source_pbf="fixture-latest.osm.pbf",
            candidate_kind="boundary_relation",
            failure_kind="invalid_geometry",
            message="bad shape",
        )
    )
    writer.close()

    table = pq.read_table(tmp_path / "fixture-00000.parquet")
    assert table.to_pylist()[0]["osm_id"] == 9
    assert table.to_pylist()[0]["message"] == "bad shape"
    assert pq.read_schema(tmp_path / "fixture-00000.parquet") == FAILURE_SCHEMA


def test_occurrence_writer_rejects_invalid_batch_size(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="^batch_rows must be positive$"):
        OccurrenceShardWriter(tmp_path, source_stem="fixture", batch_rows=0)


def test_parquet_helpers_handle_nullable_and_datetime_timestamps() -> None:
    assert _timestamp_text(None) is None
    assert _timestamp_text(datetime(2026, 1, 1, tzinfo=UTC)) == "2026-01-01T00:00:00Z"
    assert _timestamp_text("2026-01-01T00:00:00Z") == "2026-01-01T00:00:00Z"


def test_writer_rejects_invalid_stems_writes_after_close_and_closes_twice(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="source_stem"):
        OccurrenceShardWriter(tmp_path, source_stem="../fixture")

    writer = OccurrenceShardWriter(tmp_path, source_stem="fixture")
    assert writer._batch_rows == 5_000
    assert writer._closed is False
    writer.close()
    writer.close()
    with pytest.raises(RuntimeError, match="^cannot write to a closed Parquet writer$"):
        writer.write(_occurrence(1))

    with pytest.raises(ValueError, match="^source_stem must be a non-empty filename stem$"):
        OccurrenceShardWriter(tmp_path, source_stem="../fixture")


def test_parquet_row_builders_preserve_the_complete_schema_contract() -> None:
    occurrence = _occurrence(7)
    assert _occurrence_row(occurrence) == {
        "osm_type": "way",
        "osm_id": 7,
        "source_pbf": "fixture-latest.osm.pbf",
        "region": "fixture",
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
        "area_m2": 1.0,
        "area_bucket": "under_1e3_m2",
        "geometry_hash": "a" * 64,
    }

    failure = GeometryFailure(
        identity=OsmIdentity("relation", 8),
        source_pbf="fixture-latest.osm.pbf",
        candidate_kind="boundary_relation",
        failure_kind="invalid_geometry",
        message="bad shape",
    )
    assert _failure_row(failure) == {
        "osm_type": "relation",
        "osm_id": 8,
        "source_pbf": "fixture-latest.osm.pbf",
        "candidate_kind": "boundary_relation",
        "failure_kind": "invalid_geometry",
        "message": "bad shape",
    }


def test_writer_creates_missing_nested_directories_and_defaults_are_shared(tmp_path: Path) -> None:
    nested = tmp_path / "one" / "two"
    occurrence = OccurrenceShardWriter(nested, source_stem="occurrence")
    failure = FailureShardWriter(nested, source_stem="failure")
    assert occurrence._batch_rows == 5_000
    assert failure._batch_rows == 5_000
    occurrence.close()
    failure.close()


def test_writer_serializes_datetime_and_nullable_failure_identity(tmp_path: Path) -> None:
    writer = OccurrenceShardWriter(tmp_path, source_stem="datetime", batch_rows=1)
    value = replace(_occurrence(1), osm_timestamp=datetime(2026, 1, 1, tzinfo=UTC))
    writer.write(value)
    writer.close()
    assert pq.read_table(tmp_path / "datetime-00000.parquet").to_pylist()[0]["osm_timestamp"] == (
        "2026-01-01T00:00:00Z"
    )

    failure_writer = FailureShardWriter(tmp_path, source_stem="nullable", batch_rows=1)
    failure_writer.write(
        GeometryFailure(
            identity=None,
            source_pbf="fixture-latest.osm.pbf",
            candidate_kind="closed_way",
            failure_kind="invalid_identity",
            message="bad id",
        )
    )
    failure_writer.close()
    assert pq.read_table(tmp_path / "nullable-00000.parquet").to_pylist()[0]["osm_id"] is None


@pytest.mark.parametrize("preexisting", ["final", "temporary"])
def test_writer_refuses_to_overwrite_existing_shard(tmp_path: Path, preexisting: str) -> None:
    existing = tmp_path / "fixture-00000.parquet"
    if preexisting == "final":
        existing.write_bytes(b"existing")
    else:
        (tmp_path / ".fixture-00000.parquet.tmp").write_bytes(b"existing")

    writer = OccurrenceShardWriter(tmp_path, source_stem="fixture", batch_rows=1)
    with pytest.raises(FileExistsError, match="overwrite"):
        writer.write(_occurrence(1))


def test_shard_writer_uses_zstd_for_each_atomic_flush(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[Path, object, dict[str, object]]] = []

    class Writer:
        def __init__(self, path: Path, schema: object, **kwargs: object) -> None:
            calls.append((path, schema, kwargs))
            path.touch()

        def write_table(self, table: object) -> None:
            pass

        def close(self) -> None:
            pass

    monkeypatch.setattr(parquet_module.pq, "ParquetWriter", Writer)
    writer = OccurrenceShardWriter(tmp_path, source_stem="fixture", batch_rows=1)
    writer.write(_occurrence(1))
    writer.close()

    assert calls == [
        (
            tmp_path / ".fixture-00000.parquet.tmp",
            OCCURRENCE_SCHEMA,
            {"compression": "zstd"},
        )
    ]
