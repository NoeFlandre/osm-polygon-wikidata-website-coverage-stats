from pathlib import Path

import pyarrow.parquet as pq
import pytest

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
    writer.close()

    shards = sorted(tmp_path.glob("fixture-*.parquet"))
    assert [path.name for path in shards] == ["fixture-00000.parquet", "fixture-00001.parquet"]
    assert all(not path.with_suffix(".parquet.tmp").exists() for path in shards)
    assert pq.read_table(shards[0]).num_rows == 2
    assert pq.read_table(shards[1]).num_rows == 1
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
    with pytest.raises(ValueError, match="batch_rows"):
        OccurrenceShardWriter(tmp_path, source_stem="fixture", batch_rows=0)
