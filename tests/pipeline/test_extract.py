from pathlib import Path

import pyarrow.parquet as pq
import pytest

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import (
    GeometryFailure,
    Occurrence,
    OsmIdentity,
)
from osm_polygon_wikidata_website_coverage.pipeline.extract import (
    ExtractionError,
    InputChangedError,
    extract_all,
)


def _paths(tmp_path: Path, raw: Path) -> DataPaths:
    source = tmp_path / "source"
    source.mkdir()
    return DataPaths(tmp_path / "data", raw, source, source)


def _occurrence(source_pbf: str, osm_id: int) -> Occurrence:
    return Occurrence(
        identity=OsmIdentity("way", osm_id),
        source_pbf=source_pbf,
        region=source_pbf.removesuffix("-latest.osm.pbf"),
        osm_version=1,
        osm_timestamp="2026-01-01T00:00:00Z",
    )


def test_extract_all_sorts_pbf_names_writes_shards_and_preserves_sources(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    first = raw / "b-latest.osm.pbf"
    second = raw / "a-latest.osm.pbf"
    first.write_bytes(b"b")
    second.write_bytes(b"a")
    before = {path.name: path.read_bytes() for path in raw.glob("*.osm.pbf")}
    seen: list[str] = []

    def scanner(path: Path, callback) -> None:
        seen.append(path.name)
        callback(_occurrence(path.name, len(seen)))
        if path.name.startswith("b"):
            callback(
                GeometryFailure(
                    identity=OsmIdentity("relation", 100),
                    source_pbf=path.name,
                    candidate_kind="boundary_relation",
                    failure_kind="invalid_geometry",
                    message="fixture failure",
                )
            )

    result = extract_all(_paths(tmp_path, raw), "fixture", scanner=scanner, batch_rows=1)

    assert seen == ["a-latest.osm.pbf", "b-latest.osm.pbf"]
    assert result.occurrence_count == 2
    assert result.failure_count == 1
    assert (result.run_root / "occurrences" / "a-latest-00000.parquet").is_file()
    failure_path = result.run_root / "geometry-failures" / "b-latest-00000.parquet"
    assert pq.read_table(failure_path).to_pylist()[0]["failure_kind"] == "invalid_geometry"
    assert {path.name: path.read_bytes() for path in raw.glob("*.osm.pbf")} == before


def test_extract_all_fails_if_a_pbf_changes_during_scan(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"before")

    def scanner(path: Path, callback) -> None:
        callback(_occurrence(path.name, 1))
        path.write_bytes(b"changed")

    with pytest.raises(InputChangedError, match="changed during scan"):
        extract_all(_paths(tmp_path, raw), "changed", scanner=scanner)


def test_extract_all_rejects_missing_or_empty_raw_root(tmp_path: Path) -> None:
    raw = tmp_path / "missing"

    with pytest.raises(ExtractionError, match="raw PBF root"):
        extract_all(_paths(tmp_path, raw), "missing")
