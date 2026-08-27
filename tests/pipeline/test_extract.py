import inspect
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.pipeline.extract as extract_module
from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import (
    GeometryFailure,
    Occurrence,
    OsmIdentity,
)
from osm_polygon_wikidata_website_coverage.pipeline.extract import (
    ExtractionError,
    InputChangedError,
    SourceSnapshot,
    _assert_unchanged,
    _emit_to_writers,
    _pbf_files,
    extract_all,
)


def _paths(tmp_path: Path, raw: Path) -> DataPaths:
    source = tmp_path / "source"
    source.mkdir(exist_ok=True)
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


def test_extract_all_counts_every_failure_and_records_inventory(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")

    def scanner(path: Path, callback) -> None:
        callback(
            GeometryFailure(
                identity=None,
                source_pbf=path.name,
                candidate_kind="closed_way",
                failure_kind="invalid_identity",
                message="first",
            )
        )
        callback(
            GeometryFailure(
                identity=OsmIdentity("relation", 3),
                source_pbf=path.name,
                candidate_kind="boundary_relation",
                failure_kind="invalid_geometry",
                message="second",
            )
        )

    result = extract_all(_paths(tmp_path, raw), "inventory", scanner=scanner)

    assert result.failure_count == 2
    assert len(result.source_inventory) == 1
    inventory = result.source_inventory[0]
    assert inventory.before.path == pbf
    assert inventory.after.path == pbf
    assert inventory.before.size_bytes == inventory.after.size_bytes == len(b"fixture")


def test_extract_all_preserves_exact_output_directories_and_batch_size(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")

    def scanner(path: Path, callback) -> None:
        callback(_occurrence(path.name, 1))
        callback(_occurrence(path.name, 2))
        callback(
            GeometryFailure(
                identity=None,
                source_pbf=path.name,
                candidate_kind="closed_way",
                failure_kind="invalid_geometry",
                message="failure one",
            )
        )
        callback(
            GeometryFailure(
                identity=None,
                source_pbf=path.name,
                candidate_kind="closed_way",
                failure_kind="invalid_geometry",
                message="failure two",
            )
        )

    result = extract_all(_paths(tmp_path, raw), "exact", scanner=scanner, batch_rows=1)

    assert {path.name for path in result.run_root.iterdir()} == {
        "occurrences",
        "geometry-failures",
    }
    assert sorted(path.name for path in (result.run_root / "occurrences").glob("*.parquet")) == [
        "fixture-latest-00000.parquet",
        "fixture-latest-00001.parquet",
    ]
    assert sorted(
        path.name for path in (result.run_root / "geometry-failures").glob("*.parquet")
    ) == ["fixture-latest-00000.parquet", "fixture-latest-00001.parquet"]


def test_extract_all_default_batch_size_is_five_thousand() -> None:
    assert inspect.signature(extract_all).parameters["batch_rows"].default == 5_000


def test_extract_all_forwards_default_writer_paths_and_batch_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    calls: list[tuple[str, Path, str, int]] = []

    class Writer:
        def __init__(self, root: Path, *, source_stem: str, batch_rows: int) -> None:
            calls.append(("init", root, source_stem, batch_rows))

        def __enter__(self) -> "Writer":
            return self

        def __exit__(self, *args: object) -> None:
            del args

        def write(self, result: object) -> None:
            calls.append(("write", Path(str(result)), "", 0))

    monkeypatch.setattr(extract_module, "OccurrenceShardWriter", Writer)
    monkeypatch.setattr(extract_module, "FailureShardWriter", Writer)

    def scanner(path: Path, callback) -> None:
        callback(_occurrence(path.name, 1))

    result = extract_all(_paths(tmp_path, raw), "default-writer", scanner=scanner)

    assert result.occurrence_count == 1
    assert calls[:2] == [
        ("init", result.run_root / "occurrences", "fixture-latest", 5_000),
        ("init", result.run_root / "geometry-failures", "fixture-latest", 5_000),
    ]


def test_extract_helpers_report_exact_source_and_change_contracts(tmp_path: Path) -> None:
    first = tmp_path / "b-latest.osm.pbf"
    second = tmp_path / "a-latest.osm.pbf"
    first.write_bytes(b"b")
    second.write_bytes(b"a")
    assert _pbf_files(tmp_path) == (second, first)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "osm_polygon_wikidata_website_coverage.pipeline.extract.os.access",
            lambda *args: False,
        )
        with pytest.raises(ExtractionError) as raised:
            _pbf_files(tmp_path)
    assert str(raised.value) == "raw PBF files are unreadable: a-latest.osm.pbf, b-latest.osm.pbf"

    unchanged = SourceSnapshot(Path("fixture"), 1, 2)
    _assert_unchanged(unchanged, SourceSnapshot(Path("fixture"), 1, 2))
    with pytest.raises(InputChangedError, match="^source PBF changed during scan: fixture$"):
        _assert_unchanged(unchanged, SourceSnapshot(Path("fixture"), 2, 2))
    with pytest.raises(InputChangedError, match="^source PBF changed during scan: fixture$"):
        _assert_unchanged(unchanged, SourceSnapshot(Path("fixture"), 1, 3))


def test_emit_to_writers_returns_one_only_for_occurrences() -> None:
    class Writer:
        def __init__(self) -> None:
            self.values: list[object] = []

        def write(self, value: object) -> None:
            self.values.append(value)

    occurrence_writer = Writer()
    failure_writer = Writer()
    occurrence = _occurrence("fixture-latest.osm.pbf", 1)
    failure = GeometryFailure(None, "fixture-latest.osm.pbf", "closed_way", "invalid", "bad")

    occurrence_writer_typed = cast(extract_module.OccurrenceShardWriter, occurrence_writer)
    failure_writer_typed = cast(extract_module.FailureShardWriter, failure_writer)
    assert _emit_to_writers(occurrence, occurrence_writer_typed, failure_writer_typed) == 1
    assert _emit_to_writers(failure, occurrence_writer_typed, failure_writer_typed) == 0
    assert occurrence_writer.values == [occurrence]
    assert failure_writer.values == [failure]


def test_extract_all_rejects_missing_or_empty_raw_root(tmp_path: Path) -> None:
    raw = tmp_path / "missing"

    with pytest.raises(ExtractionError, match="raw PBF root"):
        extract_all(_paths(tmp_path, raw), "missing")

    raw.mkdir()
    with pytest.raises(ExtractionError, match="no regular PBF"):
        extract_all(_paths(tmp_path, raw), "empty")


def test_extract_all_rejects_unreadable_sources_and_existing_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    paths = _paths(tmp_path, raw)

    monkeypatch.setattr(
        "osm_polygon_wikidata_website_coverage.pipeline.extract.os.access", lambda *args: False
    )
    with pytest.raises(ExtractionError, match="unreadable"):
        extract_all(paths, "unreadable")

    monkeypatch.undo()
    paths.run_root("existing").mkdir(parents=True)
    with pytest.raises(ExtractionError, match="already exists"):
        extract_all(paths, "existing")


def test_source_snapshot_reports_stat_errors(tmp_path: Path) -> None:
    with pytest.raises(ExtractionError, match="cannot stat"):
        SourceSnapshot.read(tmp_path / "missing.osm.pbf")
