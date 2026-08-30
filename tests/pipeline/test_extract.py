import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.pipeline.extract as extract_module
from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.pipeline.extract import (
    ExtractionError,
    InputChangedError,
    extract_all,
    regular_pbf_files,
)


def paths_for(tmp_path: Path, raw: Path) -> DataPaths:
    source = tmp_path / "source"
    source.mkdir(exist_ok=True)
    return DataPaths(tmp_path / "data", raw, source, source)


def test_extract_all_writes_one_bounded_identity_file_per_pbf(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "b-latest.osm.pbf").write_bytes(b"b")
    (raw / "a-latest.osm.pbf").write_bytes(b"a")
    seen: list[str] = []

    def scanner(path: Path, callback) -> None:
        seen.append(path.name)
        callback(OsmIdentity("way", len(seen)))
        callback(OsmIdentity("relation", len(seen) + 10))

    result = extract_all(paths_for(tmp_path, raw), "fixture", scanner=scanner, batch_rows=1)

    assert seen == ["a-latest.osm.pbf", "b-latest.osm.pbf"]
    assert result.occurrence_count == 4
    assert sorted(path.name for path in (result.run_root / "raw-identities").glob("*.parquet")) == [
        "a-latest.parquet",
        "b-latest.parquet",
    ]
    assert not (result.run_root / "occurrences").exists()
    assert (
        pq.ParquetFile(result.run_root / "raw-identities" / "a-latest.parquet").metadata.num_rows
        == 2
    )


def test_extract_all_resumes_a_completed_pbf_without_scanning_it_again(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "fixture-latest.osm.pbf").write_bytes(b"fixture")
    calls = 0

    def scanner(path: Path, callback) -> None:
        nonlocal calls
        calls += 1
        callback(OsmIdentity("way", 1))

    first = extract_all(paths_for(tmp_path, raw), "resume", scanner=scanner, resume=True)
    checkpoint = json.loads(
        (first.run_root / "checkpoints" / "fixture-latest.json").read_text(encoding="utf-8")
    )
    assert checkpoint["scanner_mode"] == "custom"
    assert checkpoint["row_count"] == 1
    assert calls == 1

    def unexpected(path: Path, callback) -> None:
        del path, callback
        raise AssertionError("completed PBF was rescanned")

    second = extract_all(paths_for(tmp_path, raw), "resume", scanner=unexpected, resume=True)
    assert second.occurrence_count == 1
    assert calls == 1


def test_checkpoint_source_snapshot_reuses_saved_digest_for_unchanged_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pbf = tmp_path / "fixture.osm.pbf"
    pbf.write_bytes(b"fixture")
    current = extract_module.SourceSnapshot.read(pbf)
    payload = {
        "size_bytes": current.size_bytes,
        "mtime_ns": current.mtime_ns,
        "sha256": current.sha256,
    }

    def fail_if_hashed(path: Path) -> extract_module.SourceSnapshot:
        raise AssertionError(f"resumed source was hashed: {path}")

    monkeypatch.setattr(extract_module.SourceSnapshot, "read", fail_if_hashed)

    snapshot = extract_module._checkpoint_source_snapshot(payload, pbf)

    assert snapshot == current


def test_checkpoint_source_snapshot_hashes_when_metadata_is_stale(tmp_path: Path) -> None:
    pbf = tmp_path / "fixture.osm.pbf"
    pbf.write_bytes(b"fixture")
    current = extract_module.SourceSnapshot.read(pbf)
    payload = {
        "size_bytes": current.size_bytes + 1,
        "mtime_ns": current.mtime_ns,
        "sha256": current.sha256,
    }

    assert extract_module._checkpoint_source_snapshot(payload, pbf) == current


def test_extract_all_detects_source_mutation_and_does_not_checkpoint(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"before")

    def scanner(path: Path, callback) -> None:
        callback(OsmIdentity("way", 1))
        path.write_bytes(b"changed")

    with pytest.raises(InputChangedError, match="changed during scan"):
        extract_all(paths_for(tmp_path, raw), "changed", scanner=scanner, resume=True)
    assert not (
        tmp_path / "data" / "runs" / "changed" / "checkpoints" / "fixture-latest.json"
    ).exists()


def test_extract_inventory_rejects_bad_raw_roots_and_unsafe_parallel_scanners(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    paths = DataPaths(tmp_path / "data", tmp_path / "missing", source, source)
    with pytest.raises(ExtractionError, match="not a directory"):
        extract_all(paths, "missing")

    raw = tmp_path / "raw"
    raw.mkdir()
    outside = tmp_path / "outside-latest.osm.pbf"
    outside.write_bytes(b"x")
    (raw / "outside-latest.osm.pbf").symlink_to(outside)
    assert regular_pbf_files(raw) == ()
    with pytest.raises(ExtractionError, match="no regular"):
        extract_all(DataPaths(tmp_path / "other-data", raw, source, source), "empty")

    (raw / "fixture-latest.osm.pbf").write_bytes(b"fixture")

    def custom(path: Path, callback) -> None:
        del path, callback

    with pytest.raises(ExtractionError, match="default scanner"):
        extract_all(
            DataPaths(tmp_path / "parallel-data", raw, source, source),
            "parallel",
            scanner=custom,
            workers=2,
        )


def test_extract_all_parallel_default_scanner_returns_deterministic_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "a-latest.osm.pbf").write_bytes(b"a")
    (raw / "b-latest.osm.pbf").write_bytes(b"b")

    class Future:
        def __init__(self, value: object) -> None:
            self.value = value

        def result(self) -> object:
            return self.value

    class Executor:
        workers: int

        def __init__(self, *, max_workers: int) -> None:
            self.workers = max_workers

        def __enter__(self) -> "Executor":
            return self

        def __exit__(self, *args: object) -> None:
            del args

        def submit(self, function, *args: object) -> Future:
            return Future(function(*args))

    monkeypatch.setattr(extract_module, "ProcessPoolExecutor", Executor)
    monkeypatch.setattr(
        extract_module, "scan_pbf_keys", lambda path, callback: callback(OsmIdentity("way", 1))
    )
    result = extract_all(
        DataPaths(tmp_path / "data", raw, tmp_path / "source", tmp_path / "source"),
        "parallel",
        scanner=extract_module.scan_pbf_keys,
        workers=2,
    )
    assert result.occurrence_count == 2


def test_extract_validation_helpers_cover_unreadable_and_unstable_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ExtractionError, match="cannot read source PBF"):
        extract_module.SourceSnapshot.read(tmp_path / "missing.osm.pbf")
    with pytest.raises(ExtractionError, match="cannot read source PBF"):
        extract_module.SourceSnapshot.stat_only(tmp_path / "missing.osm.pbf")

    class BrokenPath:
        def resolve(self) -> Path:
            raise OSError("resolve failed")

    assert extract_module._regular_file_under(BrokenPath(), tmp_path) is False  # type: ignore[arg-type]

    monkeypatch.setattr(extract_module.os, "access", lambda path, mode: False)
    with pytest.raises(ExtractionError, match="unreadable"):
        extract_module._require_readable((tmp_path / "raw.osm.pbf",))

    before = extract_module.SourceSnapshot(tmp_path / "source", 1, 2, "a")
    after = extract_module.SourceSnapshot(tmp_path / "source", 1, 3, "a")
    with pytest.raises(InputChangedError, match="changed during scan"):
        extract_module._assert_unchanged(before, after)
    extract_module._assert_unchanged(before, extract_module.SourceSnapshot(before.path, 1, 2, ""))
    with pytest.raises(InputChangedError, match="changed during scan"):
        extract_module._assert_unchanged(
            before, extract_module.SourceSnapshot(before.path, 1, 2, "b")
        )


def test_extract_output_validation_rejects_bad_metadata_schema_stats_and_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "identity.parquet"
    pq.write_table(pa.table({"wrong": [1]}), output)
    assert extract_module._output_metadata_matches(output, 1, {"row_count": 1}) is False

    monkeypatch.setattr(
        extract_module.pq,
        "ParquetFile",
        lambda path: SimpleNamespace(metadata=None),
    )
    assert extract_module._output_metadata_matches(output, 1, {"row_count": 1}) is False
    monkeypatch.undo()

    with extract_module.IdentityParquetWriter(tmp_path, filename="valid.parquet") as writer:
        writer.write(OsmIdentity("way", 1))
    valid = tmp_path / "valid.parquet"
    stat = valid.stat()
    expected = {
        "row_count": 1,
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sha256": extract_module._sha256(valid),
    }
    assert extract_module._output_matches(valid, 2, expected) is False
    wrong_size = {**expected, "size_bytes": expected["size_bytes"] + 1}
    assert extract_module._output_matches(valid, 1, wrong_size) is False
    wrong_hash = {**expected, "sha256": "wrong"}
    assert extract_module._output_matches(valid, 1, wrong_hash) is False
    assert extract_module._output_matches(valid, 1, {"row_count": 1}) is False


def test_extract_checkpoint_helpers_reject_invalid_payloads(tmp_path: Path) -> None:
    pbf = tmp_path / "fixture.osm.pbf"
    pbf.write_bytes(b"fixture")
    current = extract_module.SourceSnapshot.read(pbf)
    checkpoint = tmp_path / "checkpoints" / "fixture.json"
    checkpoint.parent.mkdir()
    checkpoint.write_text("[]", encoding="utf-8")
    assert (
        extract_module._read_checkpoint(tmp_path, pbf, scanner=extract_module.scan_pbf_keys) is None
    )

    assert extract_module._checkpoint_count({"row_count": "1"}) is None
    assert extract_module._checkpoint_count({"row_count": True}) is None
    assert extract_module._checkpoint_count({"row_count": -1}) is None
    assert (
        extract_module._checkpoint_output_matches(tmp_path, pbf, 1, {"path": "wrong.parquet"})
        is False
    )
    assert extract_module._checkpoint_count_and_output(tmp_path, pbf, {"row_count": 1}) is None
    assert (
        extract_module._checkpoint_extraction(
            tmp_path, pbf, [], current, extract_module.scan_pbf_keys
        )
        is None
    )
    assert (
        extract_module._checkpoint_extraction(
            tmp_path,
            pbf,
            {"source_pbf": "other.osm.pbf"},
            current,
            extract_module.scan_pbf_keys,
        )
        is None
    )
    assert (
        extract_module._checkpoint_extraction(
            tmp_path,
            pbf,
            {
                "source_pbf": pbf.name,
                "scanner_mode": "coverage-only",
                "size_bytes": current.size_bytes,
                "mtime_ns": current.mtime_ns,
                "sha256": current.sha256,
                "row_count": 1,
            },
            current,
            extract_module.scan_pbf_keys,
        )
        is None
    )


def test_extract_checkpoint_writer_rejects_missing_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pbf = tmp_path / "fixture.osm.pbf"
    pbf.write_bytes(b"fixture")
    output = extract_module._output_path(tmp_path, pbf)
    output.parent.mkdir(parents=True)
    output.write_bytes(b"output")
    current = extract_module.SourceSnapshot.read(pbf)
    extraction = extract_module._SourceExtraction(
        0, extract_module.SourceInventory(current, current)
    )
    monkeypatch.setattr(
        extract_module.pq,
        "ParquetFile",
        lambda path: SimpleNamespace(metadata=None),
    )
    with pytest.raises(ExtractionError, match="invalid extracted Parquet metadata"):
        extract_module._write_checkpoint(
            tmp_path, pbf, extraction, scanner=extract_module.scan_pbf_keys
        )


def test_extract_rejects_invalid_worker_counts_and_existing_run_root(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="positive"):
        extract_module._validate_worker_configuration(0, extract_module.scan_pbf_keys)
    with pytest.raises(ValueError, match="<="):
        extract_module._validate_worker_configuration(
            extract_module.MAX_WORKERS + 1, extract_module.scan_pbf_keys
        )

    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "fixture.osm.pbf").write_bytes(b"fixture")
    data = tmp_path / "data"
    existing = data / "runs" / "existing"
    existing.mkdir(parents=True)
    source = tmp_path / "source"
    source.mkdir()
    with pytest.raises(ExtractionError, match="already exists"):
        extract_all(DataPaths(data, raw, source, source), "existing")
