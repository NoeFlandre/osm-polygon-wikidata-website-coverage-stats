import inspect
import json
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
from osm_polygon_wikidata_website_coverage.io.pbf import scan_pbf_keys
from osm_polygon_wikidata_website_coverage.pipeline.extract import (
    ExtractionError,
    InputChangedError,
    SourceInventory,
    SourceSnapshot,
    _assert_unchanged,
    _checkpoint_counts,
    _checkpoint_matches,
    _checkpoint_path,
    _emit_to_writers,
    _extract_in_parallel,
    _extract_sources,
    _load_checkpoint_payload,
    _nonnegative_count,
    _pbf_files,
    _prepare_run_root,
    _read_checkpoint,
    _read_checkpoint_fields,
    _remove_incomplete_outputs,
    _source_output_paths,
    _source_path_matches,
    _source_temporary_paths,
    _write_checkpoint,
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

    assert result.occurrence_count == 2
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


def test_extract_all_rejects_nonpositive_workers(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()

    with pytest.raises(ValueError, match=r"^workers must be positive$"):
        extract_all(_paths(tmp_path, raw), "invalid-workers", workers=0)


def test_extract_all_rejects_parallel_custom_scanners(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "fixture-latest.osm.pbf").write_bytes(b"fixture")

    def scanner(path: Path, callback) -> None:
        del path, callback

    with pytest.raises(
        ExtractionError,
        match=r"^parallel extraction requires the default scanner$",
    ):
        extract_all(_paths(tmp_path, raw), "custom-scanner", scanner=scanner, workers=2)


def test_extract_all_runs_default_scanner_workers_in_deterministic_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "b-latest.osm.pbf").write_bytes(b"b")
    (raw / "a-latest.osm.pbf").write_bytes(b"a")
    seen: list[str] = []
    executor_workers: list[int] = []

    def scanner(path: Path, callback) -> None:
        seen.append(path.name)
        callback(_occurrence(path.name, len(seen)))

    class Future:
        def __init__(self, value: object) -> None:
            self.value = value

        def result(self) -> object:
            return self.value

    class Executor:
        max_workers: int

        def __init__(self, *, max_workers: int) -> None:
            executor_workers.append(max_workers)
            self.max_workers = max_workers

        def __enter__(self) -> "Executor":
            return self

        def __exit__(self, *args: object) -> None:
            del args

        def submit(self, function, *args: object) -> Future:
            return Future(function(*args))

    monkeypatch.setattr(extract_module, "scan_pbf", scanner)
    monkeypatch.setattr(extract_module, "ProcessPoolExecutor", Executor)

    result = extract_all(
        _paths(tmp_path, raw), "parallel", scanner=scanner, batch_rows=1, workers=2
    )

    assert seen == ["a-latest.osm.pbf", "b-latest.osm.pbf"]
    assert executor_workers == [2]
    assert result.occurrence_count == 2
    assert len(result.source_inventory) == 2


def test_extract_all_resumes_completed_sources_without_rescanning(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    calls: list[str] = []

    def scanner(path: Path, callback) -> None:
        calls.append(path.name)
        callback(_occurrence(path.name, 1))

    first = extract_all(
        _paths(tmp_path, raw),
        "resumable",
        scanner=scanner,
        batch_rows=1,
        resume=True,
    )
    assert first.occurrence_count == 1
    assert (first.run_root / "checkpoints" / "fixture-latest.json").is_file()

    def unexpected_scan(path: Path, callback) -> None:
        del path, callback
        raise AssertionError("completed source should not be rescanned")

    second = extract_all(
        _paths(tmp_path, raw),
        "resumable",
        scanner=unexpected_scan,
        batch_rows=1,
        resume=True,
    )

    assert second.occurrence_count == 1
    assert calls == [pbf.name]


def test_extract_all_rescans_incomplete_sources_after_cleaning_derived_shards(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")

    def scanner(path: Path, callback) -> None:
        callback(_occurrence(path.name, 1))

    first = extract_all(
        _paths(tmp_path, raw),
        "recoverable",
        scanner=scanner,
        batch_rows=1,
        resume=True,
    )
    checkpoint = first.run_root / "checkpoints" / "fixture-latest.json"
    checkpoint.unlink()
    stale = first.run_root / "occurrences" / "fixture-latest-99999.parquet"
    stale.write_bytes(b"stale")
    temporary = first.run_root / "geometry-failures" / ".fixture-latest-00000.parquet.tmp"
    temporary.write_bytes(b"stale")

    result = extract_all(
        _paths(tmp_path, raw),
        "recoverable",
        scanner=scanner,
        batch_rows=1,
        resume=True,
    )

    assert result.occurrence_count == 1
    assert not stale.exists()
    assert not temporary.exists()
    assert sorted(path.name for path in (result.run_root / "occurrences").glob("*.parquet")) == [
        "fixture-latest-00000.parquet"
    ]


def test_extract_resume_helpers_use_exact_source_and_checkpoint_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "fixture-latest.osm.pbf"
    source.write_bytes(b"fixture")
    run_root = tmp_path / "nested" / "run"

    assert (
        _source_path_matches(
            tmp_path / "fixture-latest-00000.parquet", "fixture-latest-", ".parquet"
        )
        is False
    )
    matching = tmp_path / "fixture-latest-00000.parquet"
    matching.write_bytes(b"shard")
    assert _source_path_matches(matching, "fixture-latest-", ".parquet") is True
    wrong_suffix = tmp_path / "fixture-latest-00000.tmp"
    wrong_suffix.write_bytes(b"temporary")
    assert _source_path_matches(wrong_suffix, "fixture-latest-", ".parquet") is False
    wrong_prefix = tmp_path / "other-00000.parquet"
    wrong_prefix.write_bytes(b"other")
    assert _source_path_matches(wrong_prefix, "fixture-latest-", ".parquet") is False
    directory_with_matching_suffix = tmp_path / "fixture-latest-directory.parquet"
    directory_with_matching_suffix.mkdir()
    assert (
        _source_path_matches(directory_with_matching_suffix, "fixture-latest-", ".parquet") is False
    )

    checkpoint_root = run_root / "checkpoints"
    snapshot = SourceSnapshot.read(source)
    extraction = extract_module._SourceExtraction(0, 0, SourceInventory(snapshot, snapshot))
    _write_checkpoint(checkpoint_root, source, extraction)
    checkpoint = checkpoint_root / "fixture-latest.json"
    assert checkpoint.read_text(encoding="utf-8") == (
        '{\n  "failure_count": 0,\n  "mtime_ns": '
        f'{snapshot.mtime_ns},\n  "occurrence_count": 0,\n  "size_bytes": {snapshot.size_bytes},\n'
        '  "source_pbf": "fixture-latest.osm.pbf"\n}'
    )

    captured_encodings: list[str | None] = []
    original_write_text = Path.write_text

    def capture_write_text(
        path: Path,
        text: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        captured_encodings.append(encoding)
        return original_write_text(path, text, encoding=encoding, errors=errors, newline=newline)

    monkeypatch.setattr(Path, "write_text", capture_write_text)
    second_source = tmp_path / "second-latest.osm.pbf"
    second_source.write_bytes(b"second")
    _write_checkpoint(
        tmp_path / "second-checkpoints",
        second_source,
        extract_module._SourceExtraction(
            1,
            2,
            SourceInventory(SourceSnapshot.read(second_source), SourceSnapshot.read(second_source)),
        ),
    )
    assert captured_encodings == ["utf-8"]


def test_extract_checkpoint_reading_is_typed_and_accepts_zero_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "fixture-latest.osm.pbf"
    source.write_bytes(b"fixture")
    run_root = tmp_path / "run"
    checkpoint_root = run_root / "checkpoints"
    snapshot = SourceSnapshot.read(source)
    extraction = extract_module._SourceExtraction(0, 0, SourceInventory(snapshot, snapshot))
    _write_checkpoint(checkpoint_root, source, extraction)
    (run_root / "occurrences").mkdir(parents=True)
    (run_root / "geometry-failures").mkdir(parents=True)
    (run_root / "occurrences" / "fixture-latest-00000.parquet").write_bytes(b"occurrences")
    (run_root / "geometry-failures" / "fixture-latest-00000.parquet").write_bytes(b"failures")

    captured_encodings: list[str | None] = []
    original_read_text = Path.read_text

    def capture_read_text(
        path: Path,
        encoding: str | None = None,
        errors: str | None = None,
    ) -> str:
        captured_encodings.append(encoding)
        return original_read_text(path, encoding=encoding, errors=errors)

    monkeypatch.setattr(Path, "read_text", capture_read_text)
    loaded = _load_checkpoint_payload(
        checkpoint_root / "fixture-latest.json",
        checkpoint_root / ".fixture-latest.json.tmp",
        source,
    )
    assert loaded is not None
    assert captured_encodings == ["utf-8"]
    assert loaded[1] == snapshot
    assert loaded[2:] == (0, 0)
    saved = _read_checkpoint(checkpoint_root, run_root, source)
    assert saved is not None
    assert saved.occurrence_count == 0
    assert saved.failure_count == 0
    assert saved.source_inventory == SourceInventory(snapshot, snapshot)

    assert (
        _checkpoint_matches(
            {
                "source_pbf": source.name,
                "size_bytes": snapshot.size_bytes,
                "mtime_ns": snapshot.mtime_ns,
            },
            source,
            snapshot,
            0,
            0,
        )
        is True
    )
    assert (
        _checkpoint_matches(
            {
                "source_pbf": source.name,
                "size_bytes": snapshot.size_bytes,
                "mtime_ns": snapshot.mtime_ns,
            },
            source,
            snapshot,
            0,
            -1,
        )
        is False
    )

    checkpoint = checkpoint_root / "fixture-latest.json"
    temporary = checkpoint_root / ".fixture-latest.json.tmp"
    temporary.write_text("in progress", encoding="utf-8")
    assert _load_checkpoint_payload(checkpoint, temporary, source) is None


def test_extract_checkpoint_validation_helpers_cover_valid_and_invalid_values(
    tmp_path: Path,
) -> None:
    assert _nonnegative_count(0) == 0
    assert _nonnegative_count(7) == 7
    assert _nonnegative_count(-1) is None
    assert _nonnegative_count("7") is None

    assert _checkpoint_counts(1, 2) == (1, 2)
    assert _checkpoint_counts(-1, 2) is None
    assert _checkpoint_counts(1, -1) is None
    assert _checkpoint_counts("1", 2) is None
    assert _checkpoint_counts(1, "2") is None

    source = tmp_path / "fixture-latest.osm.pbf"
    source.write_bytes(b"fixture")
    checkpoint = tmp_path / "fixture-latest.json"
    payload = {"occurrence_count": 1, "failure_count": 2}
    checkpoint.write_text(json.dumps(payload), encoding="utf-8")
    fields = _read_checkpoint_fields(checkpoint, source)
    assert fields is not None
    assert fields[0] == payload
    assert fields[1] == SourceSnapshot.read(source)
    assert fields[2:] == (1, 2)

    malformed = tmp_path / "malformed.json"
    malformed.write_text("{", encoding="utf-8")
    assert _read_checkpoint_fields(malformed, source) is None


def test_extract_read_checkpoint_uses_exact_source_output_directory_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "fixture-latest.osm.pbf"
    source.write_bytes(b"fixture")
    run_root = tmp_path / "run"
    checkpoint_root = run_root / "checkpoints"
    snapshot = SourceSnapshot.read(source)
    extraction = extract_module._SourceExtraction(1, 2, SourceInventory(snapshot, snapshot))
    _write_checkpoint(checkpoint_root, source, extraction)
    calls: list[tuple[Path, str, str]] = []

    def source_output_paths(root: Path, directory: str, source_stem: str) -> tuple[Path, ...]:
        calls.append((root, directory, source_stem))
        return (root / directory / f"{source_stem}-00000.parquet",)

    monkeypatch.setattr(extract_module, "_source_output_paths", source_output_paths)
    loaded = _read_checkpoint(checkpoint_root, run_root, source)

    assert loaded == extraction
    assert calls == [
        (run_root, "occurrences", "fixture-latest"),
        (run_root, "geometry-failures", "fixture-latest"),
    ]


def test_extract_cleanup_removes_all_source_outputs_and_checkpoint_temps(tmp_path: Path) -> None:
    source = tmp_path / "fixture-latest.osm.pbf"
    source.write_bytes(b"fixture")
    run_root = tmp_path / "run"
    for directory, name in (
        ("occurrences", "fixture-latest-00000.parquet"),
        ("geometry-failures", "fixture-latest-00000.parquet"),
    ):
        (run_root / directory).mkdir(parents=True, exist_ok=True)
        (run_root / directory / name).write_bytes(b"stale")
        (run_root / directory / ".fixture-latest-00001.parquet.tmp").write_bytes(b"stale")
    (run_root / "checkpoints").mkdir(parents=True)
    (run_root / "checkpoints" / "fixture-latest.json").write_bytes(b"stale")
    (run_root / "checkpoints" / ".fixture-latest.json.tmp").write_bytes(b"stale")

    _remove_incomplete_outputs(run_root, source)

    assert not list((run_root / "occurrences").iterdir())
    assert not list((run_root / "geometry-failures").iterdir())
    assert not list((run_root / "checkpoints").iterdir())


def test_extract_cleanup_uses_exact_directory_and_checkpoint_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "fixture-latest.osm.pbf"
    source.write_bytes(b"fixture")
    run_root = tmp_path / "run"
    output_directories: list[str] = []
    temporary_directories: list[str] = []
    checkpoint_roots: list[Path] = []

    def source_output_paths(root: Path, directory: str, source_stem: str) -> tuple[Path, ...]:
        del root, source_stem
        output_directories.append(directory)
        return ()

    def source_temporary_paths(root: Path, directory: str, source_stem: str) -> tuple[Path, ...]:
        del root, source_stem
        temporary_directories.append(directory)
        return ()

    def checkpoint_path(root: Path, pbf_path: Path) -> Path:
        checkpoint_roots.append(root)
        return root / f"{pbf_path.stem}.json"

    monkeypatch.setattr(extract_module, "_source_output_paths", source_output_paths)
    monkeypatch.setattr(extract_module, "_source_temporary_paths", source_temporary_paths)
    monkeypatch.setattr(extract_module, "_checkpoint_path", checkpoint_path)

    _remove_incomplete_outputs(run_root, source)

    assert output_directories == ["occurrences", "geometry-failures"]
    assert temporary_directories == ["occurrences", "geometry-failures"]
    assert checkpoint_roots == [run_root / "checkpoints"]


def test_extract_prepare_run_root_creates_the_three_resume_directories(tmp_path: Path) -> None:
    run_root = tmp_path / "nested" / "run"

    assert _prepare_run_root(run_root, resume=False) is None
    assert (run_root / "occurrences").is_dir()
    assert (run_root / "geometry-failures").is_dir()
    assert not (run_root / "checkpoints").exists()
    checkpoint_root = _prepare_run_root(run_root, resume=True)
    assert checkpoint_root == run_root / "checkpoints"
    assert checkpoint_root.is_dir()

    with pytest.raises(ExtractionError) as error:
        _prepare_run_root(run_root, resume=False)
    assert str(error.value) == f"run root already exists: {run_root}"


def test_extract_prepare_run_root_uses_exact_resume_directory_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_root = tmp_path / "run"
    mkdir_calls: list[tuple[Path, bool, bool]] = []
    original_mkdir = Path.mkdir

    def tracked_mkdir(
        path: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        del mode
        mkdir_calls.append((path, parents, exist_ok))
        original_mkdir(path, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", tracked_mkdir)
    assert _prepare_run_root(run_root, resume=True) == run_root / "checkpoints"

    child_calls = [call for call in mkdir_calls if call[0].parent == run_root]
    assert {call[0] for call in child_calls} == {
        run_root / "occurrences",
        run_root / "geometry-failures",
        run_root / "checkpoints",
    }
    assert (run_root / "occurrences", True, True) in child_calls
    assert (run_root / "geometry-failures", False, True) in child_calls
    assert (run_root / "checkpoints", False, True) in child_calls


def test_extract_parallel_helpers_forward_the_checkpoint_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "fixture-latest.osm.pbf"
    source.write_bytes(b"fixture")
    run_root = tmp_path / "run"
    checkpoint_root = run_root / "checkpoints"
    snapshot = SourceSnapshot.read(source)
    extraction = extract_module._SourceExtraction(1, 0, SourceInventory(snapshot, snapshot))
    source_calls: list[Path | None] = []

    def fake_parallel(
        pbf_files: tuple[Path, ...],
        actual_run_root: Path,
        batch_rows: int,
        workers: int,
        scanner: object,
        actual_checkpoint_root: Path | None,
    ) -> tuple[extract_module._SourceExtraction, ...]:
        assert pbf_files == (source,)
        assert actual_run_root == run_root
        assert batch_rows == 17
        assert workers == 2
        assert scanner is scan_pbf_keys
        source_calls.append(actual_checkpoint_root)
        return (extraction,)

    monkeypatch.setattr(extract_module, "_extract_in_parallel", fake_parallel)
    assert _extract_sources((source,), run_root, 17, scan_pbf_keys, 2, checkpoint_root) == (
        extraction,
    )
    assert source_calls == [checkpoint_root]

    submitted: list[tuple[object, tuple[object, ...]]] = []

    class Future:
        def result(self) -> extract_module._SourceExtraction:
            return extraction

    class Executor:
        def __init__(self, *, max_workers: int) -> None:
            assert max_workers == 3

        def __enter__(self) -> "Executor":
            return self

        def __exit__(self, *args: object) -> None:
            del args

        def submit(self, function: object, *args: object) -> Future:
            submitted.append((function, args))
            return Future()

    monkeypatch.setattr(extract_module, "ProcessPoolExecutor", Executor)
    assert _extract_in_parallel((source,), run_root, 17, 3, scan_pbf_keys, checkpoint_root) == (
        extraction,
    )
    assert submitted == [
        (
            extract_module._extract_one,
            (source, run_root, 17, scan_pbf_keys, checkpoint_root),
        )
    ]


def test_extract_checkpoint_validation_is_fail_closed(tmp_path: Path) -> None:
    pbf = tmp_path / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    run_root = tmp_path / "run"
    checkpoint_root = run_root / "checkpoints"
    snapshot = SourceSnapshot.read(pbf)
    extraction = extract_module._SourceExtraction(1, 0, SourceInventory(snapshot, snapshot))

    assert _source_output_paths(run_root, "occurrences", "fixture-latest") == ()
    assert _source_temporary_paths(run_root, "geometry-failures", "fixture-latest") == ()
    _write_checkpoint(checkpoint_root, pbf, extraction)
    with pytest.raises(FileExistsError, match="extraction checkpoint"):
        _write_checkpoint(checkpoint_root, pbf, extraction)

    checkpoint = _checkpoint_path(checkpoint_root, pbf)
    assert _read_checkpoint(checkpoint_root, run_root, pbf) is None

    occurrences = run_root / "occurrences" / "fixture-latest-00000.parquet"
    failures = run_root / "geometry-failures" / "fixture-latest-00000.parquet"
    occurrences.parent.mkdir(parents=True)
    failures.parent.mkdir(parents=True)
    occurrences.write_bytes(b"occurrences")
    assert _read_checkpoint(checkpoint_root, run_root, pbf) is None
    failures.write_bytes(b"failures")
    assert _read_checkpoint(checkpoint_root, run_root, pbf) is not None

    checkpoint.write_text("{", encoding="utf-8")
    assert _read_checkpoint(checkpoint_root, run_root, pbf) is None
    checkpoint.write_text(json.dumps([]), encoding="utf-8")
    assert _read_checkpoint(checkpoint_root, run_root, pbf) is None
    checkpoint.write_text(
        json.dumps(
            {
                "source_pbf": pbf.name,
                "size_bytes": pbf.stat().st_size + 1,
                "mtime_ns": pbf.stat().st_mtime_ns,
                "occurrence_count": 1,
                "failure_count": 0,
            }
        ),
        encoding="utf-8",
    )
    assert _read_checkpoint(checkpoint_root, run_root, pbf) is None
    valid_payload = {
        "source_pbf": pbf.name,
        "size_bytes": snapshot.size_bytes,
        "mtime_ns": snapshot.mtime_ns,
        "occurrence_count": 1,
        "failure_count": 0,
    }
    checkpoint.write_text(json.dumps({**valid_payload, "occurrence_count": "1"}), encoding="utf-8")
    assert _read_checkpoint(checkpoint_root, run_root, pbf) is None
    checkpoint.write_text(json.dumps({**valid_payload, "failure_count": -1}), encoding="utf-8")
    assert _read_checkpoint(checkpoint_root, run_root, pbf) is None
    checkpoint.write_text(json.dumps({**valid_payload, "failure_count": "0"}), encoding="utf-8")
    assert _read_checkpoint(checkpoint_root, run_root, pbf) is None
    assert not _checkpoint_matches([], pbf, snapshot, 1, 0)
    assert not _checkpoint_matches(
        {"source_pbf": pbf.name, "size_bytes": snapshot.size_bytes, "mtime_ns": snapshot.mtime_ns},
        pbf,
        snapshot,
        -1,
        0,
    )


def test_extract_all_allows_key_scanner_in_parallel_and_forwards_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    snapshot = SourceSnapshot(pbf, pbf.stat().st_size, pbf.stat().st_mtime_ns)
    extraction = extract_module._SourceExtraction(1, 0, SourceInventory(snapshot, snapshot))
    calls: list[tuple[object, Path, int, int, object]] = []

    def fake_parallel(
        pbf_files: object,
        run_root: Path,
        batch_rows: int,
        workers: int,
        scanner: object,
        checkpoint_root: Path | None,
    ) -> tuple[object, ...]:
        calls.append((pbf_files, run_root, batch_rows, workers, scanner))
        assert checkpoint_root is None
        return (extraction,)

    monkeypatch.setattr(extract_module, "_extract_in_parallel", fake_parallel)

    result = extract_all(_paths(tmp_path, raw), "key-scanner", scanner=scan_pbf_keys, workers=2)

    assert result.occurrence_count == 1
    assert calls == [
        ((pbf,), result.run_root, 5_000, 2, scan_pbf_keys),
    ]


def test_extract_all_requires_one_extraction_result_per_pending_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    first = raw / "a-latest.osm.pbf"
    second = raw / "b-latest.osm.pbf"
    first.write_bytes(b"a")
    second.write_bytes(b"b")
    snapshot = SourceSnapshot.read(first)
    extraction = extract_module._SourceExtraction(1, 0, SourceInventory(snapshot, snapshot))

    def short_extract(
        pbf_files: tuple[Path, ...],
        run_root: Path,
        batch_rows: int,
        scanner: object,
        workers: int,
        checkpoint_root: Path | None,
    ) -> tuple[extract_module._SourceExtraction, ...]:
        del run_root, batch_rows, scanner, workers, checkpoint_root
        assert pbf_files == (first, second)
        return (extraction,)

    monkeypatch.setattr(extract_module, "_extract_sources", short_extract)

    with pytest.raises(ValueError, match="zip"):
        extract_all(_paths(tmp_path, raw), "short-extraction")


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
