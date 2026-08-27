import json
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.publishing.hf as hf_module
from osm_polygon_wikidata_website_coverage.pipeline.aggregate import EXPECTED_GLOBAL_COLUMNS
from osm_polygon_wikidata_website_coverage.publishing.hf import (
    PublicationBoundaryError,
    _check_completed_run,
    _copy_staging_files,
    _copy_summary_json,
    _parquet_files,
    _prepare_destination,
    _repository_file,
    _staging_files,
    _validate_schema,
    _validate_staging_files,
    stage_hf,
)


def _write_run(tmp_path: Path, *, include_forbidden: bool = False) -> Path:
    run = tmp_path / "run"
    (run / "coverage" / "global").mkdir(parents=True)
    (run / "summaries").mkdir()
    (run / "manifests").mkdir()
    row = {column: None for column in EXPECTED_GLOBAL_COLUMNS}
    row.update(
        {
            "osm_type": "way",
            "osm_id": 1,
            "source_pbf": "fixture-latest.osm.pbf",
            "source_pbfs": '["fixture-latest.osm.pbf"]',
            "region": "fixture",
            "geometry_type": "Polygon",
            "area_m2": 1.0,
            "website": True,
            "wikipedia": False,
            "wikivoyage": False,
            "covered_by_any_text": True,
            "overlap_category": "website_only",
        }
    )
    if include_forbidden:
        row["geometry"] = "full geometry"
    pq.write_table(pa.Table.from_pylist([row]), run / "coverage" / "global" / "shard-00.parquet")
    pq.write_table(
        pa.Table.from_pylist(
            [{"scope": "global", "group_name": "all", "metric": "valid", "value": 1.0}]
        ),
        run / "summaries" / "global.parquet",
    )
    (run / "manifests" / "manifest.json").write_text(
        json.dumps({"status": "complete", "run_id": "fixture"}), encoding="utf-8"
    )
    return run


def _schema_names(stage: Path) -> set[str]:
    names: set[str] = set()
    for path in stage.rglob("*.parquet"):
        names.update(pq.read_schema(path).names)
    return names


def test_hf_staging_contains_no_text_or_full_geometry(tmp_path: Path) -> None:
    stage = stage_hf(_write_run(tmp_path), tmp_path / "hf")

    names = _schema_names(stage)
    assert all("website_text" not in field for field in names)
    assert all("full_text" not in field for field in names)
    assert "geometry" not in names
    assert (stage / "README.md").is_file()
    assert (stage / "CITATION.cff").is_file()
    assert (stage / "LICENSE").is_file()
    assert (stage / "ATTRIBUTION.md").is_file()
    assert (stage / "data" / "coverage" / "global" / "shard-00.parquet").is_file()


def test_hf_staging_rejects_forbidden_schema(tmp_path: Path) -> None:
    with pytest.raises(PublicationBoundaryError, match="geometry"):
        stage_hf(_write_run(tmp_path, include_forbidden=True), tmp_path / "hf")


def test_hf_staging_rejects_nonempty_destination(tmp_path: Path) -> None:
    destination = tmp_path / "hf"
    destination.mkdir()
    (destination / "existing.txt").write_text("keep", encoding="utf-8")

    with pytest.raises(PublicationBoundaryError, match="empty"):
        stage_hf(_write_run(tmp_path), destination)


@pytest.mark.parametrize(
    ("manifest_text", "message"),
    [
        (None, "manifest is missing"),
        ("not-json", "not valid JSON"),
        (json.dumps({"status": "running"}), "not complete"),
    ],
)
def test_hf_staging_requires_a_valid_complete_manifest(
    tmp_path: Path, manifest_text: str | None, message: str
) -> None:
    run = tmp_path / "run"
    (run / "manifests").mkdir(parents=True)
    if manifest_text is not None:
        (run / "manifests" / "manifest.json").write_text(manifest_text, encoding="utf-8")

    with pytest.raises(PublicationBoundaryError, match=message):
        stage_hf(run, tmp_path / "hf")


def test_hf_staging_rejects_missing_or_empty_artifact_directories(tmp_path: Path) -> None:
    with pytest.raises(PublicationBoundaryError, match="directory is missing"):
        _parquet_files(tmp_path / "missing", "global coverage")

    run = _write_run(tmp_path)
    (run / "coverage" / "global" / "shard-00.parquet").unlink()
    with pytest.raises(PublicationBoundaryError, match="no Parquet"):
        stage_hf(run, tmp_path / "hf-missing-global")

    run = _write_run(tmp_path / "empty-summary")
    (run / "summaries" / "global.parquet").unlink()
    with pytest.raises(PublicationBoundaryError, match="no Parquet"):
        stage_hf(run, tmp_path / "hf-empty-summary")


def test_hf_staging_rejects_a_noncompact_global_schema(tmp_path: Path) -> None:
    run = _write_run(tmp_path)
    pq.write_table(
        pa.Table.from_pylist([{"osm_type": "way"}]),
        run / "coverage" / "global" / "bad.parquet",
    )

    with pytest.raises(PublicationBoundaryError, match="compact global schema"):
        stage_hf(run, tmp_path / "hf")


def test_hf_staging_copies_optional_summary_json_and_uses_run_name_fallback(tmp_path: Path) -> None:
    run = _write_run(tmp_path)
    (run / "reports").mkdir()
    (run / "reports" / "summary.json").write_text("{}", encoding="utf-8")
    (run / "manifests" / "manifest.json").write_text(
        json.dumps({"status": "complete"}), encoding="utf-8"
    )

    stage = stage_hf(run, tmp_path / "hf")

    assert (stage / "data" / "summary.json").read_text(encoding="utf-8") == "{}"
    assert "run `run`" in (stage / "README.md").read_text(encoding="utf-8")


def test_hf_publication_reports_missing_repository_files(tmp_path: Path) -> None:
    with pytest.raises(PublicationBoundaryError, match="publication file is missing"):
        _repository_file("definitely-missing-publication-file")


def test_hf_private_helpers_have_exact_paths_and_error_contracts(tmp_path: Path) -> None:
    run = _write_run(tmp_path)
    manifest = run / "manifests" / "manifest.json"
    _check_completed_run(run)

    running_manifest = tmp_path / "running" / "manifests" / "manifest.json"
    running_manifest.parent.mkdir(parents=True)
    running_manifest.write_text(json.dumps({"status": "running"}), encoding="utf-8")
    with pytest.raises(PublicationBoundaryError) as error:
        _check_completed_run(running_manifest.parents[1])
    assert str(error.value) == "run manifest is not complete"

    assert _staging_files(run) == (
        (run / "coverage" / "global" / "shard-00.parquet",),
        (run / "summaries" / "global.parquet",),
    )
    assert _repository_file("CITATION.cff") == Path(__file__).parents[2] / "CITATION.cff"
    assert manifest.read_text(encoding="utf-8")


def test_hf_parquet_sort_uses_filename() -> None:
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


def test_hf_schema_validation_has_exact_forbidden_and_global_contracts(tmp_path: Path) -> None:
    forbidden = tmp_path / "forbidden.parquet"
    pq.write_table(pa.Table.from_pydict({"geometry": ["x"], "full_text": ["y"]}), forbidden)
    with pytest.raises(PublicationBoundaryError) as forbidden_error:
        _validate_schema(forbidden, is_global=False)
    assert str(forbidden_error.value) == (
        f"{forbidden} contains forbidden fields: full_text, geometry"
    )

    invalid_global = tmp_path / "invalid-global.parquet"
    pq.write_table(pa.Table.from_pydict({"osm_type": ["way"]}), invalid_global)
    with pytest.raises(PublicationBoundaryError) as global_error:
        _validate_schema(invalid_global, is_global=True)
    assert str(global_error.value) == f"{invalid_global} does not match the compact global schema"


def test_hf_destination_creation_is_nested_idempotent_and_exact(tmp_path: Path) -> None:
    destination = tmp_path / "nested" / "deeper" / "hf"
    _prepare_destination(destination)
    _prepare_destination(destination)
    assert destination.is_dir()
    (destination / "existing.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(PublicationBoundaryError) as error:
        _prepare_destination(destination)
    assert str(error.value) == "HF staging destination must be empty"


def test_hf_validation_and_copy_helpers_preserve_each_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    global_file = tmp_path / "global.parquet"
    summary_file = tmp_path / "summary.parquet"
    global_file.write_bytes(b"global")
    summary_file.write_bytes(b"summary")
    global_files = (global_file,)
    summary_files = (summary_file,)
    calls: list[tuple[Path, bool]] = []

    def fake_validate(path: Path, *, is_global: bool) -> None:
        calls.append((path, is_global))

    monkeypatch.setattr(hf_module, "_validate_schema", fake_validate)
    _validate_staging_files(global_files, summary_files)
    assert calls == [(global_file, True), (summary_file, False)]

    destination = tmp_path / "stage"
    _copy_staging_files(destination, global_files, summary_files)
    assert (
        destination / "data" / "coverage" / "global" / "global.parquet"
    ).read_bytes() == b"global"
    assert (destination / "data" / "summaries" / "summary.parquet").read_bytes() == b"summary"


def test_hf_summary_copy_and_stage_preserve_exact_names_and_utf8(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = _write_run(tmp_path)
    reports = run / "reports"
    reports.mkdir()
    (reports / "summary.json").write_text('{"value":"café"}', encoding="utf-8")
    destination = tmp_path / "summary-stage"
    (destination / "data").mkdir(parents=True)
    _copy_summary_json(run, destination)
    assert (destination / "data" / "summary.json").read_text(encoding="utf-8") == (
        '{"value":"café"}'
    )

    read_encodings: list[str | None] = []
    write_encodings: list[tuple[str, str | None]] = []
    original_read_text = Path.read_text
    original_write_text = Path.write_text

    def tracked_read_text(
        path: Path, encoding: str | None = None, errors: str | None = None
    ) -> str:
        read_encodings.append(encoding)
        return original_read_text(path, encoding=encoding, errors=errors)

    def tracked_write_text(
        path: Path,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        write_encodings.append((path.name, encoding))
        return original_write_text(path, data, encoding=encoding, errors=errors, newline=newline)

    monkeypatch.setattr(Path, "read_text", tracked_read_text)
    monkeypatch.setattr(Path, "write_text", tracked_write_text)
    stage = stage_hf(run, tmp_path / "hf-stage")

    assert "run `fixture`" in (stage / "README.md").read_text(encoding="utf-8")
    assert (stage / "CITATION.cff").read_text(encoding="utf-8") == (
        Path(__file__).parents[2] / "CITATION.cff"
    ).read_text(encoding="utf-8")
    assert (stage / "LICENSE").read_text(encoding="utf-8") == (
        Path(__file__).parents[2] / "LICENSE"
    ).read_text(encoding="utf-8")
    assert read_encodings[:2] == ["utf-8", "utf-8"]
    assert ("README.md", "utf-8") in write_encodings
    assert ("ATTRIBUTION.md", "utf-8") in write_encodings


def test_hf_manifest_check_uses_exact_path_components() -> None:
    components: list[str] = []

    class FakePath:
        def __truediv__(self, component: str) -> "FakePath":
            components.append(component)
            return self

        def is_file(self) -> bool:
            return True

        def read_text(self, *, encoding: str) -> str:
            assert encoding == "utf-8"
            return '{"status": "complete"}'

    _check_completed_run(cast(Path, FakePath()))

    assert components == ["manifests", "manifest.json"]


def test_hf_staging_file_helper_forwards_exact_descriptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Path, str | None]] = []

    def fake_files(directory: Path, description: str) -> tuple[Path, ...]:
        calls.append((directory, description))
        return (directory / "fixture.parquet",)

    monkeypatch.setattr(hf_module, "_parquet_files", fake_files)
    result = _staging_files(Path("run"))

    assert result == (
        (Path("run/coverage/global/fixture.parquet"),),
        (Path("run/summaries/fixture.parquet"),),
    )
    assert calls == [
        (Path("run/coverage/global"), "global coverage"),
        (Path("run/summaries"), "summary"),
    ]


def test_hf_copy_helpers_forward_exact_paths_and_directory_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    global_file = tmp_path / "global.parquet"
    summary_file = tmp_path / "summary.parquet"
    copy_calls: list[tuple[Path, Path]] = []
    mkdir_calls: list[tuple[Path, tuple[object, ...], dict[str, object]]] = []

    def fake_copy(source: Path, destination: Path) -> None:
        copy_calls.append((source, destination))

    def fake_mkdir(path: Path, *args: object, **kwargs: object) -> None:
        mkdir_calls.append((path, args, kwargs))

    monkeypatch.setattr(hf_module.shutil, "copy2", fake_copy)
    monkeypatch.setattr(Path, "mkdir", fake_mkdir)
    destination = tmp_path / "stage"

    _copy_staging_files(destination, (global_file,), (summary_file,))

    assert mkdir_calls == [
        (
            destination / "data/coverage/global",
            (),
            {"parents": True, "exist_ok": True},
        ),
        (destination / "data/summaries", (), {"parents": True, "exist_ok": True}),
    ]
    assert copy_calls == [
        (global_file, destination / "data/coverage/global/global.parquet"),
        (summary_file, destination / "data/summaries/summary.parquet"),
    ]


def test_hf_summary_json_copy_uses_exact_source_and_destination_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = tmp_path / "run"
    summary_json = run / "reports" / "summary.json"
    summary_json.parent.mkdir(parents=True)
    summary_json.write_text("{}", encoding="utf-8")
    calls: list[tuple[Path, Path]] = []
    monkeypatch.setattr(
        hf_module.shutil, "copy2", lambda source, target: calls.append((source, target))
    )

    _copy_summary_json(run, tmp_path / "stage")

    assert calls == [(summary_json, tmp_path / "stage/data/summary.json")]


def test_stage_hf_forwards_exact_manifest_and_repository_file_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = tmp_path / "run"
    manifest = run / "manifests" / "manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text('{"status": "complete", "run_id": "fixture"}', encoding="utf-8")
    destination = tmp_path / "stage"
    calls: list[tuple[str, object]] = []
    repository_names: list[str] = []
    copy_calls: list[tuple[Path, Path]] = []
    read_paths: list[Path] = []
    write_calls: list[tuple[Path, str, str | None]] = []
    original_read_text = Path.read_text

    def fake_check(run_root: Path) -> None:
        calls.append(("check", run_root))

    def fake_prepare(target: Path) -> None:
        calls.append(("prepare", target))

    def fake_staging(run_root: Path) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
        calls.append(("staging", run_root))
        return (), ()

    def fake_validate(global_files: tuple[Path, ...], summary_files: tuple[Path, ...]) -> None:
        calls.append(("validate", (global_files, summary_files)))

    def fake_copy_files(
        target: Path, global_files: tuple[Path, ...], summary_files: tuple[Path, ...]
    ) -> None:
        calls.append(("copy-files", (target, global_files, summary_files)))

    def fake_copy_summary(run_root: Path, target: Path) -> None:
        calls.append(("copy-summary", (run_root, target)))

    def fake_repository_file(name: str) -> Path:
        repository_names.append(name)
        return tmp_path / "repository" / name

    def fake_copy(source: Path, target: Path) -> None:
        copy_calls.append((source, target))

    def tracked_read_text(
        path: Path, encoding: str | None = None, errors: str | None = None
    ) -> str:
        read_paths.append(path)
        return original_read_text(path, encoding=encoding, errors=errors)

    def tracked_write_text(
        path: Path,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        write_calls.append((path, data, encoding))
        return len(data)

    monkeypatch.setattr(hf_module, "_check_completed_run", fake_check)
    monkeypatch.setattr(hf_module, "_prepare_destination", fake_prepare)
    monkeypatch.setattr(hf_module, "_staging_files", fake_staging)
    monkeypatch.setattr(hf_module, "_validate_staging_files", fake_validate)
    monkeypatch.setattr(hf_module, "_copy_staging_files", fake_copy_files)
    monkeypatch.setattr(hf_module, "_copy_summary_json", fake_copy_summary)
    monkeypatch.setattr(hf_module, "_repository_file", fake_repository_file)
    monkeypatch.setattr(hf_module.shutil, "copy2", fake_copy)
    monkeypatch.setattr(Path, "read_text", tracked_read_text)
    monkeypatch.setattr(Path, "write_text", tracked_write_text)

    result = stage_hf(run, destination)
    absolute_run = run.absolute()
    absolute_destination = destination.absolute()

    assert result == absolute_destination
    assert calls == [
        ("check", absolute_run),
        ("prepare", absolute_destination),
        ("staging", absolute_run),
        ("validate", ((), ())),
        ("copy-files", (absolute_destination, (), ())),
        ("copy-summary", (absolute_run, absolute_destination)),
    ]
    assert read_paths == [absolute_run / "manifests/manifest.json"]
    assert repository_names == ["CITATION.cff", "LICENSE"]
    assert copy_calls == [
        (tmp_path / "repository/CITATION.cff", absolute_destination / "CITATION.cff"),
        (tmp_path / "repository/LICENSE", absolute_destination / "LICENSE"),
    ]
    assert [path for path, _, _ in write_calls] == [
        absolute_destination / "README.md",
        absolute_destination / "ATTRIBUTION.md",
    ]
    assert all(encoding == "utf-8" for _, _, encoding in write_calls)
