from pathlib import Path

import pytest

import osm_polygon_wikidata_website_coverage.config.paths as paths_module
from osm_polygon_wikidata_website_coverage.config.paths import (
    DEFAULT_DATA_ROOT,
    DataPaths,
    _absolute,
    _overlaps,
    _validate_run_id,
)


def test_paths_default_to_the_seagate_project_root() -> None:
    paths = DataPaths.from_values()

    assert paths.data_root == DEFAULT_DATA_ROOT
    assert paths.raw_pbf_root.name == "raw"
    assert paths.source_paths == (
        paths.raw_pbf_root,
        paths.wikidata_root,
        paths.website_root,
    )


def test_absolute_path_resolution_is_physical_and_non_strict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[bool] = []

    class FakePath:
        def resolve(self, *, strict: bool) -> Path:
            calls.append(strict)
            return Path("/physical/path")

    monkeypatch.setattr(paths_module, "Path", lambda value: FakePath())

    assert _absolute("logical/path") == Path("/physical/path")
    assert calls == [False]


def test_paths_reject_an_output_root_outside_seagate(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Seagate"):
        DataPaths.from_values(data_root=tmp_path)


def test_source_paths_are_read_only_and_must_be_directories(tmp_path: Path) -> None:
    source = tmp_path / "raw"
    source.mkdir()

    paths = DataPaths.from_values(
        data_root=DEFAULT_DATA_ROOT,
        raw_pbf_root=source,
        wikidata_root=source,
        website_root=source,
    )

    assert paths.raw_pbf_root == source.resolve()
    assert paths.source_paths == (source.resolve(), source.resolve(), source.resolve())


def test_paths_reject_a_missing_source_directory(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()

    with pytest.raises(ValueError, match="not a directory"):
        DataPaths.from_values(
            raw_pbf_root=tmp_path / "missing",
            wikidata_root=source,
            website_root=source,
        )


def test_paths_reject_a_source_root_that_overlaps_output_root() -> None:
    with pytest.raises(ValueError, match="overlaps output"):
        DataPaths.from_values(
            data_root=Path("/Volumes/Seagate M3/projects"),
            raw_pbf_root=Path("/Volumes/Seagate M3/projects"),
        )


def test_paths_reject_a_source_symlink_that_physically_overlaps_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    projects = tmp_path / "projects"
    output = projects / "output"
    output.mkdir(parents=True)
    source_link = tmp_path / "source-link"
    source_link.symlink_to(output, target_is_directory=True)
    monkeypatch.setattr(paths_module, "DEFAULT_PROJECTS_ROOT", projects)

    with pytest.raises(ValueError, match="overlaps output"):
        DataPaths.from_values(
            data_root=output,
            raw_pbf_root=source_link,
            wikidata_root=tmp_path / "wikidata",
            website_root=tmp_path / "website",
        )


def test_paths_reject_an_output_symlink_that_physically_escapes_projects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    projects = tmp_path / "projects"
    projects.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    output_link = projects / "output-link"
    output_link.symlink_to(outside, target_is_directory=True)
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setattr(paths_module, "DEFAULT_PROJECTS_ROOT", projects)

    with pytest.raises(ValueError, match="Seagate"):
        DataPaths.from_values(
            data_root=output_link,
            raw_pbf_root=source,
            wikidata_root=source,
            website_root=source,
        )


def test_overlap_detection_handles_each_nested_path_direction() -> None:
    root = Path("/Volumes/Seagate M3/projects/output")
    child = root / "source"

    assert _overlaps(root, child) is True
    assert _overlaps(child, root) is True
    assert _overlaps(Path("/tmp/first"), Path("/tmp/second")) is False


def test_run_root_stays_under_the_data_root(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    paths = DataPaths.from_values(
        data_root=DEFAULT_DATA_ROOT,
        raw_pbf_root=source,
        wikidata_root=source,
        website_root=source,
    )

    assert paths.run_root("fixture") == DEFAULT_DATA_ROOT / "runs" / "fixture"


def test_run_root_rejects_a_symlinked_runs_directory_that_escapes_output(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (data_root / "runs").symlink_to(outside, target_is_directory=True)
    source = tmp_path / "source"
    source.mkdir()
    paths = DataPaths(data_root, source, source, source)

    with pytest.raises(ValueError, match="escapes data root"):
        paths.run_root("fixture")


@pytest.mark.parametrize("run_id", ["", ".", "..", "../outside", "nested/run"])
def test_run_root_rejects_unsafe_run_ids(tmp_path: Path, run_id: str) -> None:
    source = tmp_path / "source"
    source.mkdir()
    paths = DataPaths.from_values(
        data_root=DEFAULT_DATA_ROOT,
        raw_pbf_root=source,
        wikidata_root=source,
        website_root=source,
    )

    with pytest.raises(ValueError, match="run ID"):
        paths.run_root(run_id)


@pytest.mark.parametrize("run_id", [".", ".."])
def test_validate_run_id_rejects_dot_aliases_with_the_exact_error(run_id: str) -> None:
    with pytest.raises(ValueError) as raised:
        _validate_run_id(run_id)

    assert str(raised.value) == f"unsafe run ID: {run_id!r}"


def test_validate_run_id_rejects_falsey_nonempty_values_and_accepts_dotted_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FalseyString(str):
        def __bool__(self) -> bool:
            return False

    class FakePath:
        parts = ("valid",)
        name = "valid"

        def is_absolute(self) -> bool:
            return False

    monkeypatch.setattr(paths_module, "Path", lambda value: FakePath())
    with pytest.raises(ValueError, match=r"unsafe run ID: 'valid'"):
        _validate_run_id(FalseyString("valid"))

    monkeypatch.undo()
    _validate_run_id("XX.XX")
