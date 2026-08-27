from pathlib import Path

import pytest

from osm_polygon_wikidata_website_coverage.config.paths import (
    DEFAULT_DATA_ROOT,
    DataPaths,
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
