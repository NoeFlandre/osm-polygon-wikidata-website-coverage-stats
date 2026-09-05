import json
from pathlib import Path
from typing import NoReturn

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tests.support import write_source_tree, write_wikidata_tree

import osm_polygon_wikidata_website_coverage.pipeline.join as join_module
from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.io.parquet import MEMBERSHIP_SCHEMA
from osm_polygon_wikidata_website_coverage.pipeline.join import load_memberships


def test_load_memberships_materializes_only_website_and_union_wikidata(
    tmp_path: Path,
) -> None:
    website = tmp_path / "website"
    wikidata = tmp_path / "wikidata"
    write_source_tree(website)
    write_wikidata_tree(wikidata)
    run_root = tmp_path / "run"

    result = load_memberships(
        DataPaths(tmp_path / "data", tmp_path / "raw", wikidata, website), run_root
    )

    assert [path.name for path in result.paths] == ["website.parquet", "wikidata.parquet"]
    assert sorted(
        pq.read_table(result.paths[0]).to_pylist(), key=lambda row: (row["osm_type"], row["osm_id"])
    ) == [
        {"osm_type": "relation", "osm_id": 2},
        {"osm_type": "way", "osm_id": 1},
    ]
    assert sorted(
        pq.read_table(result.paths[1]).to_pylist(), key=lambda row: (row["osm_type"], row["osm_id"])
    ) == [
        {"osm_type": "relation", "osm_id": 2},
        {"osm_type": "way", "osm_id": 1},
    ]
    assert not (run_root / "members" / "wikipedia.parquet").exists()
    assert not (run_root / "scratch").exists()
    assert result.manifest_path == run_root / "members" / "manifest.json"


def test_source_inventory_preserves_each_source_label_and_relative_path(tmp_path: Path) -> None:
    website = tmp_path / "website"
    wikidata = tmp_path / "wikidata"
    write_source_tree(website)
    write_wikidata_tree(wikidata)

    inventory = join_module._source_inventory(
        DataPaths(tmp_path / "data", tmp_path / "raw", wikidata, website)
    )

    assert [(record["label"], record["path"]) for record in inventory] == [
        ("website", "polygons/website.parquet"),
        ("wikimedia-links", "polygon_document_links/links.parquet"),
        ("wikimedia-documents", "wikipedia/documents/documents.parquet"),
        ("wikimedia-documents", "wikivoyage/documents/documents.parquet"),
    ]


def test_load_memberships_reuses_matching_stage_without_source_queries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    website = tmp_path / "website"
    wikidata = tmp_path / "wikidata"
    write_source_tree(website)
    write_wikidata_tree(wikidata)
    paths = DataPaths(tmp_path / "data", tmp_path / "raw", wikidata, website)
    run_root = tmp_path / "run"
    first = load_memberships(paths, run_root)
    manifest = json.loads((run_root / "members" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "1"

    def fail_if_rescanned(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("rescanned")

    monkeypatch.setattr(join_module, "export_query", fail_if_rescanned)
    second = load_memberships(paths, run_root, resume=True)

    assert second.paths == first.paths
    assert second.manifest_path == first.manifest_path == run_root / "members" / "manifest.json"
    outputs = join_module._output_paths(run_root)
    inventory = join_module._source_inventory(paths)
    assert join_module._stage_is_reusable(run_root, outputs, inventory) is True
    changed_inventory = [*inventory]
    changed_inventory[0] = {**changed_inventory[0], "path": "changed.parquet"}
    assert join_module._stage_is_reusable(run_root, outputs, changed_inventory) is False


def test_load_memberships_rebuilds_when_stage_manifest_or_output_is_invalid(
    tmp_path: Path,
) -> None:
    website = tmp_path / "website"
    wikidata = tmp_path / "wikidata"
    write_source_tree(website)
    write_wikidata_tree(wikidata)
    paths = DataPaths(tmp_path / "data", tmp_path / "raw", wikidata, website)
    run_root = tmp_path / "run"
    first = load_memberships(paths, run_root)
    manifest_path = run_root / "members" / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")
    first.paths[0].unlink()

    result = load_memberships(paths, run_root, resume=True)

    assert result.paths == first.paths
    assert pq.read_table(result.paths[0]).num_rows == 2


def test_join_manifest_records_the_exact_output_contract(tmp_path: Path) -> None:
    path = tmp_path / "run" / "members" / "manifest.json"
    source_inventory = [{"label": "website", "path": "website.parquet"}]

    join_module._write_manifest(path, source_inventory)

    assert json.loads(path.read_text(encoding="utf-8")) == {
        "schema_version": "1",
        "source_inventory": source_inventory,
        "outputs": ["website.parquet", "wikidata.parquet"],
    }


def test_load_memberships_rejects_missing_source_roots(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    with pytest.raises(ValueError, match="website polygons"):
        load_memberships(
            DataPaths(tmp_path / "data", tmp_path / "raw", source, source), tmp_path / "run"
        )


def test_join_output_validation_rejects_missing_corrupt_and_wrong_schema_files(
    tmp_path: Path,
) -> None:
    assert join_module._output_is_valid(tmp_path / "missing.parquet") is False

    corrupt = tmp_path / "corrupt.parquet"
    corrupt.write_bytes(b"not parquet")
    assert join_module._output_is_valid(corrupt) is False

    wrong = tmp_path / "wrong.parquet"
    pq.write_table(pa.table({"wrong": [1]}), wrong)
    assert join_module._output_is_valid(wrong) is False

    valid = tmp_path / "valid.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=MEMBERSHIP_SCHEMA), valid)
    assert join_module._output_is_valid(valid) is True


def test_join_spill_cleanup_leaves_unrelated_scratch_files_alone(tmp_path: Path) -> None:
    scratch = tmp_path / "scratch"
    (scratch / "keep").mkdir(parents=True)
    (scratch / "duckdb-temp").mkdir()
    join_module._cleanup_spill(tmp_path)
    assert (scratch / "keep").is_dir()
    assert not (scratch / "duckdb-temp").exists()
    join_module._cleanup_spill(tmp_path / "missing")


def test_join_spill_cleanup_targets_the_exact_directory_and_ignores_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[Path, bool]] = []

    def fake_rmtree(path: str | Path, *, ignore_errors: bool = False) -> None:
        calls.append((Path(path), ignore_errors))

    monkeypatch.setattr(join_module.shutil, "rmtree", fake_rmtree)
    run_root = tmp_path / "run"

    join_module._cleanup_spill(run_root)

    assert calls == [(run_root / "scratch" / "duckdb-temp", True)]


def test_load_memberships_default_rebuilds_an_existing_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    website = tmp_path / "website"
    wikidata = tmp_path / "wikidata"
    write_source_tree(website)
    write_wikidata_tree(wikidata)
    paths = DataPaths(tmp_path / "data", tmp_path / "raw", wikidata, website)
    run_root = tmp_path / "run"
    load_memberships(paths, run_root)
    calls: list[tuple[object, ...]] = []

    def record_query(*args: object, **kwargs: object) -> None:
        calls.append((*args, *kwargs.values()))

    monkeypatch.setattr(join_module, "export_query", record_query)

    result = load_memberships(paths, run_root)

    assert result.manifest_path == run_root / "members" / "manifest.json"
    assert len(calls) == 2


def test_load_memberships_uses_an_in_memory_duckdb_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    website = tmp_path / "website"
    wikidata = tmp_path / "wikidata"
    write_source_tree(website)
    write_wikidata_tree(wikidata)
    paths = DataPaths(tmp_path / "data", tmp_path / "raw", wikidata, website)
    seen_databases: list[str] = []

    class Connection:
        def close(self) -> None:
            pass

    def connect(*, database: str) -> Connection:
        seen_databases.append(database)
        return Connection()

    monkeypatch.setattr(join_module.duckdb, "connect", connect)
    monkeypatch.setattr(join_module, "configure_connection", lambda connection, root: None)
    monkeypatch.setattr(join_module, "export_query", lambda *args, **kwargs: None)

    load_memberships(paths, tmp_path / "run")

    assert seen_databases == [":memory:"]
