from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest
from tests.support import write_rows, write_wikidata_tree

import osm_polygon_wikidata_website_coverage.sources._files as files_module
import osm_polygon_wikidata_website_coverage.sources.website as website_module
import osm_polygon_wikidata_website_coverage.sources.wikimedia as wikimedia_module
from osm_polygon_wikidata_website_coverage.sources._files import (
    SourceDatasetError,
    file_inventory,
    parquet_files,
    read_column_names,
    validate_columns,
)
from osm_polygon_wikidata_website_coverage.sources.website import validate_website_source
from osm_polygon_wikidata_website_coverage.sources.wikimedia import validate_wikidata_source


def test_website_validation_rejects_missing_directory_and_columns(tmp_path: Path) -> None:
    with pytest.raises(SourceDatasetError, match="directory is missing"):
        validate_website_source(tmp_path / "missing")

    root = tmp_path / "website"
    (root / "polygons").mkdir(parents=True)
    write_rows(root / "polygons" / "bad.parquet", [{"osm_id": 1}])
    with pytest.raises(SourceDatasetError, match="missing columns"):
        validate_website_source(root)


def test_wikidata_validation_rejects_missing_project_and_columns(tmp_path: Path) -> None:
    root = tmp_path / "wikidata"
    (root / "polygon_document_links").mkdir(parents=True)
    (root / "wikipedia" / "documents").mkdir(parents=True)
    (root / "wikivoyage" / "documents").mkdir(parents=True)
    write_rows(root / "polygon_document_links" / "bad.parquet", [{"project": "wikipedia"}])
    write_rows(root / "wikipedia" / "documents" / "bad.parquet", [{"project": "wikipedia"}])
    write_rows(root / "wikivoyage" / "documents" / "bad.parquet", [{"project": "wikivoyage"}])

    with pytest.raises(SourceDatasetError, match="link file"):
        validate_wikidata_source(root)


def test_wikidata_validation_rejects_missing_document_directory(tmp_path: Path) -> None:
    root = tmp_path / "wikidata"
    (root / "polygon_document_links").mkdir(parents=True)
    write_rows(
        root / "polygon_document_links" / "links.parquet",
        [{"project": "wikipedia", "document_id": "x", "osm_type": "way", "osm_id": 2}],
    )
    write_rows(
        root / "wikipedia" / "documents" / "documents.parquet",
        [{"project": "wikipedia", "document_id": "x", "fetch_status": "ok", "full_text": "x"}],
    )
    with pytest.raises(SourceDatasetError, match="wikivoyage document directory"):
        validate_wikidata_source(root)


def test_source_file_inventory_rejects_empty_and_escaping_symlink(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    with pytest.raises(SourceDatasetError, match="contains no Parquet"):
        parquet_files(tmp_path / "empty", "files", "file")

    root = tmp_path / "files"
    root.mkdir()
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"x")
    (root / "outside.parquet").symlink_to(outside)
    with pytest.raises(SourceDatasetError, match="escapes"):
        parquet_files(root, "files", "file")


def test_source_file_inventory_handles_resolution_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class BrokenPath:
        def resolve(self) -> Path:
            raise OSError("resolve failed")

    assert files_module._regular_file_under(cast(Path, BrokenPath()), tmp_path) is False


def test_shared_source_validator_reports_missing_columns(tmp_path: Path) -> None:
    path = tmp_path / "source.parquet"
    write_rows(path, [{"present": 1}])

    with pytest.raises(SourceDatasetError, match="source file .* missing columns: required"):
        validate_columns((path,), frozenset({"required"}), "source")


def test_source_adapters_return_sorted_files_and_validate_complete_trees(tmp_path: Path) -> None:
    website = tmp_path / "website"
    (website / "polygons").mkdir(parents=True)
    write_rows(
        website / "polygons" / "b.parquet",
        [
            {
                "osm_type": "way",
                "osm_id": 2,
                "website_text_status": "success",
                "website_text": "text",
                "contact_website_text_status": "absent",
                "contact_website_text": None,
            }
        ],
    )
    write_rows(
        website / "polygons" / "a.parquet",
        [
            {
                "osm_type": "relation",
                "osm_id": 1,
                "website_text_status": "success",
                "website_text": "text",
                "contact_website_text_status": "absent",
                "contact_website_text": None,
            }
        ],
    )
    website_files = website_module.website_parquet_files(website)
    assert website_files == tuple(sorted(website_files))
    assert validate_website_source(website) == website_files

    wikidata = tmp_path / "wikidata"
    write_wikidata_tree(wikidata)
    link_files = wikimedia_module.wikimedia_link_files(wikidata)
    wikipedia_files = wikimedia_module.wikimedia_document_files(wikidata, "wikipedia")
    wikivoyage_files = wikimedia_module.wikimedia_document_files(wikidata, "wikivoyage")
    assert link_files == tuple(sorted(link_files))
    assert wikipedia_files == tuple(sorted(wikipedia_files))
    assert wikivoyage_files == tuple(sorted(wikivoyage_files))
    assert validate_wikidata_source(wikidata) == (link_files, wikipedia_files + wikivoyage_files)


def test_shared_source_validator_accepts_a_complete_schema(tmp_path: Path) -> None:
    path = tmp_path / "source.parquet"
    write_rows(path, [{"required": 1, "also_required": "yes"}])

    assert read_column_names(path, "source") == {"required", "also_required"}
    validate_columns((path,), frozenset({"required", "also_required"}), "source")


def test_shared_file_inventory_records_relative_metadata_and_optional_label(
    tmp_path: Path,
) -> None:
    path = tmp_path / "nested" / "source.parquet"
    path.parent.mkdir()
    path.write_bytes(b"source")
    stat = path.stat()

    assert file_inventory(tmp_path, (path,), label="website") == [
        {
            "label": "website",
            "path": "nested/source.parquet",
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
    ]
    assert file_inventory(tmp_path, (path,)) == [
        {
            "path": "nested/source.parquet",
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
    ]


def test_source_schema_readers_wrap_arrow_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    website_file = tmp_path / "website.parquet"
    wikimedia_file = tmp_path / "wikimedia.parquet"

    def fail(path: Path) -> object:
        del path
        raise pa.ArrowInvalid("bad schema")

    monkeypatch.setattr(files_module.pq, "read_schema", fail)
    with pytest.raises(SourceDatasetError, match="website file schema"):
        read_column_names(website_file, "website")

    monkeypatch.setattr(files_module.pq, "read_schema", fail)
    with pytest.raises(SourceDatasetError, match="Wikimedia file schema"):
        read_column_names(wikimedia_file, "Wikimedia")


def test_wikimedia_file_inventory_rejects_unknown_project(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="wikipedia or wikivoyage"):
        wikimedia_module.wikimedia_document_files(tmp_path, "unknown")


def test_source_reader_queries_have_read_only_paths_and_required_contracts() -> None:
    assert "read_parquet" in website_module.WEBSITE_SUCCESS_SQL
    assert "read_parquet" in wikimedia_module.WIKIDATA_SUCCESS_SQL
    assert "fetch_status = 'ok'" in wikimedia_module.WIKIDATA_SUCCESS_SQL
    assert "full_text" in wikimedia_module.WIKIDATA_SUCCESS_SQL
