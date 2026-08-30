from pathlib import Path

import pyarrow as pa
import pytest
from tests.support import write_rows

import osm_polygon_wikidata_website_coverage.sources._files as files_module
import osm_polygon_wikidata_website_coverage.sources.website as website_module
import osm_polygon_wikidata_website_coverage.sources.wikimedia as wikimedia_module
from osm_polygon_wikidata_website_coverage.sources._files import SourceDatasetError, parquet_files
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

    assert files_module._regular_file_under(BrokenPath(), tmp_path) is False  # type: ignore[arg-type]


def test_source_schema_readers_wrap_arrow_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    website_file = tmp_path / "website.parquet"
    wikimedia_file = tmp_path / "wikimedia.parquet"

    def fail(path: Path) -> object:
        del path
        raise pa.ArrowInvalid("bad schema")

    monkeypatch.setattr(website_module.pq, "read_schema", fail)
    with pytest.raises(SourceDatasetError, match="website file schema"):
        website_module._column_names(website_file)

    monkeypatch.setattr(wikimedia_module.pq, "read_schema", fail)
    with pytest.raises(SourceDatasetError, match="Wikimedia file schema"):
        wikimedia_module._column_names(wikimedia_file)


def test_wikimedia_file_inventory_rejects_unknown_project(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="wikipedia or wikivoyage"):
        wikimedia_module.wikimedia_document_files(tmp_path, "unknown")


def test_source_reader_queries_have_read_only_paths_and_required_contracts() -> None:
    assert "read_parquet" in website_module.WEBSITE_SUCCESS_SQL
    assert "read_parquet" in wikimedia_module.WIKIDATA_SUCCESS_SQL
    assert "fetch_status = 'ok'" in wikimedia_module.WIKIDATA_SUCCESS_SQL
    assert "full_text" in wikimedia_module.WIKIDATA_SUCCESS_SQL
