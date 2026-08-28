from pathlib import Path
from typing import cast

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.sources.wikimedia as wikimedia_module
from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.sources._duckdb import read_only_connection
from osm_polygon_wikidata_website_coverage.sources.wikimedia import (
    WIKIMEDIA_SUCCESS_SQL,
    SourceDatasetError,
    _column_names,
    read_successful_wikimedia_keys,
    validate_wikimedia_source,
    wikimedia_document_files,
    wikimedia_link_files,
    wikimedia_success_parameters,
)


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_wikimedia_success_is_project_specific_and_requires_full_text(tmp_path: Path) -> None:
    root = tmp_path / "processed_v2"
    _write_rows(
        root / "polygon_document_links" / "links.parquet",
        [
            {"project": "wikipedia", "document_id": "w1", "osm_type": "way", "osm_id": 20},
            {
                "project": "wikivoyage",
                "document_id": "v1",
                "osm_type": "relation",
                "osm_id": 23,
            },
            {"project": "wikipedia", "document_id": "w2", "osm_type": "way", "osm_id": 24},
            {
                "project": "wikipedia",
                "document_id": "w3",
                "osm_type": "relation",
                "osm_id": 27,
            },
            {"project": "wikipedia", "document_id": "w4", "osm_type": "way", "osm_id": 21},
            {
                "project": "wikipedia",
                "document_id": "w5",
                "osm_type": "relation",
                "osm_id": 26,
            },
        ],
    )
    _write_rows(
        root / "wikipedia" / "documents" / "documents.parquet",
        [
            {
                "project": "wikipedia",
                "document_id": "w1",
                "fetch_status": "ok",
                "full_text": "Article",
            },
            {"project": "wikipedia", "document_id": "w2", "fetch_status": "ok", "full_text": ""},
            {
                "project": "wikipedia",
                "document_id": "w3",
                "fetch_status": "error",
                "full_text": "Failed",
            },
            {
                "project": "wikipedia",
                "document_id": "w4",
                "fetch_status": "ok",
                "full_text": "Invalid way area ID",
            },
            {
                "project": "wikipedia",
                "document_id": "w5",
                "fetch_status": "ok",
                "full_text": "Invalid relation area ID",
            },
        ],
    )
    _write_rows(
        root / "wikivoyage" / "documents" / "documents.parquet",
        [
            {
                "project": "wikivoyage",
                "document_id": "v1",
                "fetch_status": "ok",
                "full_text": "Guide",
            }
        ],
    )

    assert read_successful_wikimedia_keys(root, project="wikipedia") == {OsmIdentity("way", 10)}
    assert read_successful_wikimedia_keys(root, project="wikivoyage") == {
        OsmIdentity("relation", 11)
    }


def test_wikimedia_reader_rejects_parquet_symlink_that_escapes_source_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "processed_v2"
    external = tmp_path / "external.parquet"
    external.write_bytes(b"outside")
    link_root = root / "polygon_document_links"
    link_root.mkdir(parents=True)
    (link_root / "external.parquet").symlink_to(external)

    with pytest.raises(SourceDatasetError) as error:
        wikimedia_link_files(root)
    assert (
        str(error.value)
        == f"Wikimedia link file escapes source root: {link_root / 'external.parquet'}"
    )


def test_wikimedia_reader_rejects_broken_parquet_symlink(tmp_path: Path) -> None:
    root = tmp_path / "processed_v2"
    link_root = root / "polygon_document_links"
    link_root.mkdir(parents=True)
    (link_root / "broken.parquet").symlink_to(tmp_path / "missing.parquet")

    with pytest.raises(SourceDatasetError) as error:
        wikimedia_link_files(root)
    assert (
        str(error.value)
        == f"Wikimedia link file escapes source root: {link_root / 'broken.parquet'}"
    )


def test_wikimedia_reader_rejects_unknown_project(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="wikipedia or wikivoyage"):
        read_successful_wikimedia_keys(tmp_path, project="commons")


def test_wikimedia_reader_rejects_missing_document_columns(tmp_path: Path) -> None:
    root = tmp_path / "processed_v2"
    _write_rows(
        root / "polygon_document_links" / "links.parquet",
        [{"project": "wikipedia", "document_id": "w1", "osm_type": "way", "osm_id": 10}],
    )
    _write_rows(root / "wikipedia" / "documents" / "documents.parquet", [{"document_id": "w1"}])

    with pytest.raises(SourceDatasetError, match="full_text"):
        read_successful_wikimedia_keys(root, project="wikipedia")


def test_wikimedia_reader_rejects_missing_or_empty_directories(tmp_path: Path) -> None:
    root = tmp_path / "processed_v2"
    with pytest.raises(SourceDatasetError, match="directory is missing"):
        wikimedia_link_files(root)

    (root / "polygon_document_links").mkdir(parents=True)
    with pytest.raises(SourceDatasetError, match="no Parquet files"):
        wikimedia_link_files(root)

    with pytest.raises(SourceDatasetError, match="directory is missing"):
        wikimedia_document_files(root, "wikipedia")

    (root / "wikipedia" / "documents").mkdir(parents=True)
    with pytest.raises(SourceDatasetError, match="no Parquet files"):
        wikimedia_document_files(root, "wikipedia")


def test_wikimedia_reader_rejects_missing_link_columns(tmp_path: Path) -> None:
    root = tmp_path / "processed_v2"
    _write_rows(root / "polygon_document_links" / "links.parquet", [{"document_id": "w1"}])
    _write_rows(
        root / "wikipedia" / "documents" / "documents.parquet",
        [{"project": "wikipedia", "document_id": "w1", "fetch_status": "ok", "full_text": "x"}],
    )

    with (
        read_only_connection() as connection,
        pytest.raises(SourceDatasetError, match="Wikimedia link file"),
    ):
        validate_wikimedia_source(root, "wikipedia", connection)


def test_wikimedia_paths_and_messages_are_exact(tmp_path: Path) -> None:
    root = tmp_path / "processed_v2"
    link = root / "polygon_document_links" / "links.parquet"
    document = root / "wikipedia" / "documents" / "documents.parquet"
    _write_rows(link, [{"project": "wikipedia"}])
    _write_rows(document, [{"project": "wikipedia"}])

    assert wikimedia_link_files(root) == (link,)
    assert wikimedia_document_files(root, "wikipedia") == (document,)
    assert wikimedia_success_parameters(root, "wikipedia") == [
        str(root / "polygon_document_links" / "*.parquet"),
        "wikipedia",
        str(root / "wikipedia" / "documents" / "*.parquet"),
        "wikipedia",
    ]
    assert wikimedia_success_parameters(root, "wikivoyage")[1::2] == [
        "wikivoyage",
        "wikivoyage",
    ]

    with pytest.raises(ValueError, match="^project must be wikipedia or wikivoyage$"):
        wikimedia_document_files(root, "commons")

    with pytest.raises(SourceDatasetError) as raised, read_only_connection() as connection:
        validate_wikimedia_source(root, "wikipedia", connection)
    assert str(raised.value) == (
        f"Wikimedia link file {link} is missing columns: document_id, osm_id, osm_type"
    )


def test_wikimedia_column_reader_uses_the_read_only_schema_query(tmp_path: Path) -> None:
    class Result:
        def fetchall(self) -> list[tuple[str]]:
            return [("document_id",)]

    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[str]]] = []

        def execute(self, query: str, parameters: list[str]) -> Result:
            self.calls.append((query, parameters))
            return Result()

    connection = Connection()
    assert _column_names(
        cast(duckdb.DuckDBPyConnection, connection), tmp_path / "fixture.parquet"
    ) == {"document_id"}
    assert connection.calls == [
        ("DESCRIBE SELECT * FROM read_parquet(?)", [str(tmp_path / "fixture.parquet")])
    ]


def test_wikimedia_file_helpers_keep_exact_descriptions(tmp_path: Path) -> None:
    root = tmp_path / "processed_v2"
    with pytest.raises(SourceDatasetError) as link_error:
        wikimedia_link_files(root)
    assert str(link_error.value) == (
        f"Wikimedia link directory is missing: {root / 'polygon_document_links'}"
    )

    with pytest.raises(SourceDatasetError) as document_error:
        wikimedia_document_files(root, "wikipedia")
    assert str(document_error.value) == (
        f"wikipedia document directory is missing: {root / 'wikipedia' / 'documents'}"
    )


def test_wikimedia_validation_formats_document_missing_columns_exactly(tmp_path: Path) -> None:
    root = tmp_path / "processed_v2"
    _write_rows(
        root / "polygon_document_links" / "links.parquet",
        [{"project": "wikipedia", "document_id": "w1", "osm_type": "way", "osm_id": 1}],
    )
    document = root / "wikipedia" / "documents" / "documents.parquet"
    _write_rows(document, [{"project": "wikipedia", "document_id": "w1"}])

    with read_only_connection() as connection, pytest.raises(SourceDatasetError) as error:
        validate_wikimedia_source(root, "wikipedia", connection)
    assert str(error.value) == (
        f"Wikimedia document file {document} is missing columns: fetch_status, full_text"
    )


def test_read_successful_wikimedia_keys_uses_exact_project_globs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "processed_v2"
    calls: list[tuple[str, list[str]]] = []

    class Result:
        def fetchall(self) -> list[tuple[str, int]]:
            return [("way", 5), ("relation", 6)]

    class Connection:
        def execute(self, query: str, parameters: list[str]) -> Result:
            calls.append((query, parameters))
            return Result()

    class Context:
        def __enter__(self) -> Connection:
            return Connection()

        def __exit__(self, *args: object) -> None:
            pass

    def fake_validate(
        source_root: Path, project: str, connection: object
    ) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
        assert source_root == root
        assert project == "wikipedia"
        return (), ()

    monkeypatch.setattr(wikimedia_module, "read_only_connection", lambda: Context())
    monkeypatch.setattr(wikimedia_module, "validate_wikimedia_source", fake_validate)

    assert wikimedia_module.read_successful_wikimedia_keys(root, project="wikipedia") == {
        OsmIdentity("way", 5),
        OsmIdentity("relation", 6),
    }
    assert calls == [
        (
            WIKIMEDIA_SUCCESS_SQL,
            [
                str(root / "polygon_document_links" / "*.parquet"),
                "wikipedia",
                str(root / "wikipedia" / "documents" / "*.parquet"),
                "wikipedia",
            ],
        )
    ]
