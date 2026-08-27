from pathlib import Path
from typing import cast

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.sources.website as website_module
from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.sources._duckdb import read_only_connection
from osm_polygon_wikidata_website_coverage.sources.website import (
    WEBSITE_SUCCESS_SQL,
    SourceDatasetError,
    _column_names,
    read_successful_website_keys,
    validate_website_source,
    website_parquet_files,
    website_success_parameters,
)


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_website_success_requires_success_status_and_nonempty_text(tmp_path: Path) -> None:
    root = tmp_path / "website"
    _write_rows(
        root / "polygons" / "fixture.parquet",
        [
            {
                "osm_type": "way",
                "osm_id": 1,
                "website_text_status": "success",
                "website_text": "About",
                "contact_website_text_status": "absent",
                "contact_website_text": None,
            },
            {
                "osm_type": "way",
                "osm_id": 2,
                "website_text_status": "empty",
                "website_text": "",
                "contact_website_text_status": "success",
                "contact_website_text": "Contact",
            },
            {
                "osm_type": "way",
                "osm_id": 3,
                "website_text_status": "success",
                "website_text": "   ",
                "contact_website_text_status": "absent",
                "contact_website_text": None,
            },
        ],
    )

    assert read_successful_website_keys(root) == {
        OsmIdentity("way", 1),
        OsmIdentity("way", 2),
    }


def test_website_reader_rejects_missing_required_columns(tmp_path: Path) -> None:
    root = tmp_path / "website"
    _write_rows(root / "polygons" / "fixture.parquet", [{"osm_id": 1}])

    with pytest.raises(SourceDatasetError, match="website_text_status"):
        read_successful_website_keys(root)


def test_website_reader_rejects_missing_or_empty_polygon_directories(tmp_path: Path) -> None:
    with pytest.raises(SourceDatasetError, match="directory is missing"):
        read_successful_website_keys(tmp_path / "missing")

    root = tmp_path / "website"
    (root / "polygons").mkdir(parents=True)
    with pytest.raises(SourceDatasetError, match="no Parquet files"):
        read_successful_website_keys(root)


def test_website_paths_and_schema_contract_are_exact(tmp_path: Path) -> None:
    root = tmp_path / "website"
    first = root / "polygons" / "a.parquet"
    second = root / "polygons" / "b.parquet"
    _write_rows(second, [{"osm_id": 2}])
    _write_rows(first, [{"osm_id": 1}])

    assert website_parquet_files(root) == (first, second)
    assert website_success_parameters(root) == [str(root / "polygons" / "*.parquet")]

    with pytest.raises(SourceDatasetError) as raised, read_only_connection() as connection:
        validate_website_source(root, connection)
    assert str(raised.value) == (
        f"website file {first} is missing columns: "
        "contact_website_text, contact_website_text_status, osm_type, "
        "website_text, website_text_status"
    )


def test_website_column_reader_uses_the_read_only_schema_query(tmp_path: Path) -> None:
    class Result:
        def fetchall(self) -> list[tuple[str]]:
            return [("osm_id",)]

    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[str]]] = []

        def execute(self, query: str, parameters: list[str]) -> Result:
            self.calls.append((query, parameters))
            return Result()

    connection = Connection()
    assert _column_names(
        cast(duckdb.DuckDBPyConnection, connection), tmp_path / "fixture.parquet"
    ) == {"osm_id"}
    assert connection.calls == [
        ("DESCRIBE SELECT * FROM read_parquet(?)", [str(tmp_path / "fixture.parquet")])
    ]


def test_read_successful_website_keys_uses_the_exact_polygon_glob(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "website"
    calls: list[tuple[str, list[str]]] = []

    class Result:
        def fetchall(self) -> list[tuple[str, int]]:
            return [("way", 1), ("relation", 2)]

    class Connection:
        def execute(self, query: str, parameters: list[str]) -> Result:
            calls.append((query, parameters))
            return Result()

    class Context:
        def __enter__(self) -> Connection:
            return Connection()

        def __exit__(self, *args: object) -> None:
            pass

    def fake_validate(source_root: Path, connection: object) -> tuple[Path, ...]:
        assert source_root == root
        return ()

    monkeypatch.setattr(website_module, "read_only_connection", lambda: Context())
    monkeypatch.setattr(website_module, "validate_website_source", fake_validate)

    assert website_module.read_successful_website_keys(root) == {
        OsmIdentity("way", 1),
        OsmIdentity("relation", 2),
    }
    assert calls == [(WEBSITE_SUCCESS_SQL, [str(root / "polygons" / "*.parquet")])]
