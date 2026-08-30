from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from osm_polygon_wikidata_website_coverage.io.atomic import (
    atomic_path,
    read_json_object,
    write_json,
)
from osm_polygon_wikidata_website_coverage.io.duckdb import export_query


def test_atomic_path_promotes_output_and_cleans_stale_temp(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "result.txt"
    temporary = output.with_name(".result.txt.tmp")
    temporary.parent.mkdir(parents=True)
    temporary.write_text("stale", encoding="utf-8")

    with atomic_path(output) as path:
        assert path == temporary
        path.write_text("fresh", encoding="utf-8")

    assert output.read_text(encoding="utf-8") == "fresh"
    assert not temporary.exists()


def test_atomic_path_keeps_old_output_when_producer_fails(tmp_path: Path) -> None:
    output = tmp_path / "result.txt"
    output.write_text("old", encoding="utf-8")

    with pytest.raises(RuntimeError, match="producer failed"), atomic_path(output) as path:
        path.write_text("partial", encoding="utf-8")
        raise RuntimeError("producer failed")

    assert output.read_text(encoding="utf-8") == "old"
    assert not output.with_name(".result.txt.tmp").exists()


def test_write_json_formats_and_promotes_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "nested" / "manifest.json"
    encodings: list[str | None] = []
    original_write_text = Path.write_text

    def record_encoding(
        path: Path,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        encodings.append(encoding)
        return original_write_text(path, data, encoding=encoding, errors=errors, newline=newline)

    monkeypatch.setattr(Path, "write_text", record_encoding)

    write_json(output, {"z": 1, "a": [True]})

    assert output.read_text(encoding="utf-8") == '{\n  "a": [\n    true\n  ],\n  "z": 1\n}\n'
    assert encodings == ["utf-8"]
    assert not output.with_name(".manifest.json.tmp").exists()


def test_read_json_object_returns_a_dictionary_for_valid_json(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text('{"status": "complete", "count": 2}', encoding="utf-8")

    assert read_json_object(path) == {"status": "complete", "count": 2}


def test_read_json_object_uses_utf8_encoding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "manifest.json"
    path.write_text('{"text": "café"}', encoding="utf-8")
    encodings: list[str | None] = []
    original_read_text = Path.read_text

    def record_encoding(path: Path, encoding: str | None = None, errors: str | None = None) -> str:
        encodings.append(encoding)
        return original_read_text(path, encoding=encoding, errors=errors)

    monkeypatch.setattr(Path, "read_text", record_encoding)

    assert read_json_object(path) == {"text": "café"}
    assert encodings == ["utf-8"]


@pytest.mark.parametrize("content", ["not json", "[]", "null"])
def test_read_json_object_returns_none_for_unusable_json(tmp_path: Path, content: str) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(content, encoding="utf-8")

    assert read_json_object(path) is None


def test_read_json_object_returns_none_for_a_missing_file(tmp_path: Path) -> None:
    assert read_json_object(tmp_path / "missing.json") is None


def test_export_query_writes_a_parquet_result(tmp_path: Path) -> None:
    connection = duckdb.connect(database=":memory:")
    try:
        output = tmp_path / "result.parquet"
        export_query(connection, "SELECT 7 AS value", [], output)
    finally:
        connection.close()

    assert pq.read_table(output).to_pylist() == [{"value": 7}]
