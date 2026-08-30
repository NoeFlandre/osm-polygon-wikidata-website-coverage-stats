from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from osm_polygon_wikidata_website_coverage.io.atomic import atomic_path
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


def test_export_query_writes_a_parquet_result(tmp_path: Path) -> None:
    connection = duckdb.connect(database=":memory:")
    try:
        output = tmp_path / "result.parquet"
        export_query(connection, "SELECT 7 AS value", [], output)
    finally:
        connection.close()

    assert pq.read_table(output).to_pylist() == [{"value": 7}]
