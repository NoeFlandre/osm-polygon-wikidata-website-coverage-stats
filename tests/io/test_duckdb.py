from pathlib import Path
from typing import cast

import duckdb
import pyarrow.parquet as pq

import osm_polygon_wikidata_website_coverage.io.duckdb as duckdb_module


def test_configure_connection_sets_bounded_runtime_and_reuses_spill_directory(
    tmp_path: Path,
) -> None:
    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, object | None]] = []

        def execute(self, query: str, parameters: object | None = None) -> None:
            self.calls.append((query, parameters))

    connection = Connection()
    output_root = tmp_path / "run's"
    duckdb_module.configure_connection(cast(duckdb.DuckDBPyConnection, connection), output_root)
    duckdb_module.configure_connection(cast(duckdb.DuckDBPyConnection, connection), output_root)

    spill = output_root / "scratch" / "duckdb-temp"
    destination = str(spill).replace("'", "''")
    expected = [
        ("SET memory_limit = '3GB'", None),
        (f"SET temp_directory = '{destination}'", None),
        ("SET threads = 4", None),
        ("SET preserve_insertion_order = false", None),
    ]
    assert spill.is_dir()
    assert connection.calls == expected * 2


def test_export_query_escapes_apostrophes_and_promotes_parquet_atomically(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(database=":memory:")
    try:
        output = tmp_path / "run's" / "result.parquet"
        duckdb_module.export_query(
            connection,
            "SELECT ?::INTEGER AS value",
            ["7"],
            output,
        )
    finally:
        connection.close()

    assert pq.read_table(output).to_pylist() == [{"value": 7}]
