"""Bounded DuckDB runtime configuration for Seagate-backed pipeline stages."""

from __future__ import annotations

from pathlib import Path

import duckdb

MEMORY_LIMIT = "3GB"
DUCKDB_THREADS = 4


def configure_connection(connection: duckdb.DuckDBPyConnection, output_root: Path) -> None:
    """Keep DuckDB memory bounded and put all spill files under the run root."""

    spill = output_root / "scratch" / "duckdb-temp"
    spill.mkdir(parents=True, exist_ok=True)
    destination = str(spill).replace("'", "''")
    connection.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    connection.execute(f"SET temp_directory = '{destination}'")
    connection.execute(f"SET threads = {DUCKDB_THREADS}")
    connection.execute("SET preserve_insertion_order = false")
