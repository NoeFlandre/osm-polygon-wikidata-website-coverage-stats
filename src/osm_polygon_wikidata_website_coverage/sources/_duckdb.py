"""Temporary read-only DuckDB connection for external source files."""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb


@contextmanager
def read_only_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """Yield a read-only connection backed by a disposable empty catalog."""

    with tempfile.TemporaryDirectory(prefix="osm-polygon-coverage-") as directory:
        catalog = Path(directory) / "catalog.duckdb"
        initializer = duckdb.connect(database=str(catalog))
        initializer.close()
        connection = duckdb.connect(database=str(catalog), read_only=True)
        try:
            yield connection
        finally:
            connection.close()
