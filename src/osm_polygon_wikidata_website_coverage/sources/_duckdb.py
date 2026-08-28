"""Temporary read-only DuckDB connection for external source files."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import duckdb


@contextmanager
def read_only_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """Yield an in-memory connection that cannot modify source files."""

    connection = duckdb.connect(database=":memory:")
    try:
        yield connection
    finally:
        connection.close()
