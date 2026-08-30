"""Bounded, atomic Parquet output for the overlap pipeline."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity

IDENTITY_SCHEMA = pa.schema([pa.field("osm_type", pa.string()), pa.field("osm_id", pa.int64())])
MEMBERSHIP_SCHEMA = IDENTITY_SCHEMA
OVERLAP_SCHEMA = pa.schema(
    [
        pa.field("osm_type", pa.string()),
        pa.field("osm_id", pa.int64()),
        pa.field("website", pa.bool_()),
        pa.field("wikidata", pa.bool_()),
        pa.field("overlap_category", pa.string()),
    ]
)
SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("overlap_category", pa.string()),
        pa.field("count", pa.int64()),
        pa.field("percentage", pa.float64()),
    ]
)


class IdentityParquetWriter:
    """Write one atomic identity Parquet with bounded row groups."""

    def __init__(
        self,
        directory: Path,
        *,
        filename: str,
        batch_rows: int = 100_000,
    ) -> None:
        if batch_rows < 1:
            raise ValueError("batch_rows must be positive")
        if not filename or Path(filename).name != filename or not filename.endswith(".parquet"):
            raise ValueError("filename must be a Parquet filename")
        self._directory = directory
        self._final = directory / filename
        self._temporary = directory / f".{filename}.tmp"
        self._batch_rows = batch_rows
        self._osm_types: list[str] = []
        self._osm_ids: list[int] = []
        self._writer: pq.ParquetWriter | None = None
        self._closed = False
        self._directory.mkdir(parents=True, exist_ok=True)
        self._refuse_overwrite()

    def write(self, identity: OsmIdentity) -> None:
        if self._closed:
            raise RuntimeError("cannot write to a closed Parquet writer")
        self._osm_types.append(identity.osm_type)
        self._osm_ids.append(identity.osm_id)
        if len(self._osm_ids) >= self._batch_rows:
            self._flush()

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._flush()
            self._open_writer()
            assert self._writer is not None
            self._writer.close()
            self._writer = None
            os.replace(self._temporary, self._final)
        except BaseException:
            self._close_writer()
            raise
        self._closed = True

    def abort(self) -> None:
        if self._closed:
            return
        self._close_writer()
        self._osm_types.clear()
        self._osm_ids.clear()
        self._temporary.unlink(missing_ok=True)
        self._closed = True

    def __enter__(self) -> IdentityParquetWriter:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        del exc_value, traceback
        if exc_type is None:
            self.close()
        else:
            self.abort()

    def _refuse_overwrite(self) -> None:
        if self._final.exists() or self._temporary.exists():
            raise FileExistsError(f"refusing to overwrite Parquet output: {self._final}")

    def _open_writer(self) -> None:
        if self._writer is not None:
            return
        self._refuse_overwrite()
        self._writer = pq.ParquetWriter(self._temporary, IDENTITY_SCHEMA, compression="zstd")

    def _flush(self) -> None:
        if not self._osm_ids:
            return
        self._open_writer()
        assert self._writer is not None
        self._writer.write_table(
            pa.Table.from_pydict(
                {"osm_type": self._osm_types, "osm_id": self._osm_ids},
                schema=IDENTITY_SCHEMA,
            )
        )
        self._osm_types.clear()
        self._osm_ids.clear()

    def _close_writer(self) -> None:
        if self._writer is None:
            return
        try:
            self._writer.close()
        finally:
            self._writer = None
