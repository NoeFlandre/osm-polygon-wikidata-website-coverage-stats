"""Bounded, schema-checked Parquet shard writers."""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.domain.identity import GeometryFailure, Occurrence

OCCURRENCE_SCHEMA = pa.schema(
    [
        pa.field("osm_type", pa.string()),
        pa.field("osm_id", pa.int64()),
        pa.field("source_pbf", pa.string()),
        pa.field("region", pa.string()),
        pa.field("osm_version", pa.int64()),
        pa.field("osm_timestamp", pa.string()),
        pa.field("relation_kind", pa.string()),
        pa.field("geometry_type", pa.string()),
        pa.field("geometry", pa.string()),
        pa.field("centroid_lon", pa.float64()),
        pa.field("centroid_lat", pa.float64()),
        pa.field("bbox_min_lon", pa.float64()),
        pa.field("bbox_min_lat", pa.float64()),
        pa.field("bbox_max_lon", pa.float64()),
        pa.field("bbox_max_lat", pa.float64()),
        pa.field("area_m2", pa.float64()),
        pa.field("area_bucket", pa.string()),
        pa.field("geometry_hash", pa.string()),
    ]
)

FAILURE_SCHEMA = pa.schema(
    [
        pa.field("osm_type", pa.string()),
        pa.field("osm_id", pa.int64()),
        pa.field("source_pbf", pa.string()),
        pa.field("candidate_kind", pa.string()),
        pa.field("failure_kind", pa.string()),
        pa.field("message", pa.string()),
    ]
)


def _timestamp_text(value: str | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return value


def _occurrence_row(value: Occurrence) -> dict[str, Any]:
    return {
        "osm_type": value.identity.osm_type,
        "osm_id": value.identity.osm_id,
        "source_pbf": value.source_pbf,
        "region": value.region,
        "osm_version": value.osm_version,
        "osm_timestamp": _timestamp_text(value.osm_timestamp),
        "relation_kind": value.relation_kind,
        "geometry_type": value.geometry_type,
        "geometry": value.geometry,
        "centroid_lon": value.centroid_lon,
        "centroid_lat": value.centroid_lat,
        "bbox_min_lon": value.bbox_min_lon,
        "bbox_min_lat": value.bbox_min_lat,
        "bbox_max_lon": value.bbox_max_lon,
        "bbox_max_lat": value.bbox_max_lat,
        "area_m2": value.area_m2,
        "area_bucket": value.area_bucket,
        "geometry_hash": value.geometry_hash,
    }


def _failure_row(value: GeometryFailure) -> dict[str, Any]:
    return {
        "osm_type": value.identity.osm_type if value.identity is not None else None,
        "osm_id": value.identity.osm_id if value.identity is not None else None,
        "source_pbf": value.source_pbf,
        "candidate_kind": value.candidate_kind,
        "failure_kind": value.failure_kind,
        "message": value.message,
    }


class _ShardWriter[ValueT]:
    def __init__(
        self,
        directory: Path,
        *,
        source_stem: str,
        schema: pa.Schema,
        row_builder: Callable[[ValueT], dict[str, Any]],
        batch_rows: int,
    ) -> None:
        if batch_rows < 1:
            raise ValueError("batch_rows must be positive")
        if not source_stem or Path(source_stem).name != source_stem:
            raise ValueError("source_stem must be a non-empty filename stem")
        self._directory = directory
        self._source_stem = source_stem
        self._schema = schema
        self._row_builder = row_builder
        self._batch_rows = batch_rows
        self._rows: list[dict[str, Any]] = []
        self._shard_index = 0
        self._closed = False
        self._directory.mkdir(parents=True, exist_ok=True)

    def write(self, value: ValueT) -> None:
        if self._closed:
            raise RuntimeError("cannot write to a closed Parquet writer")
        self._rows.append(self._row_builder(value))
        if len(self._rows) >= self._batch_rows:
            self._flush()

    def close(self) -> None:
        if self._closed:
            return
        self._flush()
        self._closed = True

    def __enter__(self) -> _ShardWriter[ValueT]:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def _flush(self) -> None:
        if not self._rows:
            return
        final = self._directory / f"{self._source_stem}-{self._shard_index:05d}.parquet"
        temporary = self._directory / f".{final.name}.tmp"
        if final.exists() or temporary.exists():
            raise FileExistsError(f"refusing to overwrite existing Parquet shard: {final}")
        table = pa.Table.from_pylist(self._rows, schema=self._schema)
        writer = pq.ParquetWriter(temporary, self._schema, compression="zstd")
        try:
            writer.write_table(table)
        finally:
            writer.close()
        os.replace(temporary, final)
        self._rows.clear()
        self._shard_index += 1


class OccurrenceShardWriter(_ShardWriter[Occurrence]):
    """Write bounded occurrence shards."""

    def __init__(self, directory: Path, *, source_stem: str, batch_rows: int = 5_000) -> None:
        super().__init__(
            directory,
            source_stem=source_stem,
            schema=OCCURRENCE_SCHEMA,
            row_builder=_occurrence_row,
            batch_rows=batch_rows,
        )

    def __enter__(self) -> OccurrenceShardWriter:
        super().__enter__()
        return self


class FailureShardWriter(_ShardWriter[GeometryFailure]):
    """Write bounded geometry-failure shards."""

    def __init__(self, directory: Path, *, source_stem: str, batch_rows: int = 5_000) -> None:
        super().__init__(
            directory,
            source_stem=source_stem,
            schema=FAILURE_SCHEMA,
            row_builder=_failure_row,
            batch_rows=batch_rows,
        )

    def __enter__(self) -> FailureShardWriter:
        super().__enter__()
        return self
