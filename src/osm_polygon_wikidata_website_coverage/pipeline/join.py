"""Materialize the two successful-text membership sets once per run."""

from __future__ import annotations

import shutil
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.io.atomic import read_json_object, write_json
from osm_polygon_wikidata_website_coverage.io.duckdb import configure_connection, export_query
from osm_polygon_wikidata_website_coverage.io.parquet import MEMBERSHIP_SCHEMA
from osm_polygon_wikidata_website_coverage.sources._files import file_inventory
from osm_polygon_wikidata_website_coverage.sources.website import (
    WEBSITE_SUCCESS_SQL,
    validate_website_source,
    website_success_parameters,
)
from osm_polygon_wikidata_website_coverage.sources.wikimedia import (
    WIKIDATA_SUCCESS_SQL,
    validate_wikidata_source,
    wikidata_success_parameters,
)


@dataclass(frozen=True, slots=True)
class MembershipResult:
    """Local key-only membership files used by the overlap join."""

    paths: tuple[Path, Path]
    manifest_path: Path | None = None


_write_query = export_query


def _source_inventory(paths: DataPaths) -> list[dict[str, Any]]:
    website_files = validate_website_source(paths.website_root)
    link_files, document_files = validate_wikidata_source(paths.wikidata_root)
    return [
        *file_inventory(paths.website_root, website_files, label="website"),
        *file_inventory(paths.wikidata_root, link_files, label="wikimedia-links"),
        *file_inventory(paths.wikidata_root, document_files, label="wikimedia-documents"),
    ]


def _output_paths(run_root: Path) -> tuple[Path, Path]:
    members = run_root / "members"
    return members / "website.parquet", members / "wikidata.parquet"


def _manifest_path(run_root: Path) -> Path:
    return run_root / "members" / "manifest.json"


def _read_manifest(path: Path) -> dict[str, Any] | None:
    return read_json_object(path)


def _output_is_valid(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        metadata = pq.ParquetFile(path).metadata
        return metadata is not None and pq.read_schema(path) == MEMBERSHIP_SCHEMA
    except (OSError, ValueError, pa.ArrowException):
        return False


def _stage_is_reusable(
    run_root: Path,
    outputs: tuple[Path, Path],
    source_inventory: list[dict[str, Any]],
) -> bool:
    manifest = _read_manifest(_manifest_path(run_root))
    return bool(
        manifest
        and manifest.get("schema_version") == "1"
        and manifest.get("source_inventory") == source_inventory
        and all(_output_is_valid(path) for path in outputs)
    )


def _write_manifest(path: Path, source_inventory: list[dict[str, Any]]) -> None:
    write_json(
        path,
        {
            "schema_version": "1",
            "source_inventory": source_inventory,
            "outputs": ["website.parquet", "wikidata.parquet"],
        },
    )


def _cleanup_spill(run_root: Path) -> None:
    scratch = run_root / "scratch"
    shutil.rmtree(scratch / "duckdb-temp", ignore_errors=True)
    with suppress(OSError):
        scratch.rmdir()


def load_memberships(paths: DataPaths, run_root: Path, *, resume: bool = False) -> MembershipResult:
    """Read successful text predicates and write exactly two local key tables."""

    outputs = _output_paths(run_root)
    source_inventory = _source_inventory(paths)
    if resume and _stage_is_reusable(run_root, outputs, source_inventory):
        return MembershipResult(outputs, _manifest_path(run_root))

    connection = duckdb.connect(database=":memory:")
    try:
        configure_connection(connection, run_root)
        _write_query(
            connection,
            WEBSITE_SUCCESS_SQL,
            website_success_parameters(paths.website_root),
            outputs[0],
        )
        _write_query(
            connection,
            WIKIDATA_SUCCESS_SQL,
            wikidata_success_parameters(paths.wikidata_root),
            outputs[1],
        )
    finally:
        try:
            connection.close()
        finally:
            _cleanup_spill(run_root)
    _write_manifest(_manifest_path(run_root), source_inventory)
    return MembershipResult(outputs, _manifest_path(run_root))
