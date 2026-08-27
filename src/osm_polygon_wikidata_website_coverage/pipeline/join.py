"""Materialize compact source-membership tables under a run directory."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.sources._duckdb import read_only_connection
from osm_polygon_wikidata_website_coverage.sources.website import (
    WEBSITE_SUCCESS_SQL,
    validate_website_source,
    website_success_parameters,
)
from osm_polygon_wikidata_website_coverage.sources.wikimedia import (
    WIKIMEDIA_SUCCESS_SQL,
    validate_wikimedia_source,
    wikimedia_success_parameters,
)

MEMBERSHIP_SCHEMA = pa.schema([pa.field("osm_type", pa.string()), pa.field("osm_id", pa.int64())])


@dataclass(frozen=True, slots=True)
class MembershipDiagnostic:
    source: str
    input_file_count: int
    successful_row_count: int
    successful_key_count: int
    duplicate_key_count: int
    output_path: Path


@dataclass(frozen=True, slots=True)
class MembershipResult:
    membership_paths: tuple[Path, ...]
    diagnostics: tuple[MembershipDiagnostic, ...]


def _count_keys(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[str],
) -> tuple[int, int]:
    row = connection.execute(f"SELECT COUNT(*) FROM ({query}) AS rows", parameters).fetchone()
    assert row is not None
    row_count = int(row[0])
    distinct_row = connection.execute(
        f"SELECT COUNT(*) FROM (SELECT DISTINCT osm_type, osm_id FROM ({query}) AS rows) AS keys",
        parameters,
    ).fetchone()
    assert distinct_row is not None
    key_count = int(distinct_row[0])
    return row_count, key_count


def _write_distinct_keys(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[str],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.tmp")
    if output_path.exists() or temporary.exists():
        raise FileExistsError(f"refusing to overwrite membership table: {output_path}")
    destination = str(temporary).replace("'", "''")
    connection.execute(
        f"""
        COPY (
            SELECT DISTINCT osm_type, osm_id
            FROM ({query}) AS rows
            ORDER BY osm_type, osm_id
        ) TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        parameters,
    )
    os.replace(temporary, output_path)


def _materialize(
    *,
    source: str,
    query: str,
    parameters: list[str],
    input_file_count: int,
    connection: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> MembershipDiagnostic:
    successful_row_count, successful_key_count = _count_keys(connection, query, parameters)
    _write_distinct_keys(connection, query, parameters, output_path)
    return MembershipDiagnostic(
        source=source,
        input_file_count=input_file_count,
        successful_row_count=successful_row_count,
        successful_key_count=successful_key_count,
        duplicate_key_count=successful_row_count - successful_key_count,
        output_path=output_path,
    )


def load_source_membership(paths: DataPaths, run_root: Path) -> MembershipResult:
    """Read source trees and write three key-only membership Parquets."""

    members_root = run_root / "members"
    members_root.mkdir(parents=True, exist_ok=True)
    diagnostics: list[MembershipDiagnostic] = []
    membership_paths: list[Path] = []

    with read_only_connection() as website_connection:
        website_files = validate_website_source(paths.website_root, website_connection)
        website_output = members_root / "website.parquet"
        diagnostics.append(
            _materialize(
                source="website",
                query=WEBSITE_SUCCESS_SQL,
                parameters=website_success_parameters(paths.website_root),
                input_file_count=len(website_files),
                connection=website_connection,
                output_path=website_output,
            )
        )
        membership_paths.append(website_output)

    for project in ("wikipedia", "wikivoyage"):
        with read_only_connection() as connection:
            link_files, document_files = validate_wikimedia_source(
                paths.wikidata_root, project, connection
            )
            output = members_root / f"{project}.parquet"
            diagnostics.append(
                _materialize(
                    source=project,
                    query=WIKIMEDIA_SUCCESS_SQL,
                    parameters=wikimedia_success_parameters(paths.wikidata_root, project),
                    input_file_count=len(link_files) + len(document_files),
                    connection=connection,
                    output_path=output,
                )
            )
            membership_paths.append(output)

    return MembershipResult(tuple(membership_paths), tuple(diagnostics))
