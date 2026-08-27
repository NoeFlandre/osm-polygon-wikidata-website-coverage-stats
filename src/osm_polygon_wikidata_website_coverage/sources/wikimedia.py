"""Read-only successful Wikipedia and Wikivoyage membership queries."""

from __future__ import annotations

from pathlib import Path

import duckdb

from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.sources._duckdb import read_only_connection

WIKIMEDIA_LINK_REQUIRED_COLUMNS = frozenset({"project", "document_id", "osm_type", "osm_id"})
WIKIMEDIA_DOCUMENT_REQUIRED_COLUMNS = frozenset(
    {"project", "document_id", "fetch_status", "full_text"}
)
SUPPORTED_PROJECTS = frozenset({"wikipedia", "wikivoyage"})

WIKIMEDIA_SUCCESS_SQL = """
WITH linked AS (
    SELECT project, document_id, osm_type, CAST(osm_id AS BIGINT) AS osm_id
    FROM read_parquet(?, union_by_name = true)
    WHERE project = ?
      AND osm_type IN ('way', 'relation')
), successful_documents AS (
    SELECT project, document_id
    FROM read_parquet(?, union_by_name = true)
    WHERE project = ?
      AND fetch_status = 'ok'
      AND length(trim(coalesce(full_text, ''))) > 0
)
SELECT linked.osm_type, linked.osm_id
FROM linked
JOIN successful_documents
  ON successful_documents.project = linked.project
 AND successful_documents.document_id = linked.document_id
"""


class SourceDatasetError(ValueError):
    """Raised when a source tree is missing or violates its schema contract."""


def _files(root: Path, relative: str, description: str) -> tuple[Path, ...]:
    directory = root / relative
    if not directory.is_dir():
        raise SourceDatasetError(f"{description} directory is missing: {directory}")
    files = tuple(sorted(directory.glob("*.parquet")))
    if not files:
        raise SourceDatasetError(f"{description} directory contains no Parquet files: {directory}")
    return files


def wikimedia_link_files(root: Path) -> tuple[Path, ...]:
    """Return sorted polygon-document link Parquets."""

    return _files(root, "polygon_document_links", "Wikimedia link")


def wikimedia_document_files(root: Path, project: str) -> tuple[Path, ...]:
    """Return sorted document Parquets for one Wikimedia project."""

    _validate_project(project)
    return _files(root, f"{project}/documents", f"{project} document")


def _validate_project(project: str) -> None:
    if project not in SUPPORTED_PROJECTS:
        raise ValueError("project must be wikipedia or wikivoyage")


def _column_names(connection: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    rows = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    return {str(row[0]) for row in rows}


def validate_wikimedia_source(
    root: Path,
    project: str,
    connection: duckdb.DuckDBPyConnection,
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    """Validate link/document files and return their sorted inventories."""

    _validate_project(project)
    link_files = wikimedia_link_files(root)
    document_files = wikimedia_document_files(root, project)
    for path in link_files:
        missing = WIKIMEDIA_LINK_REQUIRED_COLUMNS - _column_names(connection, path)
        if missing:
            names = ", ".join(sorted(missing))
            raise SourceDatasetError(f"Wikimedia link file {path} is missing columns: {names}")
    for path in document_files:
        missing = WIKIMEDIA_DOCUMENT_REQUIRED_COLUMNS - _column_names(connection, path)
        if missing:
            names = ", ".join(sorted(missing))
            raise SourceDatasetError(f"Wikimedia document file {path} is missing columns: {names}")
    return link_files, document_files


def read_successful_wikimedia_keys(root: Path, *, project: str) -> set[OsmIdentity]:
    """Return identities linked to successful nonempty project documents."""

    _validate_project(project)
    with read_only_connection() as connection:
        validate_wikimedia_source(root, project, connection)
        parameters = [
            str(root / "polygon_document_links" / "*.parquet"),
            project,
            str(root / project / "documents" / "*.parquet"),
            project,
        ]
        rows = connection.execute(WIKIMEDIA_SUCCESS_SQL, parameters).fetchall()
    return {OsmIdentity(str(osm_type), int(osm_id)) for osm_type, osm_id in rows}


def wikimedia_success_parameters(root: Path, project: str) -> list[str]:
    """Return parameters for the reusable project-specific membership query."""

    _validate_project(project)
    return [
        str(root / "polygon_document_links" / "*.parquet"),
        project,
        str(root / project / "documents" / "*.parquet"),
        project,
    ]
