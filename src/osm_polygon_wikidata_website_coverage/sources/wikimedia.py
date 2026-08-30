"""Read-only successful Wikipedia/Wikivoyage membership as one Wikidata set."""

from __future__ import annotations

from pathlib import Path

from osm_polygon_wikidata_website_coverage.sources._files import (
    parquet_files,
    read_column_names,
    validate_columns,
)

PROJECTS = ("wikipedia", "wikivoyage")
WIKIMEDIA_LINK_REQUIRED_COLUMNS = frozenset({"project", "document_id", "osm_type", "osm_id"})
WIKIMEDIA_DOCUMENT_REQUIRED_COLUMNS = frozenset(
    {"project", "document_id", "fetch_status", "full_text"}
)
WIKIDATA_SUCCESS_SQL = """
WITH linked AS (
    SELECT
        project,
        document_id,
        osm_type,
        CAST(osm_id AS BIGINT) AS osm_id
    FROM read_parquet(?, union_by_name = true)
    WHERE project IN ('wikipedia', 'wikivoyage')
      AND osm_type IN ('way', 'relation')
), successful_documents AS (
    SELECT project, document_id
    FROM read_parquet(?, union_by_name = true)
    WHERE project IN ('wikipedia', 'wikivoyage')
      AND fetch_status = 'ok'
      AND length(trim(coalesce(full_text, ''))) > 0
)
SELECT DISTINCT linked.osm_type, linked.osm_id
FROM linked
JOIN successful_documents
  ON successful_documents.project = linked.project
 AND successful_documents.document_id = linked.document_id
"""


def wikimedia_link_files(root: Path) -> tuple[Path, ...]:
    """Return sorted polygon-document link Parquets."""

    return parquet_files(root / "polygon_document_links", "Wikimedia link", "Wikimedia link")


def wikimedia_document_files(root: Path, project: str) -> tuple[Path, ...]:
    """Return sorted document Parquets for one supported project."""

    if project not in PROJECTS:
        raise ValueError("project must be wikipedia or wikivoyage")
    return parquet_files(root / project / "documents", f"{project} document", f"{project} document")


def _column_names(path: Path) -> set[str]:
    return read_column_names(path, "Wikimedia")


def validate_wikidata_source(root: Path) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    """Validate links and both project document trees."""

    link_files = wikimedia_link_files(root)
    validate_columns(link_files, WIKIMEDIA_LINK_REQUIRED_COLUMNS, "Wikimedia link")
    document_files = tuple(
        path for project in PROJECTS for path in wikimedia_document_files(root, project)
    )
    validate_columns(document_files, WIKIMEDIA_DOCUMENT_REQUIRED_COLUMNS, "Wikimedia document")
    return link_files, document_files


def wikidata_success_parameters(root: Path) -> list[str]:
    """Return parameters for the combined successful-membership query."""

    return [
        str(root / "polygon_document_links" / "*.parquet"),
        str(root / "*" / "documents" / "*.parquet"),
    ]
