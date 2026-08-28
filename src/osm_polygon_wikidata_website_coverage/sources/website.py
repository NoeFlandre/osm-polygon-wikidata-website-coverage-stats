"""Read-only successful website-text membership queries."""

from __future__ import annotations

from pathlib import Path

import duckdb

from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.sources._duckdb import read_only_connection
from osm_polygon_wikidata_website_coverage.sources._files import SourceDatasetError, parquet_files

WEBSITE_REQUIRED_COLUMNS = frozenset(
    {
        "osm_type",
        "osm_id",
        "website_text_status",
        "website_text",
        "contact_website_text_status",
        "contact_website_text",
    }
)

WEBSITE_SUCCESS_SQL = """
SELECT osm_type, CAST(osm_id AS BIGINT) AS osm_id
FROM read_parquet(?, union_by_name = true)
WHERE osm_type IN ('way', 'relation')
  AND (
    (
      website_text_status = 'success'
      AND length(trim(coalesce(website_text, ''))) > 0
    )
    OR (
      contact_website_text_status = 'success'
      AND length(trim(coalesce(contact_website_text, ''))) > 0
    )
  )
"""


def website_parquet_files(root: Path) -> tuple[Path, ...]:
    """Return sorted website polygon Parquets without touching their contents."""

    return parquet_files(root / "polygons", "website polygons", "website")


def _column_names(connection: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    rows = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    return {str(row[0]) for row in rows}


def validate_website_source(
    root: Path,
    connection: duckdb.DuckDBPyConnection,
) -> tuple[Path, ...]:
    """Validate every website file and return its sorted inventory."""

    files = website_parquet_files(root)
    missing_by_file = {
        path: WEBSITE_REQUIRED_COLUMNS - _column_names(connection, path) for path in files
    }
    missing = next(((path, columns) for path, columns in missing_by_file.items() if columns), None)
    if missing is not None:
        path, columns = missing
        names = ", ".join(sorted(columns))
        raise SourceDatasetError(f"website file {path} is missing columns: {names}")
    return files


def read_successful_website_keys(root: Path) -> set[OsmIdentity]:
    """Return identities with successful nonempty website or contact text."""

    with read_only_connection() as connection:
        validate_website_source(root, connection)
        rows = connection.execute(
            WEBSITE_SUCCESS_SQL, [str(root / "polygons" / "*.parquet")]
        ).fetchall()
    return {OsmIdentity(str(osm_type), int(osm_id)) for osm_type, osm_id in rows}


def website_success_parameters(root: Path) -> list[str]:
    """Return parameters for the reusable successful-membership query."""

    return [str(root / "polygons" / "*.parquet")]
