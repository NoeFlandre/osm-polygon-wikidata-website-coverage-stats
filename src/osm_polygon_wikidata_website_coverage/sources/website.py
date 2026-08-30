"""Read-only successful website-text membership."""

from __future__ import annotations

from pathlib import Path

from osm_polygon_wikidata_website_coverage.sources._files import (
    parquet_files,
    validate_columns,
)

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
SELECT DISTINCT osm_type, CAST(osm_id AS BIGINT) AS osm_id
FROM read_parquet(?, union_by_name = true)
WHERE osm_type IN ('way', 'relation')
  AND (
    (website_text_status = 'success' AND length(trim(coalesce(website_text, ''))) > 0)
    OR
    (contact_website_text_status = 'success'
     AND length(trim(coalesce(contact_website_text, ''))) > 0)
  )
"""


def website_parquet_files(root: Path) -> tuple[Path, ...]:
    """Return sorted website Parquets without touching source contents beyond metadata."""

    return parquet_files(root / "polygons", "website polygons", "website")


def validate_website_source(root: Path) -> tuple[Path, ...]:
    """Validate every website file and return its sorted inventory."""

    files = website_parquet_files(root)
    validate_columns(files, WEBSITE_REQUIRED_COLUMNS, "website")
    return files


def website_success_parameters(root: Path) -> list[str]:
    """Return parameters for the reusable successful-membership query."""

    return [str(root / "polygons" / "*.parquet")]
