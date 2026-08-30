from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def write_source_tree(root: Path) -> None:
    (root / "polygons").mkdir(parents=True)
    write_rows(
        root / "polygons" / "website.parquet",
        [
            {
                "osm_type": "way",
                "osm_id": 1,
                "website_text_status": "success",
                "website_text": "site",
                "contact_website_text_status": "absent",
                "contact_website_text": None,
            },
            {
                "osm_type": "relation",
                "osm_id": 2,
                "website_text_status": "failed",
                "website_text": "ignored",
                "contact_website_text_status": "success",
                "contact_website_text": " contact ",
            },
        ],
    )


def write_wikidata_tree(root: Path) -> None:
    write_rows(
        root / "polygon_document_links" / "links.parquet",
        [
            {"project": "wikipedia", "document_id": "w1", "osm_type": "way", "osm_id": 1},
            {"project": "wikivoyage", "document_id": "v1", "osm_type": "relation", "osm_id": 2},
            {"project": "wikipedia", "document_id": "empty", "osm_type": "way", "osm_id": 4},
            {"project": "wikipedia", "document_id": "odd", "osm_type": "way", "osm_id": 3},
        ],
    )
    write_rows(
        root / "wikipedia" / "documents" / "documents.parquet",
        [
            {
                "project": "wikipedia",
                "document_id": "w1",
                "fetch_status": "ok",
                "full_text": "wiki",
            },
            {
                "project": "wikipedia",
                "document_id": "empty",
                "fetch_status": "ok",
                "full_text": " ",
            },
            {
                "project": "wikipedia",
                "document_id": "odd",
                "fetch_status": "error",
                "full_text": "bad",
            },
        ],
    )
    write_rows(
        root / "wikivoyage" / "documents" / "documents.parquet",
        [
            {
                "project": "wikivoyage",
                "document_id": "v1",
                "fetch_status": "ok",
                "full_text": "voyage",
            },
        ],
    )
