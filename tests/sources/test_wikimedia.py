from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.sources.wikimedia import (
    SourceDatasetError,
    read_successful_wikimedia_keys,
)


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_wikimedia_success_is_project_specific_and_requires_full_text(tmp_path: Path) -> None:
    root = tmp_path / "processed_v2"
    _write_rows(
        root / "polygon_document_links" / "links.parquet",
        [
            {"project": "wikipedia", "document_id": "w1", "osm_type": "way", "osm_id": 10},
            {"project": "wikivoyage", "document_id": "v1", "osm_type": "way", "osm_id": 11},
            {"project": "wikipedia", "document_id": "w2", "osm_type": "way", "osm_id": 12},
            {"project": "wikipedia", "document_id": "w3", "osm_type": "way", "osm_id": 13},
        ],
    )
    _write_rows(
        root / "wikipedia" / "documents" / "documents.parquet",
        [
            {
                "project": "wikipedia",
                "document_id": "w1",
                "fetch_status": "ok",
                "full_text": "Article",
            },
            {"project": "wikipedia", "document_id": "w2", "fetch_status": "ok", "full_text": ""},
            {
                "project": "wikipedia",
                "document_id": "w3",
                "fetch_status": "error",
                "full_text": "Failed",
            },
        ],
    )
    _write_rows(
        root / "wikivoyage" / "documents" / "documents.parquet",
        [
            {
                "project": "wikivoyage",
                "document_id": "v1",
                "fetch_status": "ok",
                "full_text": "Guide",
            }
        ],
    )

    assert read_successful_wikimedia_keys(root, project="wikipedia") == {OsmIdentity("way", 10)}
    assert read_successful_wikimedia_keys(root, project="wikivoyage") == {OsmIdentity("way", 11)}


def test_wikimedia_reader_rejects_unknown_project(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="wikipedia or wikivoyage"):
        read_successful_wikimedia_keys(tmp_path, project="commons")


def test_wikimedia_reader_rejects_missing_document_columns(tmp_path: Path) -> None:
    root = tmp_path / "processed_v2"
    _write_rows(
        root / "polygon_document_links" / "links.parquet",
        [{"project": "wikipedia", "document_id": "w1", "osm_type": "way", "osm_id": 10}],
    )
    _write_rows(root / "wikipedia" / "documents" / "documents.parquet", [{"document_id": "w1"}])

    with pytest.raises(SourceDatasetError, match="full_text"):
        read_successful_wikimedia_keys(root, project="wikipedia")
