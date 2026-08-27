from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.sources.website import (
    SourceDatasetError,
    read_successful_website_keys,
)


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_website_success_requires_success_status_and_nonempty_text(tmp_path: Path) -> None:
    root = tmp_path / "website"
    _write_rows(
        root / "polygons" / "fixture.parquet",
        [
            {
                "osm_type": "way",
                "osm_id": 1,
                "website_text_status": "success",
                "website_text": "About",
                "contact_website_text_status": "absent",
                "contact_website_text": None,
            },
            {
                "osm_type": "way",
                "osm_id": 2,
                "website_text_status": "empty",
                "website_text": "",
                "contact_website_text_status": "success",
                "contact_website_text": "Contact",
            },
            {
                "osm_type": "way",
                "osm_id": 3,
                "website_text_status": "success",
                "website_text": "   ",
                "contact_website_text_status": "absent",
                "contact_website_text": None,
            },
        ],
    )

    assert read_successful_website_keys(root) == {
        OsmIdentity("way", 1),
        OsmIdentity("way", 2),
    }


def test_website_reader_rejects_missing_required_columns(tmp_path: Path) -> None:
    root = tmp_path / "website"
    _write_rows(root / "polygons" / "fixture.parquet", [{"osm_id": 1}])

    with pytest.raises(SourceDatasetError, match="website_text_status"):
        read_successful_website_keys(root)
