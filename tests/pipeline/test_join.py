from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.pipeline.join import load_source_membership


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_load_source_membership_writes_three_key_only_tables_and_diagnostics(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    wikidata = tmp_path / "processed_v2"
    website = tmp_path / "website"
    _write_rows(
        website / "polygons" / "fixture.parquet",
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
                "osm_type": "way",
                "osm_id": 1,
                "website_text_status": "success",
                "website_text": "site again",
                "contact_website_text_status": "absent",
                "contact_website_text": None,
            },
        ],
    )
    _write_rows(
        wikidata / "polygon_document_links" / "links.parquet",
        [{"project": "wikipedia", "document_id": "w1", "osm_type": "way", "osm_id": 1}],
    )
    _write_rows(
        wikidata / "wikipedia" / "documents" / "documents.parquet",
        [{"project": "wikipedia", "document_id": "w1", "fetch_status": "ok", "full_text": "text"}],
    )
    _write_rows(
        wikidata / "wikivoyage" / "documents" / "documents.parquet",
        [{"project": "wikivoyage", "document_id": "v1", "fetch_status": "error", "full_text": ""}],
    )
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*.parquet"))
    paths = DataPaths(tmp_path / "data", raw, wikidata, website)

    result = load_source_membership(paths, tmp_path / "run")

    assert {item.source for item in result.diagnostics} == {
        "website",
        "wikipedia",
        "wikivoyage",
    }
    website_diagnostic = next(item for item in result.diagnostics if item.source == "website")
    assert website_diagnostic.successful_key_count == 1
    assert website_diagnostic.duplicate_key_count == 1
    assert sorted(result.membership_paths) == [
        tmp_path / "run" / "members" / "website.parquet",
        tmp_path / "run" / "members" / "wikipedia.parquet",
        tmp_path / "run" / "members" / "wikivoyage.parquet",
    ]
    for path in result.membership_paths:
        assert pq.read_schema(path).names == ["osm_type", "osm_id"]
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*.parquet")) == sorted(
        before
        + [
            Path("run/members/website.parquet"),
            Path("run/members/wikipedia.parquet"),
            Path("run/members/wikivoyage.parquet"),
        ]
    )
