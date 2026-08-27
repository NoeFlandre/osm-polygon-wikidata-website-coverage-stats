import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import Occurrence, OsmIdentity
from osm_polygon_wikidata_website_coverage.pipeline.run import run_analysis


def test_data_paths_expose_a_deterministic_run_root_for_pipeline(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    paths = DataPaths(tmp_path / "data", source, source, source)

    assert paths.run_root("20260827-coverage-v1") == (
        tmp_path / "data" / "runs" / "20260827-coverage-v1"
    )


def test_run_analysis_writes_complete_manifest_after_all_stages(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    website = tmp_path / "website"
    (website / "polygons").mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "osm_type": "way",
                    "osm_id": 1,
                    "website_text_status": "success",
                    "website_text": "site",
                    "contact_website_text_status": "absent",
                    "contact_website_text": None,
                }
            ]
        ),
        website / "polygons" / "polygons.parquet",
    )
    wikidata = tmp_path / "processed_v2"
    (wikidata / "polygon_document_links").mkdir(parents=True)
    (wikidata / "wikipedia" / "documents").mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [{"project": "wikipedia", "document_id": "w1", "osm_type": "way", "osm_id": 1}]
        ),
        wikidata / "polygon_document_links" / "links.parquet",
    )
    (wikidata / "wikivoyage" / "documents").mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "project": "wikipedia",
                    "document_id": "w1",
                    "fetch_status": "ok",
                    "full_text": "text",
                }
            ]
        ),
        wikidata / "wikipedia" / "documents" / "docs.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "project": "wikivoyage",
                    "document_id": "v1",
                    "fetch_status": "error",
                    "full_text": "",
                }
            ]
        ),
        wikidata / "wikivoyage" / "documents" / "docs.parquet",
    )

    def scanner(path: Path, callback) -> None:
        callback(
            Occurrence(
                identity=OsmIdentity("way", 1),
                source_pbf=path.name,
                region="fixture",
                osm_version=1,
                osm_timestamp="2026-01-01T00:00:00Z",
                geometry_type="Polygon",
                geometry='{"coordinates":[],"type":"Polygon"}',
                centroid_lon=0.5,
                centroid_lat=0.5,
                bbox_min_lon=0.0,
                bbox_min_lat=0.0,
                bbox_max_lon=1.0,
                bbox_max_lat=1.0,
                area_m2=1.0,
                area_bucket="under_1e3_m2",
                geometry_hash="a" * 64,
            )
        )

    paths = DataPaths(tmp_path / "data", raw, wikidata, website)
    result = run_analysis(paths, "fixture", scanner=scanner, batch_rows=1)

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert manifest["run_id"] == "fixture"
    assert manifest["row_counts"]["valid_universe_count"] == 1
    assert manifest["input_pbf_inventory"][0]["path"] == "fixture-latest.osm.pbf"
    assert manifest["generated_parquet_count"] > 0
    assert result.manifest_path.name == "manifest.json"
