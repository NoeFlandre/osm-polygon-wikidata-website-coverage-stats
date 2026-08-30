import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tests.support import write_source_tree, write_wikidata_tree

import osm_polygon_wikidata_website_coverage.pipeline.run as run_module
from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.io.parquet import MEMBERSHIP_SCHEMA
from osm_polygon_wikidata_website_coverage.pipeline.join import MembershipResult
from osm_polygon_wikidata_website_coverage.pipeline.run import run_analysis


def test_run_analysis_computes_only_the_scoped_overlap(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "fixture-latest.osm.pbf").write_bytes(b"fixture")
    website = tmp_path / "website"
    wikidata = tmp_path / "wikidata"
    write_source_tree(website)
    write_wikidata_tree(wikidata)

    def scanner(path: Path, callback) -> None:
        del path
        callback(OsmIdentity("way", 1))
        callback(OsmIdentity("way", 2))
        callback(OsmIdentity("relation", 5))
        callback(OsmIdentity("relation", 99))

    from tests.support import write_rows

    write_rows(
        website / "polygons" / "extra.parquet",
        [
            {
                "osm_type": "way",
                "osm_id": 2,
                "website_text_status": "success",
                "website_text": "site",
                "contact_website_text_status": "absent",
                "contact_website_text": None,
            }
        ],
    )
    write_rows(
        wikidata / "polygon_document_links" / "extra.parquet",
        [{"project": "wikipedia", "document_id": "w2", "osm_type": "relation", "osm_id": 99}],
    )
    write_rows(
        wikidata / "wikipedia" / "documents" / "extra.parquet",
        [{"project": "wikipedia", "document_id": "w2", "fetch_status": "ok", "full_text": "wiki"}],
    )

    result = run_analysis(
        DataPaths(tmp_path / "data", raw, wikidata, website),
        "fixture",
        scanner=scanner,
        resume=True,
    )

    rows = [row for path in result.overlap.paths for row in pq.read_table(path).to_pylist()]
    assert len(rows) == 4
    assert {row["overlap_category"] for row in rows} == {
        "website_only",
        "wikidata_only",
        "both",
        "neither",
    }
    assert set(path.name for path in result.run_root.iterdir()) == {
        "raw-identities",
        "checkpoints",
        "members",
        "coverage",
        "manifests",
    }
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert manifest["row_counts"] == {"raw_universe": 4, "website": 3, "wikidata": 3}
    assert "geometry" not in json.dumps(manifest)
    assert "full_text" not in json.dumps(manifest)


def test_run_manifest_helpers_reject_missing_metadata_and_existing_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    membership_path = tmp_path / "membership.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=MEMBERSHIP_SCHEMA), membership_path)
    membership = MembershipResult((membership_path, membership_path))
    monkeypatch.setattr(
        run_module.pq,
        "ParquetFile",
        lambda path: SimpleNamespace(metadata=None),
    )
    with pytest.raises(RuntimeError, match="no metadata"):
        run_module._membership_counts(membership)

    run_root = tmp_path / "run"
    manifest_root = run_root / "manifests"
    manifest_root.mkdir(parents=True)
    (manifest_root / "manifest.json").write_text("{}", encoding="utf-8")
    extraction = SimpleNamespace(run_root=run_root, source_inventory=())
    overlap = SimpleNamespace(
        paths=(run_root / "coverage" / "shard-00.parquet",),
        summary_path=run_root / "coverage" / "summary.parquet",
        row_count=0,
        summary={"neither": 0, "website_only": 0, "wikidata_only": 0, "both": 0},
    )
    paths = DataPaths(tmp_path, tmp_path, tmp_path, tmp_path)
    with pytest.raises(FileExistsError, match="completion manifest"):
        run_module._write_manifest(
            paths,
            "fixture",
            extraction,
            membership,
            overlap,
            scanner=run_module.scan_pbf_keys,
            replace_existing=False,
        )
