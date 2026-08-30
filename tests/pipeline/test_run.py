import json
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tests.support import write_source_tree, write_wikidata_tree

import osm_polygon_wikidata_website_coverage.pipeline.run as run_module
from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.io.parquet import MEMBERSHIP_SCHEMA
from osm_polygon_wikidata_website_coverage.pipeline.extract import (
    ExtractionResult,
    SourceInventory,
    SourceSnapshot,
)
from osm_polygon_wikidata_website_coverage.pipeline.join import MembershipResult
from osm_polygon_wikidata_website_coverage.pipeline.overlap import OverlapResult
from osm_polygon_wikidata_website_coverage.pipeline.run import RunResult, run_analysis


def test_run_analysis_forwards_defaults_and_stage_arguments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = DataPaths(
        tmp_path / "data", tmp_path / "raw", tmp_path / "wikidata", tmp_path / "website"
    )
    extraction = ExtractionResult(tmp_path / "run", 11, ())
    membership = MembershipResult((tmp_path / "website.parquet", tmp_path / "wikidata.parquet"))
    overlap = OverlapResult(
        (),
        tmp_path / "summary.parquet",
        7,
        {"neither": 1, "website_only": 2, "wikidata_only": 3, "both": 1},
    )
    manifest_path = tmp_path / "manifest.json"
    calls: dict[str, tuple[tuple[object, ...], dict[str, object]]] = {}

    def fake_extract(*args: object, **kwargs: object) -> ExtractionResult:
        calls["extract"] = (args, kwargs)
        return extraction

    def fake_memberships(*args: object, **kwargs: object) -> MembershipResult:
        calls["membership"] = (args, kwargs)
        return membership

    def fake_overlap(*args: object, **kwargs: object) -> OverlapResult:
        calls["overlap"] = (args, kwargs)
        return overlap

    def fake_manifest(*args: object, **kwargs: object) -> Path:
        calls["manifest"] = (args, kwargs)
        return manifest_path

    monkeypatch.setattr(run_module, "extract_all", fake_extract)
    monkeypatch.setattr(run_module, "load_memberships", fake_memberships)
    monkeypatch.setattr(run_module, "compute_overlap", fake_overlap)
    monkeypatch.setattr(run_module, "_write_manifest", fake_manifest)

    result = run_analysis(paths, "fixture")

    assert calls == {
        "extract": (
            (paths, "fixture"),
            {
                "scanner": run_module.scan_pbf_keys,
                "batch_rows": 100_000,
                "workers": 1,
                "resume": False,
            },
        ),
        "membership": ((paths, extraction.run_root), {"resume": False}),
        "overlap": (
            (extraction.run_root / "raw-identities", membership, extraction.run_root),
            {"resume": False},
        ),
        "manifest": (
            (paths, "fixture", extraction, membership, overlap),
            {"scanner": run_module.scan_pbf_keys, "replace_existing": False},
        ),
    }
    assert result == RunResult(extraction.run_root, extraction, membership, overlap, manifest_path)


def test_write_manifest_records_all_metadata_and_supports_resume_overwrite(
    tmp_path: Path,
) -> None:
    run_root = tmp_path / "nested" / "run"
    website_path = tmp_path / "website.parquet"
    wikidata_path = tmp_path / "wikidata.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"osm_type": "way", "osm_id": 1}, {"osm_type": "relation", "osm_id": 2}],
            schema=MEMBERSHIP_SCHEMA,
        ),
        website_path,
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"osm_type": "way", "osm_id": 1},
                {"osm_type": "relation", "osm_id": 2},
                {"osm_type": "way", "osm_id": 3},
            ],
            schema=MEMBERSHIP_SCHEMA,
        ),
        wikidata_path,
    )
    snapshot = SourceSnapshot(tmp_path / "source.pbf", 12, 34, "source-sha")
    extraction = ExtractionResult(
        run_root,
        17,
        (SourceInventory(snapshot, snapshot),),
    )
    membership = MembershipResult((website_path, wikidata_path))
    overlap = OverlapResult(
        (run_root / "coverage" / "overlap" / "shard-00.parquet",),
        run_root / "coverage" / "overlap-summary.parquet",
        11,
        {"neither": 5, "website_only": 2, "wikidata_only": 3, "both": 1},
    )
    paths = DataPaths(
        tmp_path / "data",
        tmp_path / "raw",
        tmp_path / "wikidata-root",
        tmp_path / "website-root",
    )

    def custom_scanner(path: Path, callback) -> None:
        del path, callback

    first = run_module._write_manifest(
        paths,
        "fixture",
        extraction,
        membership,
        overlap,
        scanner=custom_scanner,
        replace_existing=False,
    )
    second = run_module._write_manifest(
        paths,
        "fixture",
        extraction,
        membership,
        overlap,
        scanner=custom_scanner,
        replace_existing=True,
    )

    assert first == run_root / "manifests" / "manifest.json"
    assert second == first
    assert json.loads(first.read_text(encoding="utf-8")) == {
        "schema_version": "1",
        "status": "complete",
        "run_id": "fixture",
        "scanner_mode": "custom",
        "input_roots": {
            "raw_pbf_root": str(paths.raw_pbf_root),
            "wikidata_root": str(paths.wikidata_root),
            "website_root": str(paths.website_root),
        },
        "row_counts": {"raw_universe": 11, "website": 2, "wikidata": 3},
        "overlap_counts": {"neither": 5, "website_only": 2, "wikidata_only": 3, "both": 1},
        "raw_occurrence_count": 17,
        "source_pbf_count": 1,
        "outputs": [
            "coverage/overlap/shard-00.parquet",
            "coverage/overlap-summary.parquet",
        ],
    }
    default = run_module._write_manifest(
        paths,
        "fixture",
        extraction,
        membership,
        overlap,
        scanner=run_module.scan_pbf_keys,
        replace_existing=True,
    )
    assert default == first
    assert json.loads(default.read_text(encoding="utf-8"))["scanner_mode"] == "coverage-only"


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
            cast(ExtractionResult, extraction),
            membership,
            cast(OverlapResult, overlap),
            scanner=run_module.scan_pbf_keys,
            replace_existing=False,
        )
