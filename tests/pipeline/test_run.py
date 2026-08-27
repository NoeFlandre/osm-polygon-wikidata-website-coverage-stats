import hashlib
import inspect
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.pipeline.run as run_module
from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import Occurrence, OsmIdentity
from osm_polygon_wikidata_website_coverage.pipeline.aggregate import AggregationResult
from osm_polygon_wikidata_website_coverage.pipeline.extract import ExtractionResult
from osm_polygon_wikidata_website_coverage.pipeline.join import MembershipResult
from osm_polygon_wikidata_website_coverage.pipeline.run import (
    _generated_artifact_inventory,
    _generated_parquet_inventory,
    _sha256,
    _source_parquet_inventory,
    _validate_generated_parquets,
    _write_manifest,
    run_analysis,
)


def test_data_paths_expose_a_deterministic_run_root_for_pipeline(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    paths = DataPaths(tmp_path / "data", source, source, source)

    assert paths.run_root("20260827-coverage-v1") == (
        tmp_path / "data" / "runs" / "20260827-coverage-v1"
    )


def test_run_analysis_writes_complete_manifest_after_all_stages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    assert result.extraction is not None
    assert result.membership is not None
    assert set(manifest) == {
        "schema_version",
        "status",
        "run_id",
        "input_roots",
        "input_pbf_inventory",
        "source_parquet_inventory",
        "schema_versions",
        "row_counts",
        "summary",
        "membership_diagnostics",
        "generated_parquet_count",
        "generated_parquet_inventory",
        "generated_artifact_count",
        "generated_artifact_inventory",
    }
    assert manifest["schema_version"] == "1.0"
    assert manifest["input_roots"] == {
        "raw_pbf_root": str(raw),
        "wikidata_root": str(wikidata),
        "website_root": str(website),
    }
    assert manifest["input_pbf_inventory"] == [
        {
            "path": "fixture-latest.osm.pbf",
            "size_bytes": pbf.stat().st_size,
            "mtime_ns": pbf.stat().st_mtime_ns,
            "sha256": run_module._sha256(pbf),
        }
    ]
    source_inventory = manifest["source_parquet_inventory"]
    assert [(item["source"], item["path"]) for item in source_inventory] == [
        ("wikidata", "polygon_document_links/links.parquet"),
        ("wikidata", "wikipedia/documents/docs.parquet"),
        ("wikidata", "wikivoyage/documents/docs.parquet"),
        ("website", "polygons/polygons.parquet"),
    ]
    assert all(
        set(item) == {"path", "size_bytes", "mtime_ns", "source"} for item in source_inventory
    )
    assert manifest["schema_versions"] == {
        "occurrences": "1",
        "membership": "1",
        "coverage": "1",
        "summaries": "1",
    }
    assert manifest["row_counts"]["valid_universe_count"] == 1
    assert manifest["row_counts"] == {
        "occurrence_count": 1,
        "geometry_failure_count": 0,
        "valid_universe_count": 1,
        "website_count": 1,
        "wikipedia_count": 1,
        "wikivoyage_count": 0,
        "covered_by_any_text_count": 1,
    }
    assert manifest["summary"] == result.aggregation.summary
    assert manifest["membership_diagnostics"] == [
        {
            "source": "website",
            "input_file_count": 1,
            "successful_row_count": 1,
            "successful_key_count": 1,
            "duplicate_key_count": 0,
        },
        {
            "source": "wikipedia",
            "input_file_count": 2,
            "successful_row_count": 1,
            "successful_key_count": 1,
            "duplicate_key_count": 0,
        },
        {
            "source": "wikivoyage",
            "input_file_count": 2,
            "successful_row_count": 0,
            "successful_key_count": 0,
            "duplicate_key_count": 0,
        },
    ]
    assert manifest["input_pbf_inventory"][0]["path"] == "fixture-latest.osm.pbf"
    assert manifest["generated_parquet_count"] > 0
    assert manifest["generated_parquet_count"] == len(manifest["generated_parquet_inventory"])
    assert manifest["generated_artifact_count"] == len(manifest["generated_artifact_inventory"])
    assert all(
        set(item) == {"path", "size_bytes", "mtime_ns", "sha256"}
        and len(item["sha256"]) == 64
        and not item["path"].endswith(".tmp")
        for item in manifest["generated_artifact_inventory"]
    )
    assert (result.run_root / "reports" / "report.md").is_file()
    assert any(
        item["path"] == "reports/report.md" for item in manifest["generated_artifact_inventory"]
    )
    assert result.manifest_path.name == "manifest.json"

    def default_scanner_extraction(paths: DataPaths, run_id: str, **kwargs: int) -> object:
        assert kwargs == {"batch_rows": 1}
        from osm_polygon_wikidata_website_coverage.pipeline.extract import extract_all

        return extract_all(paths, run_id, scanner=scanner, **kwargs)

    monkeypatch.setattr(run_module, "extract_all", default_scanner_extraction)
    default_result = run_analysis(paths, "default-scanner", batch_rows=1)
    assert default_result.aggregation.global_row_count == 1


def test_run_helpers_reject_invalid_generated_metadata_and_existing_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generated = [{"path": "output.parquet"}]

    class NoMetadataFile:
        metadata = None

    monkeypatch.setattr(run_module.pq, "ParquetFile", lambda path: NoMetadataFile())
    with pytest.raises(RuntimeError, match="invalid generated"):
        _validate_generated_parquets(generated, tmp_path)

    manifest_root = tmp_path / "run" / "manifests"
    manifest_root.mkdir(parents=True)
    manifest = manifest_root / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    with pytest.raises(FileExistsError, match="completion manifest"):
        _write_manifest(manifest_root.parent, {"status": "complete"})

    temporary = manifest_root / ".manifest.json.tmp"
    manifest.unlink()
    temporary.write_text("{}", encoding="utf-8")
    with pytest.raises(FileExistsError, match="completion manifest"):
        _write_manifest(manifest_root.parent, {"status": "complete"})


def test_run_helpers_preserve_hash_chunking_and_inventory_contracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    paths = DataPaths(tmp_path / "data", source, source, source)
    source_file = source / "fixture.parquet"
    source_file.write_bytes(b"source")

    assert _source_parquet_inventory(paths) == [
        {
            "path": "fixture.parquet",
            "size_bytes": 6,
            "mtime_ns": source_file.stat().st_mtime_ns,
            "source": "wikidata",
        },
        {
            "path": "fixture.parquet",
            "size_bytes": 6,
            "mtime_ns": source_file.stat().st_mtime_ns,
            "source": "website",
        },
    ]

    run_root = tmp_path / "run"
    nested = run_root / "nested"
    nested.mkdir(parents=True)
    parquet = nested / "output.parquet"
    parquet.write_bytes(b"parquet")
    temporary = nested / "ignored.tmp"
    temporary.write_bytes(b"temporary")
    generated = _generated_parquet_inventory(run_root)
    artifacts = _generated_artifact_inventory(run_root)
    assert generated[0]["path"] == "nested/output.parquet"
    assert generated[0]["sha256"] == hashlib.sha256(b"parquet").hexdigest()
    assert [item["path"] for item in artifacts] == ["nested/output.parquet"]

    class Stream:
        def __init__(self) -> None:
            self.read_sizes: list[int | None] = []
            self.calls = 0

        def __enter__(self) -> "Stream":
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def read(self, size: int | None) -> bytes:
            self.read_sizes.append(size)
            self.calls += 1
            return b"abc" if self.calls == 1 else b""

    stream = Stream()

    def open_stream(path: Path, mode: str) -> Stream:
        assert mode == "rb"
        return stream

    monkeypatch.setattr(Path, "open", open_stream)
    assert _sha256(tmp_path / "fixture") == hashlib.sha256(b"abc").hexdigest()
    assert stream.read_sizes == [8 * 1024 * 1024, 8 * 1024 * 1024]


def test_manifest_writer_is_nested_sorted_utf8_and_newline_terminated(tmp_path: Path) -> None:
    run_root = tmp_path / "one" / "two" / "run"
    manifest = _write_manifest(run_root, {"z": "café", "a": 1})

    assert manifest == run_root / "manifests" / "manifest.json"
    assert manifest.read_text(encoding="utf-8") == '{\n  "a": 1,\n  "z": "café"\n}\n'


def test_manifest_writer_passes_the_exact_json_serialization_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dumps_calls: list[tuple[object, dict[str, object]]] = []

    def fake_dumps(value: object, **kwargs: object) -> str:
        dumps_calls.append((value, kwargs))
        return "{}"

    monkeypatch.setattr(run_module.json, "dumps", fake_dumps)
    manifest = _write_manifest(tmp_path / "run", {"value": "café"})

    assert manifest.read_text(encoding="utf-8") == "{}\n"
    assert dumps_calls == [
        (
            {"value": "café"},
            {"ensure_ascii": False, "indent": 2, "sort_keys": True},
        )
    ]


def test_run_analysis_default_contract_uses_five_thousand_rows() -> None:
    assert inspect.signature(run_analysis).parameters["batch_rows"].default == 5_000


def test_source_inventory_requests_metadata_without_hashes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wikidata = tmp_path / "wikidata"
    website = tmp_path / "website"
    (wikidata / "nested").mkdir(parents=True)
    website.mkdir()
    wikidata_file = wikidata / "nested" / "w.parquet"
    website_file = website / "s.parquet"
    wikidata_file.write_bytes(b"w")
    website_file.write_bytes(b"s")
    paths = DataPaths(tmp_path / "data", tmp_path / "raw", wikidata, website)
    metadata_calls: list[tuple[Path, Path, bool]] = []

    def fake_metadata(path: Path, *, relative_to: Path, include_hash: bool) -> dict[str, object]:
        metadata_calls.append((path, relative_to, include_hash))
        return {"path": str(path.name)}

    monkeypatch.setattr(run_module, "_file_metadata", fake_metadata)

    assert _source_parquet_inventory(paths) == [
        {"path": "w.parquet", "source": "wikidata"},
        {"path": "s.parquet", "source": "website"},
    ]
    assert metadata_calls == [
        (wikidata_file, wikidata, False),
        (website_file, website, False),
    ]


def test_manifest_writer_preserves_temp_safety_and_explicit_utf8(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_root = tmp_path / "run"
    manifest_root = run_root / "manifests"
    manifest_root.mkdir(parents=True)
    sentinel = manifest_root / ".unrelated-manifest-sentinel.tmp"
    sentinel.write_text("sentinel", encoding="utf-8")
    write_calls: list[tuple[Path, str | None]] = []
    original_write_text = Path.write_text

    def tracked_write_text(
        path: Path,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        write_calls.append((path, encoding))
        return original_write_text(path, data, encoding=encoding, errors=errors, newline=newline)

    monkeypatch.setattr(Path, "write_text", tracked_write_text)
    manifest = _write_manifest(run_root, {"value": "café"})

    assert manifest == manifest_root / "manifest.json"
    assert sentinel.read_text(encoding="utf-8") == "sentinel"
    assert write_calls == [(manifest_root / ".manifest.json.tmp", "utf-8")]


def test_run_analysis_passes_exact_stage_roots_and_returns_all_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    wikidata = tmp_path / "wikidata"
    website = tmp_path / "website"
    raw.mkdir()
    wikidata.mkdir()
    website.mkdir()
    paths = DataPaths(tmp_path / "data", raw, wikidata, website)
    run_root = tmp_path / "run"
    extraction = ExtractionResult(run_root, 1, 2, ())
    membership = MembershipResult((), ())
    aggregation = AggregationResult(run_root, (), run_root / "by-pbf.parquet", (), 3, {})

    def scanner(path: Path, callback: object) -> None:
        del path, callback

    calls: dict[str, object] = {}

    def fake_extract(paths_arg: DataPaths, run_id_arg: str, **kwargs: object) -> ExtractionResult:
        calls["extract"] = (paths_arg, run_id_arg, kwargs)
        return extraction

    def fake_membership(paths_arg: DataPaths, run_root_arg: Path) -> MembershipResult:
        calls["membership"] = (paths_arg, run_root_arg)
        return membership

    def fake_aggregate(**kwargs: object) -> AggregationResult:
        calls["aggregate"] = kwargs
        return aggregation

    def fake_render(summary: dict[str, object], report_root: Path) -> None:
        calls["render"] = (summary, report_root)

    def fake_manifest_payload(**kwargs: object) -> dict[str, object]:
        calls["payload"] = kwargs
        return {"status": "complete"}

    def fake_write_manifest(run_root_arg: Path, payload: dict[str, object]) -> Path:
        calls["manifest"] = (run_root_arg, payload)
        return run_root_arg / "manifests" / "manifest.json"

    monkeypatch.setattr(run_module, "extract_all", fake_extract)
    monkeypatch.setattr(run_module, "load_source_membership", fake_membership)
    monkeypatch.setattr(run_module, "aggregate_run", fake_aggregate)
    monkeypatch.setattr(run_module, "render_reports", fake_render)
    monkeypatch.setattr(run_module, "_generated_parquet_inventory", lambda root: [])
    monkeypatch.setattr(run_module, "_generated_artifact_inventory", lambda root: [])
    monkeypatch.setattr(run_module, "_validate_generated_parquets", lambda generated, root: None)
    monkeypatch.setattr(run_module, "_manifest_payload", fake_manifest_payload)
    monkeypatch.setattr(run_module, "_write_manifest", fake_write_manifest)

    result = run_analysis(paths, "fixture", scanner=scanner, batch_rows=17)

    assert calls["extract"] == (paths, "fixture", {"batch_rows": 17, "scanner": scanner})
    assert calls["membership"] == (paths, run_root)
    assert calls["aggregate"] == {
        "occurrence_root": run_root / "occurrences",
        "membership_root": run_root / "members",
        "output_root": run_root,
    }
    assert calls["render"] == ({}, run_root / "reports")
    assert calls["manifest"] == (run_root, {"status": "complete"})
    assert result == run_module.RunResult(
        run_root, extraction, membership, aggregation, run_root / "manifests" / "manifest.json"
    )

    calls.clear()
    run_analysis(paths, "default-batch")
    assert calls["extract"] == (paths, "default-batch", {"batch_rows": 5_000})
