"""End-to-end run orchestration and completion manifest generation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.io.parquet import FAILURE_SCHEMA, OCCURRENCE_SCHEMA
from osm_polygon_wikidata_website_coverage.io.pbf import Result
from osm_polygon_wikidata_website_coverage.pipeline.aggregate import (
    BY_PBF_SCHEMA,
    COMPACT_GLOBAL_SCHEMA,
    CONFLICT_SCHEMA,
    FAILURE_SUMMARY_SCHEMA,
    GLOBAL_SUMMARY_SCHEMA,
    GROUP_METRIC_SCHEMA,
    GROUP_SUMMARY_SCHEMA,
    OVERLAP_SUMMARY_SCHEMA,
    AggregationResult,
    aggregate_run,
)
from osm_polygon_wikidata_website_coverage.pipeline.extract import (
    ExtractionResult,
    extract_all,
)
from osm_polygon_wikidata_website_coverage.pipeline.join import (
    MEMBERSHIP_SCHEMA,
    MembershipResult,
    load_source_membership,
)
from osm_polygon_wikidata_website_coverage.reporting.render import render_reports

Scanner = Callable[[Path, Callable[[Result], None]], None]


@dataclass(frozen=True, slots=True)
class RunResult:
    """Artifacts produced by a complete extraction and aggregation run."""

    run_root: Path
    extraction: ExtractionResult
    membership: MembershipResult
    aggregation: AggregationResult
    manifest_path: Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _file_metadata(path: Path, *, relative_to: Path, include_hash: bool) -> dict[str, Any]:
    stat = path.stat()
    result: dict[str, Any] = {
        "path": str(path.relative_to(relative_to)),
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }
    if include_hash:
        result["sha256"] = _sha256(path)
    return result


def _source_parquet_inventory(paths: DataPaths) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for label, root in (
        ("wikidata", paths.wikidata_root),
        ("website", paths.website_root),
    ):
        for path in sorted(root.rglob("*.parquet")):
            item = _file_metadata(path, relative_to=root, include_hash=True)
            item["source"] = label
            entries.append(item)
    return entries


def _generated_parquet_inventory(run_root: Path) -> list[dict[str, Any]]:
    return [
        _file_metadata(path, relative_to=run_root, include_hash=True)
        for path in sorted(run_root.rglob("*.parquet"))
    ]


def _generated_artifact_inventory(run_root: Path) -> list[dict[str, Any]]:
    return [
        _file_metadata(path, relative_to=run_root, include_hash=True)
        for path in sorted(run_root.rglob("*"))
        if path.is_file() and not path.name.endswith(".tmp")
    ]


def _validate_generated_parquets(paths: list[dict[str, Any]], run_root: Path) -> None:
    expected_schemas = {
        "coverage/by-pbf/by-pbf.parquet": BY_PBF_SCHEMA,
        "scratch/global_compact.parquet": _scratch_schema(),
        "coverage/global": COMPACT_GLOBAL_SCHEMA,
        "members": MEMBERSHIP_SCHEMA,
        "occurrences": OCCURRENCE_SCHEMA,
        "geometry-failures": FAILURE_SCHEMA,
        "summaries/global.parquet": GLOBAL_SUMMARY_SCHEMA,
        "summaries/by-source-pbf.parquet": GROUP_SUMMARY_SCHEMA,
        "summaries/by-region.parquet": GROUP_SUMMARY_SCHEMA,
        "summaries/by-source-pbf-metrics.parquet": GROUP_METRIC_SCHEMA,
        "summaries/by-region-metrics.parquet": GROUP_METRIC_SCHEMA,
        "summaries/by-overlap.parquet": OVERLAP_SUMMARY_SCHEMA,
        "summaries/geometry-failures.parquet": FAILURE_SUMMARY_SCHEMA,
        "summaries/conflicts.parquet": CONFLICT_SCHEMA,
    }
    validated = _validated_generated_entries(paths, run_root, expected_schemas)
    _validate_generated_inventory(validated)


def _validated_generated_entries(
    paths: list[dict[str, Any]], run_root: Path, expected_schemas: dict[str, Any]
) -> dict[str, int]:
    return {
        relative: row_count
        for relative, row_count in (
            _validate_generated_entry(entry, run_root, expected_schemas) for entry in paths
        )
    }


def _validate_generated_entry(
    entry: dict[str, Any], run_root: Path, expected_schemas: dict[str, Any]
) -> tuple[str, int]:
    parquet_path = run_root / entry["path"]
    metadata = pq.ParquetFile(parquet_path).metadata
    if metadata is None or metadata.num_rows < 0:
        raise RuntimeError(f"invalid generated Parquet metadata: {parquet_path}")
    relative = str(entry["path"])
    expected = _generated_schema(relative, expected_schemas)
    if expected is None:
        raise RuntimeError(f"unexpected generated Parquet: {parquet_path}")
    if not _schema_matches(pq.read_schema(parquet_path), expected):
        raise RuntimeError(f"generated Parquet schema mismatch: {parquet_path}")
    return relative, metadata.num_rows


def _schema_matches(actual: Any, expected: Any) -> bool:
    return actual.names == expected.names and [field.type for field in actual] == [
        field.type for field in expected
    ]


def _validate_generated_inventory(validated: dict[str, int]) -> None:
    required = {
        "coverage/by-pbf/by-pbf.parquet",
        "scratch/global_compact.parquet",
        "members/website.parquet",
        "members/wikipedia.parquet",
        "members/wikivoyage.parquet",
        "summaries/global.parquet",
        "summaries/by-source-pbf.parquet",
        "summaries/by-region.parquet",
        "summaries/by-source-pbf-metrics.parquet",
        "summaries/by-region-metrics.parquet",
        "summaries/by-overlap.parquet",
        "summaries/geometry-failures.parquet",
        "summaries/conflicts.parquet",
    }
    required.update(f"coverage/global/shard-{index:02d}.parquet" for index in range(64))
    _require_generated_paths(validated, required)
    _require_generated_prefix(
        validated, "occurrences/", "generated occurrence Parquets are missing"
    )
    _require_generated_prefix(
        validated, "geometry-failures/", "generated geometry-failure Parquets are missing"
    )
    _require_generated_names(
        validated,
        "coverage/global/",
        {f"shard-{index:02d}.parquet" for index in range(64)},
        "generated global Parquet shards are not the approved 64-file set",
    )
    _require_generated_names(
        validated,
        "members/",
        {"website.parquet", "wikipedia.parquet", "wikivoyage.parquet"},
        "generated membership Parquets are not the approved three-file set",
    )
    _validate_global_row_count(validated)


def _require_generated_paths(validated: dict[str, int], required: set[str]) -> None:
    missing = sorted(required - validated.keys())
    if missing:
        raise RuntimeError(f"required generated Parquets are missing: {', '.join(missing)}")


def _require_generated_prefix(validated: dict[str, int], prefix: str, message: str) -> None:
    if not any(path.startswith(prefix) for path in validated):
        raise RuntimeError(message)


def _require_generated_names(
    validated: dict[str, int], prefix: str, expected: set[str], message: str
) -> None:
    actual = {Path(path).name for path in validated if path.startswith(prefix)}
    if actual != expected:
        raise RuntimeError(message)


def _validate_global_row_count(validated: dict[str, int]) -> None:
    global_rows = sum(
        count for path, count in validated.items() if path.startswith("coverage/global/")
    )
    if global_rows != validated["scratch/global_compact.parquet"]:
        raise RuntimeError("global shard row count does not match the compact row count")


def _scratch_schema() -> Any:
    return pa.schema([*COMPACT_GLOBAL_SCHEMA, pa.field("shard_id", pa.uint64())])


def _generated_schema(relative: str, expected: dict[str, Any]) -> Any | None:
    exact = expected.get(relative)
    if exact is not None:
        return exact
    for prefix in ("coverage/global/", "members/", "occurrences/", "geometry-failures/"):
        if relative.startswith(prefix) and relative.endswith(".parquet"):
            return expected[prefix.removesuffix("/")]
    return None


def _write_manifest(
    run_root: Path, payload: dict[str, Any], *, replace_existing: bool = False
) -> Path:
    manifest_root = run_root / "manifests"
    manifest_root.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_root / "manifest.json"
    temporary = manifest_root / ".manifest.json.tmp"
    if not replace_existing and (manifest_path.exists() or temporary.exists()):
        raise FileExistsError(f"refusing to overwrite completion manifest: {manifest_path}")
    if replace_existing:
        temporary.unlink(missing_ok=True)
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(manifest_path)
    return manifest_path


def _manifest_payload(
    *,
    paths: DataPaths,
    run_id: str,
    extraction: ExtractionResult,
    membership: MembershipResult,
    aggregation: AggregationResult,
    generated: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
) -> dict[str, Any]:
    input_inventory = [
        {
            "path": item.before.path.name,
            "size_bytes": item.before.size_bytes,
            "mtime_ns": item.before.mtime_ns,
            "sha256": _sha256(item.before.path),
        }
        for item in extraction.source_inventory
    ]
    return {
        "schema_version": "1.0",
        "status": "complete",
        "run_id": run_id,
        "input_roots": {
            "raw_pbf_root": str(paths.raw_pbf_root),
            "wikidata_root": str(paths.wikidata_root),
            "website_root": str(paths.website_root),
        },
        "input_pbf_inventory": input_inventory,
        "source_parquet_inventory": _source_parquet_inventory(paths),
        "schema_versions": {
            "occurrences": "1",
            "membership": "1",
            "coverage": "1",
            "summaries": "1",
        },
        "row_counts": {
            "occurrence_count": extraction.occurrence_count,
            "geometry_failure_count": extraction.failure_count,
            "valid_universe_count": aggregation.global_row_count,
            "website_count": aggregation.summary["website_count"],
            "wikipedia_count": aggregation.summary["wikipedia_count"],
            "wikivoyage_count": aggregation.summary["wikivoyage_count"],
            "covered_by_any_text_count": aggregation.summary["covered_by_any_text_count"],
        },
        "summary": aggregation.summary,
        "membership_diagnostics": [
            {
                "source": item.source,
                "input_file_count": item.input_file_count,
                "successful_row_count": item.successful_row_count,
                "successful_key_count": item.successful_key_count,
                "duplicate_key_count": item.duplicate_key_count,
            }
            for item in membership.diagnostics
        ],
        "generated_parquet_count": len(generated),
        "generated_parquet_inventory": generated,
        "generated_artifact_count": len(artifacts),
        "generated_artifact_inventory": artifacts,
    }


def run_analysis(
    paths: DataPaths,
    run_id: str,
    *,
    scanner: Scanner | None = None,
    batch_rows: int = 5_000,
    workers: int = 1,
    resume: bool = False,
) -> RunResult:
    """Run extraction, source joins, aggregation, and manifest validation."""

    extraction_kwargs: dict[str, Any] = {"batch_rows": batch_rows, "workers": workers}
    if scanner is not None:
        extraction_kwargs["scanner"] = scanner
    if resume:
        extraction_kwargs["resume"] = True
    extraction = extract_all(paths, run_id, **extraction_kwargs)
    if resume:
        membership = load_source_membership(paths, extraction.run_root, resume=True)
        aggregation = aggregate_run(
            occurrence_root=extraction.run_root / "occurrences",
            membership_root=extraction.run_root / "members",
            output_root=extraction.run_root,
            resume=True,
        )
        render_reports(aggregation.summary, extraction.run_root / "reports", resume=True)
    else:
        membership = load_source_membership(paths, extraction.run_root)
        aggregation = aggregate_run(
            occurrence_root=extraction.run_root / "occurrences",
            membership_root=extraction.run_root / "members",
            output_root=extraction.run_root,
        )
        render_reports(aggregation.summary, extraction.run_root / "reports")
    generated = _generated_parquet_inventory(extraction.run_root)
    artifacts = _generated_artifact_inventory(extraction.run_root)
    _validate_generated_parquets(generated, extraction.run_root)
    payload = _manifest_payload(
        paths=paths,
        run_id=run_id,
        extraction=extraction,
        membership=membership,
        aggregation=aggregation,
        generated=generated,
        artifacts=artifacts,
    )
    if resume:
        manifest_path = _write_manifest(extraction.run_root, payload, replace_existing=True)
    else:
        manifest_path = _write_manifest(extraction.run_root, payload)
    return RunResult(extraction.run_root, extraction, membership, aggregation, manifest_path)
