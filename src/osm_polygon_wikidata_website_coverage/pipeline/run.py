"""Composition root for the website-versus-Wikidata overlap run."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.io.atomic import atomic_path
from osm_polygon_wikidata_website_coverage.io.pbf import scan_pbf_keys
from osm_polygon_wikidata_website_coverage.pipeline.extract import (
    ExtractionResult,
    extract_all,
    scanner_mode,
)
from osm_polygon_wikidata_website_coverage.pipeline.join import MembershipResult, load_memberships
from osm_polygon_wikidata_website_coverage.pipeline.overlap import OverlapResult, compute_overlap

Scanner = Callable[[Path, Callable[[OsmIdentity], None]], None]


@dataclass(frozen=True, slots=True)
class RunResult:
    """Artifacts produced by a complete overlap run."""

    run_root: Path
    extraction: ExtractionResult
    membership: MembershipResult
    overlap: OverlapResult
    manifest_path: Path


def _membership_counts(membership: MembershipResult) -> tuple[int, int]:
    counts: list[int] = []
    for path in membership.paths:
        metadata = pq.ParquetFile(path).metadata
        if metadata is None:
            raise RuntimeError(f"membership Parquet has no metadata: {path}")
        counts.append(metadata.num_rows)
    return counts[0], counts[1]


def _write_manifest(
    paths: DataPaths,
    run_id: str,
    extraction: ExtractionResult,
    membership: MembershipResult,
    overlap: OverlapResult,
    *,
    scanner: Scanner,
    replace_existing: bool,
) -> Path:
    manifest_root = extraction.run_root / "manifests"
    manifest_root.mkdir(parents=True, exist_ok=True)
    output = manifest_root / "manifest.json"
    temporary = output.with_name(f".{output.name}.tmp")
    if not replace_existing and (output.exists() or temporary.exists()):
        raise FileExistsError(f"refusing to overwrite completion manifest: {output}")
    website_count, wikidata_count = _membership_counts(membership)
    payload = {
        "schema_version": "1",
        "status": "complete",
        "run_id": run_id,
        "scanner_mode": scanner_mode(scanner),
        "input_roots": {
            "raw_pbf_root": str(paths.raw_pbf_root),
            "wikidata_root": str(paths.wikidata_root),
            "website_root": str(paths.website_root),
        },
        "row_counts": {
            "raw_universe": overlap.row_count,
            "website": website_count,
            "wikidata": wikidata_count,
        },
        "overlap_counts": overlap.summary,
        "raw_occurrence_count": extraction.occurrence_count,
        "source_pbf_count": len(extraction.source_inventory),
        "outputs": [str(path.relative_to(extraction.run_root)) for path in overlap.paths]
        + [str(overlap.summary_path.relative_to(extraction.run_root))],
    }
    with atomic_path(output) as temporary:
        temporary.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return output


def run_analysis(
    paths: DataPaths,
    run_id: str,
    *,
    scanner: Scanner = scan_pbf_keys,
    batch_rows: int = 100_000,
    workers: int = 1,
    resume: bool = False,
) -> RunResult:
    """Extract, join, and classify the raw universe into four overlap categories."""

    extraction = extract_all(
        paths,
        run_id,
        scanner=scanner,
        batch_rows=batch_rows,
        workers=workers,
        resume=resume,
    )
    membership = load_memberships(paths, extraction.run_root, resume=resume)
    overlap = compute_overlap(
        extraction.run_root / "raw-identities",
        membership,
        extraction.run_root,
        resume=resume,
    )
    manifest_path = _write_manifest(
        paths,
        run_id,
        extraction,
        membership,
        overlap,
        scanner=scanner,
        replace_existing=resume,
    )
    return RunResult(extraction.run_root, extraction, membership, overlap, manifest_path)
