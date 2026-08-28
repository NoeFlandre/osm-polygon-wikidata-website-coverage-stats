"""Hugging Face staging with an explicit compact-artifact boundary."""

from __future__ import annotations

import hashlib
import json
import math
import shutil
from pathlib import Path
from typing import TypeGuard

import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.domain.coverage import EXPECTED_OVERLAP_CATEGORIES
from osm_polygon_wikidata_website_coverage.pipeline.aggregate import (
    COMPACT_GLOBAL_SCHEMA,
    CONFLICT_SCHEMA,
    EXPECTED_GLOBAL_COLUMNS,
    FAILURE_SUMMARY_SCHEMA,
    GLOBAL_SUMMARY_SCHEMA,
    GROUP_METRIC_SCHEMA,
    GROUP_SUMMARY_SCHEMA,
    OVERLAP_SUMMARY_SCHEMA,
)

TARGET_REPOSITORY = "NoeFlandre/osm-polygon-wikidata-website-coverage-stats"
_FORBIDDEN_FIELDS = {
    "geometry",
    "website_text",
    "contact_website_text",
    "full_text",
    "text",
    "raw_pbf",
    "credentials",
}
_EXPECTED_GLOBAL_FILES = frozenset(f"shard-{index:02d}.parquet" for index in range(64))
_EXPECTED_SUMMARY_SCHEMAS = {
    "global.parquet": GLOBAL_SUMMARY_SCHEMA,
    "by-source-pbf.parquet": GROUP_SUMMARY_SCHEMA,
    "by-region.parquet": GROUP_SUMMARY_SCHEMA,
    "by-source-pbf-metrics.parquet": GROUP_METRIC_SCHEMA,
    "by-region-metrics.parquet": GROUP_METRIC_SCHEMA,
    "by-overlap.parquet": OVERLAP_SUMMARY_SCHEMA,
    "geometry-failures.parquet": FAILURE_SUMMARY_SCHEMA,
    "conflicts.parquet": CONFLICT_SCHEMA,
}
_SUMMARY_KEYS = frozenset(
    {
        "valid_universe_count",
        "website_count",
        "wikipedia_count",
        "wikivoyage_count",
        "covered_by_any_text_count",
        "geometry_failure_count",
        "source_keys_not_in_raw",
        "overlap_categories",
        "pairwise_intersections",
        "osm_type_counts",
        "geometry_type_counts",
        "area_statistics",
    }
)


class PublicationBoundaryError(ValueError):
    """Raised when staging would publish data outside the approved boundary."""


def _check_completed_run(run_root: Path) -> dict[str, object]:
    manifest = run_root / "manifests" / "manifest.json"
    if not manifest.is_file():
        raise PublicationBoundaryError(f"completed manifest is missing: {manifest}")
    try:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PublicationBoundaryError(f"completed manifest is not valid JSON: {manifest}") from exc
    if not isinstance(payload, dict):
        raise PublicationBoundaryError("completed manifest is not a JSON object")
    if payload.get("status") != "complete":
        raise PublicationBoundaryError("run manifest is not complete")
    return payload


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _overlaps(first: Path, second: Path) -> bool:
    return _is_within(first, second) or _is_within(second, first)


def _manifest_input_roots(manifest: dict[str, object]) -> tuple[Path, ...]:
    value = manifest.get("input_roots")
    if not isinstance(value, dict):
        raise PublicationBoundaryError("completed manifest has invalid input_roots")
    roots: list[Path] = []
    for name in ("raw_pbf_root", "wikidata_root", "website_root"):
        root = value.get(name)
        if not isinstance(root, str) or not root:
            raise PublicationBoundaryError("completed manifest has invalid input_roots")
        roots.append(Path(root).resolve())
    return tuple(roots)


def _validate_destination(
    destination: Path,
    run_root: Path,
    manifest: dict[str, object],
) -> Path:
    physical_destination = destination.resolve()
    protected_roots = (run_root.resolve(), *_manifest_input_roots(manifest))
    if any(_overlaps(physical_destination, root) for root in protected_roots):
        raise PublicationBoundaryError("HF staging destination overlaps a protected root")
    return physical_destination


def _parquet_files(directory: Path, description: str) -> tuple[Path, ...]:
    if not directory.is_dir():
        raise PublicationBoundaryError(f"{description} directory is missing: {directory}")
    files = tuple(sorted(directory.glob("*.parquet"), key=lambda path: path.name))
    if not files:
        raise PublicationBoundaryError(f"{description} directory contains no Parquet files")
    return files


def _validate_schema(path: Path, *, is_global: bool) -> None:
    names = pq.read_schema(path).names
    forbidden = _forbidden_fields(names)
    if forbidden:
        raise PublicationBoundaryError(f"{path} contains forbidden fields: {', '.join(forbidden)}")
    if is_global:
        _validate_global_schema(path, names)


def _forbidden_fields(names: list[str]) -> list[str]:
    return sorted(name for name in names if name in _FORBIDDEN_FIELDS)


def _validate_global_schema(path: Path, names: list[str]) -> None:
    if set(names) != set(EXPECTED_GLOBAL_COLUMNS):
        raise PublicationBoundaryError(f"{path} does not match the compact global schema")


def _validate_exact_schema(path: Path, expected: pa.Schema) -> None:
    actual = pq.read_schema(path)
    if actual.names != expected.names or [field.type for field in actual] != [
        field.type for field in expected
    ]:
        raise PublicationBoundaryError(f"{path} does not match its approved Parquet schema")


def _repository_file(name: str) -> Path:
    package_file = Path(__file__).resolve().parents[1] / name
    repository_file = Path(__file__).parents[3] / name
    candidates = (package_file, repository_file)
    path = next((candidate for candidate in candidates if candidate.is_file()), None)
    if path is None:
        raise PublicationBoundaryError(f"repository publication file is missing: {repository_file}")
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _dataset_card(run_id: str) -> str:
    return f"""---
license: odbl
task_categories:
  - tabular-classification
tags:
  - openstreetmap
  - wikidata
  - wikipedia
  - wikivoyage
pretty_name: OSM polygon text coverage
---

# OSM polygon text coverage

This dataset contains compact derived evidence for coverage of the valid raw
OpenStreetMap polygon universe by successful website, Wikipedia, and Wikivoyage
text records. It was generated by run `{run_id}`.

The 64 global Parquet shards contain OSM identity and provenance, including a
distinct contributing-PBF count/list, geometry type and descriptive metrics,
three independent Boolean source flags, and one of eight mutually exclusive
overlap categories. Summary Parquets contain global, per-source-PBF,
per-region, detailed area/type/overlap metrics, geometry-failure, and conflict
statistics.

No raw PBF bytes, full geometry, fetched website text, Wikipedia text,
Wikivoyage text, fetch cache, credential, or private run state is included.

## Licensing

The derived polygon evidence is based on OpenStreetMap data and is available
under the Open Database License (ODbL), with OpenStreetMap attribution. The
analysis code and documentation are Apache-2.0. This artifact republishes no
fetched source text.

See `ATTRIBUTION.md` for limitations and attribution details.
"""


def _attribution() -> str:
    return """# Attribution and limitations

Polygon identities and geometry descriptors are derived from OpenStreetMap
data. OpenStreetMap data is © OpenStreetMap contributors and is available under
the Open Database License (ODbL). See https://www.openstreetmap.org/copyright.

The website, Wikipedia, and Wikivoyage datasets were read as external
membership predicates. Successful-text content is not included in this
dataset. Counts are scoped to the supplied raw PBF inventory, source dataset
versions, geometry validation rules, and run manifest.
"""


def _prepare_destination(destination: Path) -> None:
    if destination.exists() and any(destination.iterdir()):
        raise PublicationBoundaryError("HF staging destination must be empty")
    destination.mkdir(parents=True, exist_ok=True)


def _staging_files(
    run_root: Path,
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    return (
        _parquet_files(run_root / "coverage" / "global", "global coverage"),
        _parquet_files(run_root / "summaries", "summary"),
    )


def _validate_staging_files(
    global_files: tuple[Path, ...], summary_files: tuple[Path, ...]
) -> None:
    _validate_global_staging_files(global_files)
    _validate_summary_staging_files(summary_files)


def _validate_global_staging_files(global_files: tuple[Path, ...]) -> None:
    for path in global_files:
        _validate_schema(path, is_global=True)
        _validate_exact_schema(path, COMPACT_GLOBAL_SCHEMA)
    if {path.name for path in global_files} != _EXPECTED_GLOBAL_FILES:
        raise PublicationBoundaryError(
            "global coverage files must be exactly the 64 approved shards"
        )


def _validate_summary_staging_files(summary_files: tuple[Path, ...]) -> None:
    for path in summary_files:
        _validate_schema(path, is_global=False)
        expected = _EXPECTED_SUMMARY_SCHEMAS.get(path.name)
        if expected is None:
            raise PublicationBoundaryError(f"unexpected summary artifact: {path.name}")
        _validate_exact_schema(path, expected)
    if {path.name for path in summary_files} != set(_EXPECTED_SUMMARY_SCHEMAS):
        raise PublicationBoundaryError("summary files do not match the approved summary set")


def _manifest_inventory(
    payload: dict[str, object], *, field: str, count_field: str, include_row_count: bool
) -> dict[str, dict[str, object]]:
    value = _manifest_list(payload, field, count_field)
    if value is None:
        raise PublicationBoundaryError(f"completed manifest has invalid {field}")
    expected_fields = {"path", "size_bytes", "mtime_ns", "sha256"}
    if include_row_count:
        expected_fields.add("row_count")
    indexed: dict[str, dict[str, object]] = {}
    for item in value:
        entry = _manifest_entry(item, expected_fields, include_row_count)
        if entry is None:
            raise PublicationBoundaryError(f"completed manifest has invalid {field}")
        _add_manifest_entry(indexed, entry, field)
    return indexed


def _manifest_list(payload: dict[str, object], field: str, count_field: str) -> list[object] | None:
    value = payload.get(field)
    if not isinstance(value, list):
        return None
    count = payload.get(count_field)
    if not _validate_nonnegative_int(count):
        return None
    if count != len(value):
        return None
    return value


def _manifest_entry(
    item: object, expected_fields: set[str], include_row_count: bool
) -> dict[str, object] | None:
    if not isinstance(item, dict) or not isinstance(include_row_count, bool):
        return None
    entry = item
    if set(entry) != expected_fields:
        return None
    if not _manifest_entry_values_valid(entry, include_row_count):
        return None
    return entry


def _manifest_entry_values_valid(entry: dict[str, object], include_row_count: bool) -> bool:
    if not isinstance(include_row_count, bool):
        return False
    numeric_fields = frozenset(("size_bytes", "mtime_ns"))
    if include_row_count:
        numeric_fields |= {"row_count"}
    return all(
        (
            isinstance(entry.get("path"), str),
            _validate_count_fields(entry, numeric_fields),
            _valid_sha256(entry.get("sha256")),
        )
    )


def _add_manifest_entry(
    indexed: dict[str, dict[str, object]], entry: dict[str, object], field: str
) -> None:
    path = entry.get("path")
    if not isinstance(path, str):
        raise PublicationBoundaryError(f"completed manifest has invalid {field}")
    if path in indexed:
        raise PublicationBoundaryError(f"completed manifest has invalid {field}")
    indexed[path] = entry


def _valid_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _validate_staged_file(
    path: Path,
    run_root: Path,
    artifact_inventory: dict[str, dict[str, object]],
    parquet_inventory: dict[str, dict[str, object]],
) -> None:
    try:
        relative = str(path.relative_to(run_root))
    except ValueError as exc:
        raise PublicationBoundaryError(f"staged artifact is outside completed run: {path}") from exc
    expected = _required_manifest_entry(artifact_inventory, relative, "staged artifact")
    if not _artifact_matches_manifest(path, expected):
        raise PublicationBoundaryError(
            f"staged artifact differs from completed manifest: {relative}"
        )
    if path.suffix == ".parquet":
        _validate_staged_parquet(path, relative, parquet_inventory)


def _required_manifest_entry(
    inventory: dict[str, dict[str, object]], relative: str, description: str
) -> dict[str, object]:
    expected = inventory.get(relative)
    if expected is None:
        raise PublicationBoundaryError(
            f"{description} is not present in completed manifest: {relative}"
        )
    return expected


def _artifact_matches_manifest(path: Path, expected: dict[str, object]) -> bool:
    try:
        stat = path.stat()
    except OSError:
        return False
    return (
        path.is_file()
        and stat.st_size == expected["size_bytes"]
        and stat.st_mtime_ns == expected["mtime_ns"]
        and _sha256(path) == expected["sha256"]
    )


def _validate_staged_parquet(
    path: Path, relative: str, parquet_inventory: dict[str, dict[str, object]]
) -> None:
    expected = _required_manifest_entry(parquet_inventory, relative, "staged Parquet")
    try:
        metadata = pq.ParquetFile(path).metadata
    except (OSError, ValueError, pa.ArrowException) as exc:
        raise PublicationBoundaryError(f"staged Parquet cannot be validated: {relative}") from exc
    if metadata is None or metadata.num_rows != expected["row_count"]:
        raise PublicationBoundaryError(f"staged Parquet row count differs: {relative}")


def _validate_staging_integrity(
    run_root: Path,
    global_files: tuple[Path, ...],
    summary_files: tuple[Path, ...],
    manifest: dict[str, object],
) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, object]]]:
    artifact_inventory = _manifest_inventory(
        manifest,
        field="generated_artifact_inventory",
        count_field="generated_artifact_count",
        include_row_count=False,
    )
    parquet_inventory = _manifest_inventory(
        manifest,
        field="generated_parquet_inventory",
        count_field="generated_parquet_count",
        include_row_count=True,
    )
    for path in (*global_files, *summary_files):
        _validate_staged_file(path, run_root, artifact_inventory, parquet_inventory)
    return artifact_inventory, parquet_inventory


def _copy_staging_files(
    destination: Path,
    global_files: tuple[Path, ...],
    summary_files: tuple[Path, ...],
) -> None:
    for source, relative_root in (
        (global_files, Path("data/coverage/global")),
        (summary_files, Path("data/summaries")),
    ):
        target_root = destination / relative_root
        target_root.mkdir(parents=True, exist_ok=True)
        for path in source:
            shutil.copy2(path, target_root / path.name)


def _validate_nonnegative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _validate_count_fields(payload: dict[str, object], fields: frozenset[str]) -> bool:
    return all(_validate_nonnegative_int(payload.get(field)) for field in fields)


def _validate_count_mapping(
    payload: dict[str, object],
    field: str,
    expected_keys: frozenset[str],
) -> bool:
    value = payload.get(field)
    return (
        isinstance(value, dict)
        and set(value) == expected_keys
        and all(_validate_nonnegative_int(item) for item in value.values())
    )


def _validate_subset_count_mapping(
    payload: dict[str, object], field: str, expected_keys: frozenset[str]
) -> bool:
    value = payload.get(field)
    return (
        isinstance(value, dict)
        and set(value) <= expected_keys
        and all(_validate_nonnegative_int(item) for item in value.values())
    )


def _validate_overlap_payload(payload: dict[str, object]) -> bool:
    value = payload.get("overlap_categories")
    if not isinstance(value, list) or len(value) != len(EXPECTED_OVERLAP_CATEGORIES):
        return False
    for index, expected_category in enumerate(EXPECTED_OVERLAP_CATEGORIES):
        if not _validate_overlap_item(value[index], expected_category):
            return False
    return True


def _validate_overlap_item(item: object, expected_category: str) -> bool:
    return (
        isinstance(item, dict)
        and set(item) == {"category", "count", "percentage"}
        and item.get("category") == expected_category
        and _validate_nonnegative_int(item.get("count"))
        and _valid_percentage(item.get("percentage"))
    )


def _validate_area_payload(payload: dict[str, object]) -> bool:
    value = payload.get("area_statistics")
    if not isinstance(value, dict):
        return False
    expected = {
        "total_m2",
        "min_m2",
        "max_m2",
        "mean_m2",
        "median_m2",
        "p25_m2",
        "p75_m2",
        "p95_m2",
    }
    return set(value) == expected and all(
        item is None or _valid_number(item) for item in value.values()
    )


def _valid_number(value: object) -> TypeGuard[int | float]:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _valid_percentage(value: object) -> bool:
    if not _valid_number(value):
        return False
    return 0 <= value <= 100


def _validate_summary_payload(payload: object) -> None:
    if not isinstance(payload, dict) or set(payload) != _SUMMARY_KEYS:
        raise PublicationBoundaryError("summary JSON does not match the approved summary schema")
    _validate_summary_counts(payload)
    _validate_summary_mappings(payload)
    _validate_summary_geometry(payload)


def _validate_summary_counts(payload: dict[str, object]) -> None:
    count_fields = frozenset(
        {
            "valid_universe_count",
            "website_count",
            "wikipedia_count",
            "wikivoyage_count",
            "covered_by_any_text_count",
            "geometry_failure_count",
        }
    )
    if not _validate_count_fields(payload, count_fields):
        raise PublicationBoundaryError("summary JSON has invalid count fields")


def _validate_summary_mappings(payload: dict[str, object]) -> None:
    if not _validate_count_mapping(
        payload, "source_keys_not_in_raw", frozenset({"website", "wikipedia", "wikivoyage"})
    ):
        raise PublicationBoundaryError("summary JSON has invalid source-audit fields")
    if not _validate_count_mapping(
        payload,
        "pairwise_intersections",
        frozenset({"website_wikipedia", "website_wikivoyage", "wikipedia_wikivoyage", "all_three"}),
    ):
        raise PublicationBoundaryError("summary JSON has invalid intersection fields")
    if not _validate_subset_count_mapping(
        payload, "osm_type_counts", frozenset({"way", "relation"})
    ):
        raise PublicationBoundaryError("summary JSON has invalid OSM-type fields")
    if not _validate_subset_count_mapping(
        payload,
        "geometry_type_counts",
        frozenset({"Polygon", "MultiPolygon"}),
    ):
        raise PublicationBoundaryError("summary JSON has invalid geometry-type fields")


def _validate_summary_geometry(payload: dict[str, object]) -> None:
    if not _validate_overlap_payload(payload):
        raise PublicationBoundaryError("summary JSON has invalid geometry summary fields")
    if not _validate_area_payload(payload):
        raise PublicationBoundaryError("summary JSON has invalid geometry summary fields")


def _copy_summary_json(
    run_root: Path,
    destination: Path,
    *,
    artifact_inventory: dict[str, dict[str, object]] | None = None,
) -> None:
    summary_json = run_root / "reports" / "summary.json"
    if summary_json.is_file():
        try:
            payload = json.loads(summary_json.read_bytes())
        except (OSError, json.JSONDecodeError) as exc:
            raise PublicationBoundaryError("summary JSON is not valid JSON") from exc
        _validate_summary_payload(payload)
        if artifact_inventory is not None:
            relative = str(summary_json.relative_to(run_root))
            expected = _required_manifest_entry(artifact_inventory, relative, "summary JSON")
            if not _artifact_matches_manifest(summary_json, expected):
                raise PublicationBoundaryError(
                    f"summary JSON differs from completed manifest: {relative}"
                )
        shutil.copy2(summary_json, destination / "data" / "summary.json")


def stage_hf(run_root: Path, destination: Path) -> Path:
    """Stage only compact coverage and summary artifacts for Hugging Face."""

    run_root = run_root.resolve()
    manifest = _check_completed_run(run_root)
    destination = _validate_destination(destination, run_root, manifest)
    global_files, summary_files = _staging_files(run_root)
    _validate_staging_files(global_files, summary_files)
    artifact_inventory, _ = _validate_staging_integrity(
        run_root, global_files, summary_files, manifest
    )
    _prepare_destination(destination)
    _copy_staging_files(destination, global_files, summary_files)
    _copy_summary_json(
        run_root,
        destination,
        artifact_inventory=artifact_inventory,
    )
    run_id = manifest.get("run_id", run_root.name)
    (destination / "README.md").write_text(_dataset_card(str(run_id)), encoding="utf-8")
    shutil.copy2(_repository_file("CITATION.cff"), destination / "CITATION.cff")
    shutil.copy2(_repository_file("LICENSE"), destination / "LICENSE")
    (destination / "ATTRIBUTION.md").write_text(_attribution(), encoding="utf-8")
    return destination
