"""Per-PBF extraction orchestration with fail-closed source checks."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import TypeGuard

import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import Occurrence
from osm_polygon_wikidata_website_coverage.io.parquet import (
    FAILURE_SCHEMA,
    OCCURRENCE_SCHEMA,
    FailureShardWriter,
    OccurrenceShardWriter,
)
from osm_polygon_wikidata_website_coverage.io.pbf import Result, scan_pbf, scan_pbf_keys

Scanner = Callable[[Path, Callable[[Result], None]], None]
MAX_WORKERS = 8
GEOMETRY_SCANNER_MODE = "geometry"
COVERAGE_ONLY_SCANNER_MODE = "coverage-only"


class ExtractionError(RuntimeError):
    """Raised when extraction cannot start safely."""


class InputChangedError(ExtractionError):
    """Raised when a source file changes while it is being scanned."""


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    """Stable file properties used to detect in-run source mutation."""

    path: Path
    size_bytes: int
    mtime_ns: int
    sha256: str = ""

    @classmethod
    def read(cls, path: Path) -> SourceSnapshot:
        try:
            stat = path.stat()
        except OSError as exc:
            raise ExtractionError(f"cannot stat source PBF: {path}") from exc
        try:
            sha256 = _sha256(path)
        except OSError as exc:
            raise ExtractionError(f"cannot read source PBF: {path}") from exc
        return cls(path, stat.st_size, stat.st_mtime_ns, sha256)


@dataclass(frozen=True, slots=True)
class SourceInventory:
    """Before/after snapshots for one successfully scanned source."""

    before: SourceSnapshot
    after: SourceSnapshot


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """Paths and counts emitted by one extraction stage."""

    run_root: Path
    occurrence_count: int
    failure_count: int
    source_inventory: tuple[SourceInventory, ...]


@dataclass(frozen=True, slots=True)
class _SourceExtraction:
    """Result returned by one independent PBF extraction worker."""

    occurrence_count: int
    failure_count: int
    source_inventory: SourceInventory


def scanner_mode(scanner: Scanner | None) -> str:
    """Return the persisted extraction mode used for checkpoint compatibility."""

    if scanner is scan_pbf_keys:
        return COVERAGE_ONLY_SCANNER_MODE
    return GEOMETRY_SCANNER_MODE


def regular_pbf_files(raw_root: Path) -> tuple[Path, ...]:
    """Return only regular PBF files from a raw input directory."""

    physical_root = raw_root.resolve()
    return tuple(
        sorted(
            path for path in raw_root.glob("*.osm.pbf") if _regular_file_under(path, physical_root)
        )
    )


def _regular_file_under(path: Path, root: Path) -> bool:
    try:
        physical_path = path.resolve()
    except (OSError, RuntimeError):
        return False
    return physical_path.is_file() and (physical_path == root or root in physical_path.parents)


def _pbf_files(raw_root: Path) -> tuple[Path, ...]:
    if not raw_root.is_dir():
        raise ExtractionError(f"raw PBF root is not a directory: {raw_root}")
    files = regular_pbf_files(raw_root)
    if not files:
        raise ExtractionError(f"raw PBF root contains no regular PBF files: {raw_root}")
    unreadable = _unreadable_pbf_files(files)
    if unreadable:
        names = ", ".join(path.name for path in unreadable)
        raise ExtractionError(f"raw PBF files are unreadable: {names}")
    return files


def _unreadable_pbf_files(files: tuple[Path, ...]) -> tuple[Path, ...]:
    return tuple(path for path in files if not os.access(path, os.R_OK))


def _assert_unchanged(before: SourceSnapshot, after: SourceSnapshot) -> None:
    if (
        before.size_bytes != after.size_bytes
        or before.mtime_ns != after.mtime_ns
        or before.sha256 != after.sha256
    ):
        raise InputChangedError(f"source PBF changed during scan: {before.path}")


def _emit_to_writers(
    result: Result,
    occurrence_writer: OccurrenceShardWriter,
    failure_writer: FailureShardWriter,
) -> int:
    if isinstance(result, Occurrence):
        occurrence_writer.write(result)
        return 1
    failure_writer.write(result)
    return 0


def _source_stem(pbf_path: Path) -> str:
    return pbf_path.name.removesuffix(".osm.pbf")


def _checkpoint_path(checkpoint_root: Path, pbf_path: Path) -> Path:
    return checkpoint_root / f"{_source_stem(pbf_path)}.json"


def _source_path_matches(path: Path, prefix: str, suffix: str) -> bool:
    return path.is_file() and path.name.startswith(prefix) and path.suffix == suffix


def _source_paths(run_root: Path, directory: str, prefix: str, suffix: str) -> tuple[Path, ...]:
    root = run_root / directory
    if not root.is_dir():
        return ()
    return tuple(
        sorted(path for path in root.iterdir() if _source_path_matches(path, prefix, suffix))
    )


def _source_output_paths(run_root: Path, directory: str, source_stem: str) -> tuple[Path, ...]:
    return _source_paths(run_root, directory, f"{source_stem}-", ".parquet")


def _source_temporary_paths(run_root: Path, directory: str, source_stem: str) -> tuple[Path, ...]:
    return _source_paths(run_root, directory, f".{source_stem}-", ".tmp")


def _nonnegative_count(value: object) -> int | None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        return None
    return value


def _checkpoint_counts(occurrence_count: object, failure_count: object) -> tuple[int, int] | None:
    occurrence = _nonnegative_count(occurrence_count)
    failure = _nonnegative_count(failure_count)
    if occurrence is None or failure is None:
        return None
    return occurrence, failure


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _checkpoint_shard_inventory(
    run_root: Path, pbf_path: Path, directory: str
) -> list[dict[str, object]]:
    source_stem = _source_stem(pbf_path)
    inventory: list[dict[str, object]] = []
    for path in _source_output_paths(run_root, directory, source_stem):
        metadata = pq.ParquetFile(path).metadata
        if metadata is None or metadata.num_rows < 0:
            raise ExtractionError(f"invalid extracted Parquet metadata: {path}")
        stat = path.stat()
        inventory.append(
            {
                "path": str(path.relative_to(run_root)),
                "size_bytes": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "row_count": metadata.num_rows,
                "sha256": _sha256(path),
            }
        )
    return inventory


def _checkpoint_inventory(payload: dict[str, object], field: str) -> list[dict[str, object]] | None:
    value = payload.get(field)
    if not isinstance(value, list):
        return None
    inventory: list[dict[str, object]] = []
    expected_fields = {"path", "size_bytes", "mtime_ns", "row_count", "sha256"}
    for item in value:
        entry = _checkpoint_entry(item, expected_fields)
        if entry is None:
            return None
        inventory.append(entry)
    return inventory


def _checkpoint_entry(item: object, expected_fields: set[str]) -> dict[str, object] | None:
    entry = _checkpoint_entry_shape(item, expected_fields)
    if entry is None:
        return None
    if not isinstance(entry["path"], str):
        return None
    if not _checkpoint_numbers_valid(entry):
        return None
    if not _valid_sha256(entry["sha256"]):
        return None
    return entry


def _checkpoint_entry_shape(item: object, expected_fields: set[str]) -> dict[str, object] | None:
    if not isinstance(item, dict):
        return None
    entry = item
    if set(entry) != expected_fields:
        return None
    return entry


def _checkpoint_numbers_valid(entry: dict[str, object]) -> bool:
    return all(
        _nonnegative_count(entry[field]) is not None
        for field in ("size_bytes", "mtime_ns", "row_count")
    )


def _valid_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _checkpoint_outputs_match(
    payload: dict[str, object],
    run_root: Path,
    pbf_path: Path,
    occurrence_count: int,
    failure_count: int,
) -> bool:
    source_stem = _source_stem(pbf_path)
    families = (
        ("occurrence_shards", "occurrences", OCCURRENCE_SCHEMA, occurrence_count),
        ("failure_shards", "geometry-failures", FAILURE_SCHEMA, failure_count),
    )
    for field, directory, expected_schema, expected_count in families:
        if not _checkpoint_family_matches(
            payload,
            run_root,
            source_stem,
            field,
            directory,
            expected_schema,
            expected_count,
        ):
            return False
    return True


def _checkpoint_family_matches(
    payload: dict[str, object],
    run_root: Path,
    source_stem: str,
    field: str,
    directory: str,
    expected_schema: pa.Schema,
    expected_count: int,
) -> bool:
    if not _checkpoint_family_ready(payload, run_root, source_stem, field, directory):
        return False
    inventory = _checkpoint_inventory(payload, field)
    if inventory is None:
        return False
    return _checkpoint_files_match(inventory, run_root, directory, expected_schema, expected_count)


def _checkpoint_family_ready(
    payload: dict[str, object],
    run_root: Path,
    source_stem: str,
    field: str,
    directory: str,
) -> bool:
    inventory = _checkpoint_inventory(payload, field)
    if not inventory:
        return False
    actual_paths = _source_output_paths(run_root, directory, source_stem)
    if not _checkpoint_paths_match(actual_paths, inventory, run_root):
        return False
    return not _source_temporary_paths(run_root, directory, source_stem)


def _checkpoint_paths_match(
    actual_paths: tuple[Path, ...], inventory: list[dict[str, object]], run_root: Path
) -> bool:
    return tuple(str(path.relative_to(run_root)) for path in actual_paths) == tuple(
        entry["path"] for entry in inventory
    )


def _checkpoint_files_match(
    inventory: list[dict[str, object]],
    run_root: Path,
    directory: str,
    expected_schema: pa.Schema,
    expected_count: int,
) -> bool:
    row_count = 0
    try:
        for entry in inventory:
            if not _checkpoint_file_matches(entry, run_root, directory, expected_schema):
                return False
            count = _nonnegative_count(entry["row_count"])
            if count is None:
                return False
            row_count += count
    except (OSError, ValueError, pa.ArrowException):
        return False
    return row_count == expected_count


def _checkpoint_file_matches(
    entry: dict[str, object],
    run_root: Path,
    directory: str,
    expected_schema: pa.Schema,
) -> bool:
    path_value = entry.get("path")
    if not isinstance(path_value, str):
        return False
    path = run_root / path_value
    if path.parent != run_root / directory or not path.is_file():
        return False
    if not _checkpoint_file_metadata_matches(path, entry):
        return False
    return _checkpoint_parquet_matches(path, entry, expected_schema)


def _checkpoint_parquet_matches(
    path: Path, entry: dict[str, object], expected_schema: pa.Schema
) -> bool:
    metadata = pq.ParquetFile(path).metadata
    if metadata is None:
        return False
    if metadata.num_rows != entry["row_count"]:
        return False
    return _schema_matches(pq.read_schema(path), expected_schema)


def _checkpoint_file_metadata_matches(path: Path, entry: dict[str, object]) -> bool:
    stat = path.stat()
    return (
        stat.st_size == entry["size_bytes"]
        and stat.st_mtime_ns == entry["mtime_ns"]
        and _sha256(path) == entry["sha256"]
    )


def _schema_matches(actual: pa.Schema, expected: pa.Schema) -> bool:
    return actual.names == expected.names and [field.type for field in actual] == [
        field.type for field in expected
    ]


def _remove_incomplete_outputs(run_root: Path, pbf_path: Path) -> None:
    source_stem = _source_stem(pbf_path)
    for directory in ("occurrences", "geometry-failures"):
        for path in (
            *_source_output_paths(run_root, directory, source_stem),
            *_source_temporary_paths(run_root, directory, source_stem),
        ):
            path.unlink()
    checkpoint = _checkpoint_path(run_root / "checkpoints", pbf_path)
    checkpoint.unlink(missing_ok=True)
    checkpoint.with_name(f".{checkpoint.name}.tmp").unlink(missing_ok=True)


def _write_checkpoint(
    checkpoint_root: Path,
    pbf_path: Path,
    extraction: _SourceExtraction,
    *,
    scanner_mode: str = GEOMETRY_SCANNER_MODE,
) -> None:
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    checkpoint = _checkpoint_path(checkpoint_root, pbf_path)
    temporary = checkpoint.with_name(f".{checkpoint.name}.tmp")
    if checkpoint.exists() or temporary.exists():
        raise FileExistsError(f"refusing to overwrite extraction checkpoint: {checkpoint}")
    snapshot = extraction.source_inventory.before
    run_root = checkpoint_root.parent
    payload = {
        "source_pbf": pbf_path.name,
        "size_bytes": snapshot.size_bytes,
        "mtime_ns": snapshot.mtime_ns,
        "sha256": snapshot.sha256 or _sha256(pbf_path),
        "occurrence_count": extraction.occurrence_count,
        "failure_count": extraction.failure_count,
        "scanner_mode": scanner_mode,
        "occurrence_shards": _checkpoint_shard_inventory(run_root, pbf_path, "occurrences"),
        "failure_shards": _checkpoint_shard_inventory(run_root, pbf_path, "geometry-failures"),
    }
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(temporary, checkpoint)


def _read_checkpoint_fields(
    checkpoint: Path, pbf_path: Path
) -> tuple[object, SourceSnapshot, object, object] | None:
    try:
        payload = json.loads(checkpoint.read_text(encoding="utf-8"))
        current = SourceSnapshot.read(pbf_path)
        return payload, current, payload["occurrence_count"], payload["failure_count"]
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _load_checkpoint_payload(
    checkpoint: Path, temporary: Path, pbf_path: Path
) -> tuple[object, SourceSnapshot, int, int] | None:
    if not checkpoint.is_file() or temporary.exists():
        return None
    loaded = _read_checkpoint_fields(checkpoint, pbf_path)
    if loaded is None:
        return None
    payload, current, raw_occurrence_count, raw_failure_count = loaded
    counts = _checkpoint_counts(raw_occurrence_count, raw_failure_count)
    if counts is None:
        return None
    return payload, current, counts[0], counts[1]


def _read_checkpoint(
    checkpoint_root: Path,
    run_root: Path,
    pbf_path: Path,
    *,
    scanner_mode: str = GEOMETRY_SCANNER_MODE,
) -> _SourceExtraction | None:
    checkpoint = _checkpoint_path(checkpoint_root, pbf_path)
    temporary = checkpoint.with_name(f".{checkpoint.name}.tmp")
    loaded = _load_checkpoint_payload(checkpoint, temporary, pbf_path)
    if loaded is None:
        return None
    payload, current, occurrence_count, failure_count = loaded
    if not _checkpoint_matches(
        payload,
        pbf_path,
        current,
        occurrence_count,
        failure_count,
        scanner_mode=scanner_mode,
    ):
        return None
    checkpoint_payload = payload
    if not _checkpoint_outputs_match(
        checkpoint_payload, run_root, pbf_path, occurrence_count, failure_count
    ):
        return None
    return _SourceExtraction(
        occurrence_count,
        failure_count,
        SourceInventory(current, current),
    )


def _checkpoint_matches(
    payload: object,
    pbf_path: Path,
    current: SourceSnapshot,
    occurrence_count: object,
    failure_count: object,
    *,
    scanner_mode: str = GEOMETRY_SCANNER_MODE,
) -> TypeGuard[dict[str, object]]:
    if _checkpoint_counts(occurrence_count, failure_count) is None:
        return False
    if not isinstance(payload, dict):
        return False
    return _checkpoint_identity_matches(payload, pbf_path, current, scanner_mode)


def _checkpoint_identity_matches(
    payload: dict[object, object],
    pbf_path: Path,
    current: SourceSnapshot,
    scanner_mode: str,
) -> bool:
    if not _checkpoint_source_metadata_matches(payload, pbf_path, current):
        return False
    return payload.get("scanner_mode") == scanner_mode


def _checkpoint_source_metadata_matches(
    payload: dict[object, object], pbf_path: Path, current: SourceSnapshot
) -> bool:
    if payload.get("source_pbf") != pbf_path.name:
        return False
    if payload.get("size_bytes") != current.size_bytes:
        return False
    if payload.get("mtime_ns") != current.mtime_ns:
        return False
    if not _valid_sha256(payload.get("sha256")):
        return False
    return payload.get("sha256") == current.sha256


def _extract_one(
    pbf_path: Path,
    run_root: Path,
    batch_rows: int,
    scanner: Scanner,
    checkpoint_root: Path | None,
) -> _SourceExtraction:
    before = SourceSnapshot.read(pbf_path)
    stem = _source_stem(pbf_path)
    occurrence_count = 0
    failure_count = 0
    with (
        OccurrenceShardWriter(
            run_root / "occurrences", source_stem=stem, batch_rows=batch_rows
        ) as occurrence_writer,
        FailureShardWriter(
            run_root / "geometry-failures", source_stem=stem, batch_rows=batch_rows
        ) as failure_writer,
    ):

        def emit(result: Result) -> None:
            nonlocal occurrence_count, failure_count
            if _emit_to_writers(result, occurrence_writer, failure_writer):
                occurrence_count += 1
            else:
                failure_count += 1

        scanner(pbf_path, emit)
    after = SourceSnapshot.read(pbf_path)
    _assert_unchanged(before, after)
    extraction = _SourceExtraction(occurrence_count, failure_count, SourceInventory(before, after))
    if checkpoint_root is not None:
        _write_checkpoint(
            checkpoint_root,
            pbf_path,
            extraction,
            scanner_mode=scanner_mode(scanner),
        )
    return extraction


def _extract_in_parallel(
    pbf_files: tuple[Path, ...],
    run_root: Path,
    batch_rows: int,
    workers: int,
    scanner: Scanner,
    checkpoint_root: Path | None,
) -> tuple[_SourceExtraction, ...]:
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = tuple(
            executor.submit(
                _extract_one,
                pbf_path,
                run_root,
                batch_rows,
                scanner,
                checkpoint_root,
            )
            for pbf_path in pbf_files
        )
        return tuple(future.result() for future in futures)


def _validate_worker_configuration(workers: int, scanner: Scanner) -> None:
    if workers < 1:
        raise ValueError("workers must be positive")
    if workers > MAX_WORKERS:
        raise ValueError(f"workers must be <= {MAX_WORKERS}")
    if workers > 1 and scanner not in (scan_pbf, scan_pbf_keys):
        raise ExtractionError("parallel extraction requires the default scanner")


def _extract_sources(
    pbf_files: tuple[Path, ...],
    run_root: Path,
    batch_rows: int,
    scanner: Scanner,
    workers: int,
    checkpoint_root: Path | None,
) -> tuple[_SourceExtraction, ...]:
    if workers > 1:
        return _extract_in_parallel(
            pbf_files,
            run_root,
            batch_rows,
            workers,
            scanner,
            checkpoint_root,
        )
    return tuple(
        _extract_one(pbf_path, run_root, batch_rows, scanner, checkpoint_root)
        for pbf_path in pbf_files
    )


def _prepare_run_root(run_root: Path, resume: bool) -> Path | None:
    if run_root.exists() and not resume:
        raise ExtractionError(f"run root already exists: {run_root}")
    (run_root / "occurrences").mkdir(parents=True, exist_ok=True)
    (run_root / "geometry-failures").mkdir(exist_ok=True)
    if not resume:
        return None
    checkpoint_root = run_root / "checkpoints"
    checkpoint_root.mkdir(exist_ok=True)
    return checkpoint_root


def _partition_sources(
    pbf_files: tuple[Path, ...],
    checkpoint_root: Path | None,
    run_root: Path,
    *,
    scanner_mode: str = GEOMETRY_SCANNER_MODE,
) -> tuple[dict[Path, _SourceExtraction], tuple[Path, ...]]:
    completed: dict[Path, _SourceExtraction] = {}
    pending: list[Path] = []
    for pbf_path in pbf_files:
        saved = (
            _read_checkpoint(
                checkpoint_root,
                run_root,
                pbf_path,
                scanner_mode=scanner_mode,
            )
            if checkpoint_root is not None
            else None
        )
        if saved is not None:
            completed[pbf_path] = saved
        else:
            if checkpoint_root is not None:
                _remove_incomplete_outputs(run_root, pbf_path)
            pending.append(pbf_path)
    return completed, tuple(pending)


def extract_all(
    paths: DataPaths,
    run_id: str,
    *,
    scanner: Scanner = scan_pbf,
    batch_rows: int = 5_000,
    workers: int = 1,
    resume: bool = False,
) -> ExtractionResult:
    """Extract all sorted raw PBFs into bounded run-local Parquet shards."""

    _validate_worker_configuration(workers, scanner)
    pbf_files = _pbf_files(paths.raw_pbf_root)
    run_root = paths.run_root(run_id)
    checkpoint_root = _prepare_run_root(run_root, resume)
    completed, pending = _partition_sources(
        pbf_files,
        checkpoint_root,
        run_root,
        scanner_mode=scanner_mode(scanner),
    )

    extractions = _extract_sources(pending, run_root, batch_rows, scanner, workers, checkpoint_root)
    completed.update(zip(pending, extractions, strict=True))
    ordered_extractions = tuple(completed[pbf_path] for pbf_path in pbf_files)
    return ExtractionResult(
        run_root,
        sum(item.occurrence_count for item in ordered_extractions),
        sum(item.failure_count for item in ordered_extractions),
        tuple(item.source_inventory for item in ordered_extractions),
    )
