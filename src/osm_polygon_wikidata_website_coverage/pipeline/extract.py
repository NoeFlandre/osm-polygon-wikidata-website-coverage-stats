"""Per-PBF extraction orchestration with fail-closed source checks."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import Occurrence
from osm_polygon_wikidata_website_coverage.io.parquet import (
    FailureShardWriter,
    OccurrenceShardWriter,
)
from osm_polygon_wikidata_website_coverage.io.pbf import Result, scan_pbf, scan_pbf_keys

Scanner = Callable[[Path, Callable[[Result], None]], None]


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

    @classmethod
    def read(cls, path: Path) -> SourceSnapshot:
        try:
            stat = path.stat()
        except OSError as exc:
            raise ExtractionError(f"cannot stat source PBF: {path}") from exc
        return cls(path, stat.st_size, stat.st_mtime_ns)


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


def _pbf_files(raw_root: Path) -> tuple[Path, ...]:
    if not raw_root.is_dir():
        raise ExtractionError(f"raw PBF root is not a directory: {raw_root}")
    files = _regular_pbf_files(raw_root)
    if not files:
        raise ExtractionError(f"raw PBF root contains no regular PBF files: {raw_root}")
    unreadable = _unreadable_pbf_files(files)
    if unreadable:
        names = ", ".join(path.name for path in unreadable)
        raise ExtractionError(f"raw PBF files are unreadable: {names}")
    return files


def _regular_pbf_files(raw_root: Path) -> tuple[Path, ...]:
    return tuple(
        sorted(
            (path for path in raw_root.glob("*.osm.pbf") if path.is_file()),
        )
    )


def _unreadable_pbf_files(files: tuple[Path, ...]) -> tuple[Path, ...]:
    return tuple(path for path in files if not os.access(path, os.R_OK))


def _assert_unchanged(before: SourceSnapshot, after: SourceSnapshot) -> None:
    if before.size_bytes != after.size_bytes or before.mtime_ns != after.mtime_ns:
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
    if not isinstance(value, int) or value < 0:
        return None
    return value


def _checkpoint_counts(occurrence_count: object, failure_count: object) -> tuple[int, int] | None:
    occurrence = _nonnegative_count(occurrence_count)
    failure = _nonnegative_count(failure_count)
    if occurrence is None or failure is None:
        return None
    return occurrence, failure


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


def _write_checkpoint(checkpoint_root: Path, pbf_path: Path, extraction: _SourceExtraction) -> None:
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    checkpoint = _checkpoint_path(checkpoint_root, pbf_path)
    temporary = checkpoint.with_name(f".{checkpoint.name}.tmp")
    if checkpoint.exists() or temporary.exists():
        raise FileExistsError(f"refusing to overwrite extraction checkpoint: {checkpoint}")
    snapshot = extraction.source_inventory.before
    payload = {
        "source_pbf": pbf_path.name,
        "size_bytes": snapshot.size_bytes,
        "mtime_ns": snapshot.mtime_ns,
        "occurrence_count": extraction.occurrence_count,
        "failure_count": extraction.failure_count,
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
    checkpoint_root: Path, run_root: Path, pbf_path: Path
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
    ):
        return None
    source_stem = _source_stem(pbf_path)
    if not _source_output_paths(run_root, "occurrences", source_stem):
        return None
    if not _source_output_paths(run_root, "geometry-failures", source_stem):
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
) -> bool:
    if _checkpoint_counts(occurrence_count, failure_count) is None:
        return False
    if not isinstance(payload, dict):
        return False
    return (
        payload.get("source_pbf") == pbf_path.name
        and payload.get("size_bytes") == current.size_bytes
        and payload.get("mtime_ns") == current.mtime_ns
    )


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
        _write_checkpoint(checkpoint_root, pbf_path, extraction)
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
    pbf_files: tuple[Path, ...], checkpoint_root: Path | None, run_root: Path
) -> tuple[dict[Path, _SourceExtraction], tuple[Path, ...]]:
    completed: dict[Path, _SourceExtraction] = {}
    pending: list[Path] = []
    for pbf_path in pbf_files:
        saved = (
            _read_checkpoint(checkpoint_root, run_root, pbf_path)
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
    completed, pending = _partition_sources(pbf_files, checkpoint_root, run_root)

    extractions = _extract_sources(pending, run_root, batch_rows, scanner, workers, checkpoint_root)
    completed.update(zip(pending, extractions, strict=True))
    ordered_extractions = tuple(completed[pbf_path] for pbf_path in pbf_files)
    return ExtractionResult(
        run_root,
        sum(item.occurrence_count for item in ordered_extractions),
        sum(item.failure_count for item in ordered_extractions),
        tuple(item.source_inventory for item in ordered_extractions),
    )
