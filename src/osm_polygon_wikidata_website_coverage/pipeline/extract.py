"""Per-PBF extraction orchestration with fail-closed source checks."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import Occurrence
from osm_polygon_wikidata_website_coverage.io.parquet import (
    FailureShardWriter,
    OccurrenceShardWriter,
)
from osm_polygon_wikidata_website_coverage.io.pbf import Result, scan_pbf

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


def extract_all(
    paths: DataPaths,
    run_id: str,
    *,
    scanner: Scanner = scan_pbf,
    batch_rows: int = 5_000,
) -> ExtractionResult:
    """Extract all sorted raw PBFs into bounded run-local Parquet shards."""

    pbf_files = _pbf_files(paths.raw_pbf_root)
    run_root = paths.run_root(run_id)
    if run_root.exists():
        raise ExtractionError(f"run root already exists: {run_root}")
    (run_root / "occurrences").mkdir(parents=True)
    (run_root / "geometry-failures").mkdir()

    inventories: list[SourceInventory] = []
    occurrence_count = 0
    failure_count = 0
    for pbf_path in pbf_files:
        before = SourceSnapshot.read(pbf_path)
        stem = pbf_path.name.removesuffix(".osm.pbf")
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
        inventories.append(SourceInventory(before, after))

    return ExtractionResult(run_root, occurrence_count, failure_count, tuple(inventories))
