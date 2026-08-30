"""Resumable per-PBF extraction of the raw polygon identity universe."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from osm_polygon_wikidata_website_coverage.config.paths import DataPaths
from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.io.atomic import write_json
from osm_polygon_wikidata_website_coverage.io.parquet import IDENTITY_SCHEMA, IdentityParquetWriter
from osm_polygon_wikidata_website_coverage.io.pbf import scan_pbf_keys

Scanner = Callable[[Path, Callable[[OsmIdentity], None]], None]
MAX_WORKERS = 8
COVERAGE_ONLY_SCANNER_MODE = "coverage-only"
CUSTOM_SCANNER_MODE = "custom"


class ExtractionError(RuntimeError):
    """Raised when extraction cannot start or resume safely."""


class InputChangedError(ExtractionError):
    """Raised when a source PBF changes while it is being scanned."""


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    """Stable file properties used to detect source mutation."""

    path: Path
    size_bytes: int
    mtime_ns: int
    sha256: str

    @classmethod
    def read(cls, path: Path) -> SourceSnapshot:
        try:
            stat = path.stat()
            digest = _sha256(path)
        except OSError as exc:
            raise ExtractionError(f"cannot read source PBF: {path}") from exc
        return cls(path, stat.st_size, stat.st_mtime_ns, digest)

    @classmethod
    def stat_only(cls, path: Path) -> SourceSnapshot:
        """Read only metadata when a full digest is unnecessary."""

        try:
            stat = path.stat()
        except OSError as exc:
            raise ExtractionError(f"cannot read source PBF: {path}") from exc
        return cls(path, stat.st_size, stat.st_mtime_ns, "")


@dataclass(frozen=True, slots=True)
class SourceInventory:
    """Before/after snapshots for one scanned source."""

    before: SourceSnapshot
    after: SourceSnapshot


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """Rows and source inventories produced by raw identity extraction."""

    run_root: Path
    occurrence_count: int
    source_inventory: tuple[SourceInventory, ...]


@dataclass(frozen=True, slots=True)
class _SourceExtraction:
    row_count: int
    source_inventory: SourceInventory


@dataclass(frozen=True, slots=True)
class _ExtractionContext:
    """Immutable settings shared by each source extraction task."""

    run_root: Path
    batch_rows: int
    scanner: Scanner
    checkpoint_root: Path | None


def scanner_mode(scanner: Scanner) -> str:
    """Return the checkpoint mode for a scanner."""

    return COVERAGE_ONLY_SCANNER_MODE if scanner is scan_pbf_keys else CUSTOM_SCANNER_MODE


def _regular_file_under(path: Path, root: Path) -> bool:
    try:
        physical_path = path.resolve()
    except (OSError, RuntimeError):
        return False
    return physical_path.is_file() and (physical_path == root or root in physical_path.parents)


def regular_pbf_files(raw_root: Path) -> tuple[Path, ...]:
    """Return regular PBFs physically contained by ``raw_root``."""

    physical_root = raw_root.resolve()
    return tuple(
        sorted(
            path for path in raw_root.glob("*.osm.pbf") if _regular_file_under(path, physical_root)
        )
    )


def _pbf_files(raw_root: Path) -> tuple[Path, ...]:
    if not raw_root.is_dir():
        raise ExtractionError(f"raw PBF root is not a directory: {raw_root}")
    files = regular_pbf_files(raw_root)
    if not files:
        raise ExtractionError(f"raw PBF root contains no regular PBF files: {raw_root}")
    _require_readable(files)
    return files


def _require_readable(files: tuple[Path, ...]) -> None:
    unreadable = tuple(path for path in files if not os.access(path, os.R_OK))
    if unreadable:
        names = ", ".join(path.name for path in unreadable)
        raise ExtractionError(f"raw PBF files are unreadable: {names}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_unchanged(before: SourceSnapshot, after: SourceSnapshot) -> None:
    if (
        before.size_bytes != after.size_bytes
        or before.mtime_ns != after.mtime_ns
        or (after.sha256 and before.sha256 != after.sha256)
    ):
        raise InputChangedError(f"source PBF changed during scan: {before.path}")


def _output_path(run_root: Path, pbf_path: Path) -> Path:
    return run_root / "raw-identities" / f"{pbf_path.name.removesuffix('.osm.pbf')}.parquet"


def _checkpoint_path(run_root: Path, pbf_path: Path) -> Path:
    return run_root / "checkpoints" / f"{pbf_path.name.removesuffix('.osm.pbf')}.json"


def _output_metadata_matches(path: Path, expected_count: int, expected: dict[str, Any]) -> bool:
    metadata = pq.ParquetFile(path).metadata
    if metadata is None:
        return False
    if pq.read_schema(path) != IDENTITY_SCHEMA:
        return False
    return metadata.num_rows == expected_count == expected["row_count"]


def _output_file_matches(path: Path, expected: dict[str, Any]) -> bool:
    stat = path.stat()
    return stat.st_size == expected["size_bytes"] and stat.st_mtime_ns == expected["mtime_ns"]


def _output_matches(path: Path, expected_count: int, expected: dict[str, Any]) -> bool:
    try:
        if not _output_metadata_matches(path, expected_count, expected):
            return False
        if not _output_file_matches(path, expected):
            return False
        return _sha256(path) == expected["sha256"]
    except (KeyError, OSError, TypeError, ValueError, pa.ArrowException):
        return False


def _checkpoint_source_matches(
    payload: dict[str, Any], pbf_path: Path, current: SourceSnapshot, scanner: Scanner
) -> bool:
    expected = (
        pbf_path.name,
        scanner_mode(scanner),
        current.size_bytes,
        current.mtime_ns,
        current.sha256,
    )
    actual = (
        payload.get("source_pbf"),
        payload.get("scanner_mode"),
        payload.get("size_bytes"),
        payload.get("mtime_ns"),
        payload.get("sha256"),
    )
    return actual == expected


def _checkpoint_source_snapshot(payload: dict[str, Any], pbf_path: Path) -> SourceSnapshot:
    """Reuse a checkpoint digest when source metadata is unchanged.

    Raw PBFs live in a strict read-only source tree.  Their stored size and
    modification time therefore provide a cheap resume guard; a malformed or
    stale checkpoint falls back to the full digest check.
    """

    current = SourceSnapshot.stat_only(pbf_path)
    saved_sha256 = payload.get("sha256")
    if (
        current.size_bytes == payload.get("size_bytes")
        and current.mtime_ns == payload.get("mtime_ns")
        and isinstance(saved_sha256, str)
        and bool(saved_sha256)
    ):
        return SourceSnapshot(pbf_path, current.size_bytes, current.mtime_ns, saved_sha256)
    return SourceSnapshot.read(pbf_path)


def _checkpoint_count(payload: dict[str, Any]) -> int | None:
    count = payload.get("row_count")
    if not isinstance(count, int):
        return None
    if isinstance(count, bool):
        return None
    if count < 0:
        return None
    return count


def _checkpoint_output_matches(
    run_root: Path, pbf_path: Path, count: int, output: dict[str, Any]
) -> bool:
    expected_path = str(_output_path(run_root, pbf_path).relative_to(run_root))
    if output.get("path") != expected_path:
        return False
    return _output_matches(_output_path(run_root, pbf_path), count, output)


def _checkpoint_count_and_output(
    run_root: Path, pbf_path: Path, payload: dict[str, Any]
) -> int | None:
    count = _checkpoint_count(payload)
    output = payload.get("output")
    if count is None or not isinstance(output, dict):
        return None
    return count if _checkpoint_output_matches(run_root, pbf_path, count, output) else None


def _checkpoint_extraction(
    run_root: Path,
    pbf_path: Path,
    payload: Any,
    current: SourceSnapshot,
    scanner: Scanner,
) -> _SourceExtraction | None:
    if not isinstance(payload, dict):
        return None
    if not _checkpoint_source_matches(payload, pbf_path, current, scanner):
        return None
    count = _checkpoint_count_and_output(run_root, pbf_path, payload)
    if count is None:
        return None
    return _SourceExtraction(count, SourceInventory(current, current))


def _read_checkpoint(
    run_root: Path, pbf_path: Path, *, scanner: Scanner
) -> _SourceExtraction | None:
    checkpoint = _checkpoint_path(run_root, pbf_path)
    try:
        payload = json.loads(checkpoint.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return None
        current = _checkpoint_source_snapshot(payload, pbf_path)
    except (OSError, TypeError, ValueError, ExtractionError):
        return None
    return _checkpoint_extraction(run_root, pbf_path, payload, current, scanner)


def _write_checkpoint(
    run_root: Path,
    pbf_path: Path,
    extraction: _SourceExtraction,
    *,
    scanner: Scanner,
) -> None:
    checkpoint = _checkpoint_path(run_root, pbf_path)
    output = _output_path(run_root, pbf_path)
    stat = output.stat()
    metadata = pq.ParquetFile(output).metadata
    if metadata is None:
        raise ExtractionError(f"invalid extracted Parquet metadata: {output}")
    payload = {
        "source_pbf": pbf_path.name,
        "size_bytes": extraction.source_inventory.before.size_bytes,
        "mtime_ns": extraction.source_inventory.before.mtime_ns,
        "sha256": extraction.source_inventory.before.sha256,
        "row_count": extraction.row_count,
        "scanner_mode": scanner_mode(scanner),
        "output": {
            "path": str(output.relative_to(run_root)),
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "row_count": metadata.num_rows,
            "sha256": _sha256(output),
        },
    }
    write_json(checkpoint, payload)


def _remove_incomplete_outputs(run_root: Path, pbf_path: Path) -> None:
    output = _output_path(run_root, pbf_path)
    output.unlink(missing_ok=True)
    output.with_name(f".{output.name}.tmp").unlink(missing_ok=True)
    checkpoint = _checkpoint_path(run_root, pbf_path)
    checkpoint.unlink(missing_ok=True)
    checkpoint.with_name(f".{checkpoint.name}.tmp").unlink(missing_ok=True)


def _extract_one(
    pbf_path: Path,
    context: _ExtractionContext,
) -> _SourceExtraction:
    before = SourceSnapshot.read(pbf_path)
    output = _output_path(context.run_root, pbf_path)
    count = 0
    with IdentityParquetWriter(
        output.parent,
        filename=output.name,
        batch_rows=context.batch_rows,
    ) as writer:

        def emit(identity: OsmIdentity) -> None:
            nonlocal count
            writer.write(identity)
            count += 1

        context.scanner(pbf_path, emit)
    after = SourceSnapshot.stat_only(pbf_path)
    _assert_unchanged(before, after)
    extraction = _SourceExtraction(count, SourceInventory(before, after))
    if context.checkpoint_root is not None:
        _write_checkpoint(context.run_root, pbf_path, extraction, scanner=context.scanner)
    return extraction


def _validate_worker_configuration(workers: int, scanner: Scanner) -> None:
    if workers < 1:
        raise ValueError("workers must be positive")
    if workers > MAX_WORKERS:
        raise ValueError(f"workers must be <= {MAX_WORKERS}")
    if workers > 1 and scanner is not scan_pbf_keys:
        raise ExtractionError("parallel extraction requires the default scanner")


def _extract_in_parallel(
    pbf_files: tuple[Path, ...],
    workers: int,
    context: _ExtractionContext,
) -> tuple[_SourceExtraction, ...]:
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = tuple(executor.submit(_extract_one, path, context) for path in pbf_files)
        return tuple(future.result() for future in futures)


def _extract_sources(
    pbf_files: tuple[Path, ...],
    workers: int,
    context: _ExtractionContext,
) -> tuple[_SourceExtraction, ...]:
    if workers > 1:
        return _extract_in_parallel(pbf_files, workers, context)
    return tuple(_extract_one(path, context) for path in pbf_files)


def _prepare_run_root(run_root: Path, resume: bool) -> Path | None:
    if run_root.exists() and not resume:
        raise ExtractionError(f"run root already exists: {run_root}")
    (run_root / "raw-identities").mkdir(parents=True, exist_ok=True)
    if not resume:
        return None
    checkpoint_root = run_root / "checkpoints"
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    return checkpoint_root


def _partition_sources(
    pbf_files: tuple[Path, ...],
    run_root: Path,
    checkpoint_root: Path | None,
    scanner: Scanner,
) -> tuple[dict[Path, _SourceExtraction], tuple[Path, ...]]:
    completed: dict[Path, _SourceExtraction] = {}
    pending: list[Path] = []
    for path in pbf_files:
        saved = (
            _read_checkpoint(run_root, path, scanner=scanner)
            if checkpoint_root is not None
            else None
        )
        if saved is None:
            if checkpoint_root is not None:
                _remove_incomplete_outputs(run_root, path)
            pending.append(path)
        else:
            completed[path] = saved
    return completed, tuple(pending)


def extract_all(
    paths: DataPaths,
    run_id: str,
    *,
    scanner: Scanner = scan_pbf_keys,
    batch_rows: int = 100_000,
    workers: int = 1,
    resume: bool = False,
) -> ExtractionResult:
    """Extract all regular raw PBFs into one file per source PBF."""

    _validate_worker_configuration(workers, scanner)
    pbf_files = _pbf_files(paths.raw_pbf_root)
    run_root = paths.run_root(run_id)
    checkpoint_root = _prepare_run_root(run_root, resume)
    completed, pending = _partition_sources(pbf_files, run_root, checkpoint_root, scanner)
    context = _ExtractionContext(run_root, batch_rows, scanner, checkpoint_root)
    extracted = _extract_sources(pending, workers, context)
    completed.update(zip(pending, extracted, strict=True))
    ordered = tuple(completed[path] for path in pbf_files)
    return ExtractionResult(
        run_root,
        sum(item.row_count for item in ordered),
        tuple(item.source_inventory for item in ordered),
    )
