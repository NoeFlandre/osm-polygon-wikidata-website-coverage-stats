"""Shared safe inventory helpers for immutable source Parquets."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class SourceDatasetError(ValueError):
    """Raised when a source tree is missing or violates its schema contract."""


def read_column_names(path: Path, label: str) -> set[str]:
    """Read a source Parquet schema and translate adapter errors."""

    try:
        return set(pq.read_schema(path).names)
    except (OSError, pa.ArrowException) as exc:
        raise SourceDatasetError(f"cannot read {label} file schema: {path}") from exc


def validate_columns(files: tuple[Path, ...], required: frozenset[str], label: str) -> None:
    """Require every source file to expose the columns used by its query."""

    for path in files:
        missing = required - read_column_names(path, label)
        if missing:
            names = ", ".join(sorted(missing))
            raise SourceDatasetError(f"{label} file {path} is missing columns: {names}")


def _regular_file_under(path: Path, root: Path) -> bool:
    try:
        physical_path = path.resolve()
    except (OSError, RuntimeError):
        return False
    return physical_path.is_file() and (physical_path == root or root in physical_path.parents)


def _invalid_file(files: tuple[Path, ...], root: Path) -> Path | None:
    physical_root = root.resolve()
    return next(
        (path for path in files if not _regular_file_under(path, physical_root)),
        None,
    )


def parquet_files(
    directory: Path,
    description: str,
    invalid_description: str,
) -> tuple[Path, ...]:
    """Return sorted regular Parquets that remain physically below a source root."""

    if not directory.is_dir():
        raise SourceDatasetError(f"{description} directory is missing: {directory}")
    files = tuple(sorted(directory.glob("*.parquet")))
    if not files:
        raise SourceDatasetError(f"{description} directory contains no Parquet files: {directory}")
    invalid = _invalid_file(files, directory)
    if invalid is not None:
        raise SourceDatasetError(f"{invalid_description} file escapes source root: {invalid}")
    return files
