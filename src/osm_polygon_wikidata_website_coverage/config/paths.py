"""Validated paths for immutable source data and Seagate run artifacts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PROJECTS_ROOT = Path("/Volumes/Seagate M3/projects")
DEFAULT_DATA_ROOT = DEFAULT_PROJECTS_ROOT / "osm-polygon-wikidata-website-coverage-stats"
DEFAULT_RAW_PBF_ROOT = DEFAULT_PROJECTS_ROOT / "osm-polygon-wikidata-only/raw"
DEFAULT_WIKIDATA_ROOT = DEFAULT_PROJECTS_ROOT / "osm-polygon-wikidata-only/processed_v2"
DEFAULT_WEBSITE_ROOT = (
    DEFAULT_PROJECTS_ROOT / "osm-polygon-website-tag-data/runs/geofabrik-website-v1"
)


def _absolute(path: Path | str) -> Path:
    """Make a path absolute while preserving symlink spelling."""

    return Path(os.path.abspath(os.fspath(path)))


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _overlaps(first: Path, second: Path) -> bool:
    return _is_within(first, second) or _is_within(second, first)


def _source_roots(
    raw_pbf_root: Path | str, wikidata_root: Path | str, website_root: Path | str
) -> tuple[Path, ...]:
    return tuple(_absolute(path) for path in (raw_pbf_root, wikidata_root, website_root))


def _validate_source_root(output_root: Path, source_root: Path) -> None:
    if not source_root.is_dir():
        raise ValueError(f"source root is not a directory: {source_root}")
    if _overlaps(output_root, source_root):
        raise ValueError(f"source root overlaps output root: {source_root}")


def _validate_run_id(run_id: str) -> None:
    path = Path(run_id)
    if not run_id or path.is_absolute() or len(path.parts) != 1 or path.name in {".", ".."}:
        raise ValueError(f"unsafe run ID: {run_id!r}")


@dataclass(frozen=True, slots=True)
class DataPaths:
    """Read-only source roots and the project-owned output root."""

    data_root: Path
    raw_pbf_root: Path
    wikidata_root: Path
    website_root: Path

    @classmethod
    def from_values(
        cls,
        *,
        data_root: Path | str = DEFAULT_DATA_ROOT,
        raw_pbf_root: Path | str = DEFAULT_RAW_PBF_ROOT,
        wikidata_root: Path | str = DEFAULT_WIKIDATA_ROOT,
        website_root: Path | str = DEFAULT_WEBSITE_ROOT,
    ) -> DataPaths:
        output_root = _absolute(data_root)
        if not _is_within(output_root, DEFAULT_PROJECTS_ROOT):
            raise ValueError("data root must be under the Seagate projects volume")

        source_roots = _source_roots(raw_pbf_root, wikidata_root, website_root)
        for source_root in source_roots:
            _validate_source_root(output_root, source_root)

        return cls(output_root, *source_roots)

    @property
    def source_paths(self) -> tuple[Path, Path, Path]:
        """Return source roots in raw, Wikidata, website order."""

        return (self.raw_pbf_root, self.wikidata_root, self.website_root)

    def run_root(self, run_id: str) -> Path:
        """Return a safe run directory below the configured data root."""

        _validate_run_id(run_id)
        return self.data_root / "runs" / run_id
