"""Streaming structural polygon-identity extraction backed by libosmium."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import osmium

from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity

ResultCallback = Callable[[OsmIdentity], None]
HandlerFactory = Callable[..., Any]
SUPPORTED_RELATION_TYPES = frozenset(("multipolygon", "boundary"))


class PBFReadError(RuntimeError):
    """Raised when a raw PBF cannot be read."""


def _tag_pair(tag: Any) -> tuple[str, str] | None:
    try:
        if hasattr(tag, "k"):
            return str(tag.k), str(tag.v)
        key, value = tag
        return str(key), str(value)
    except (TypeError, ValueError):
        return None


def _relation_type(tags: Any) -> str | None:
    for tag in tags:
        pair = _tag_pair(tag)
        if pair is not None and pair[0] == "type" and pair[1] in SUPPORTED_RELATION_TYPES:
            return pair[1]
    return None


def _closed_way_sequence(nodes: Any) -> bool:
    refs = tuple(int(node.ref) for node in nodes)
    return len(refs) >= 4 and refs[0] == refs[-1] and len(set(refs[:-1])) >= 3


def _has_closed_ring(nodes: Any) -> bool:
    try:
        iterator = iter(nodes)
        first = int(next(iterator).ref)
        second = int(next(iterator).ref)
        third = int(next(iterator).ref)
    except (StopIteration, TypeError, ValueError, AttributeError):
        return False
    if first in (second, third) or second == third:
        return False
    try:
        return int(nodes[-1].ref) == first
    except (TypeError, IndexError, KeyError, AttributeError, ValueError):
        return True


def _is_structurally_closed(way: Any) -> bool:
    nodes = getattr(way, "nodes", ())
    closed = getattr(way, "is_closed", None)
    if closed is None:
        return _closed_way_sequence(nodes)
    if not closed():
        return False
    return _has_closed_ring(nodes)


def _emit(callback: ResultCallback, osm_type: str, raw_id: Any) -> None:
    try:
        identity = OsmIdentity(osm_type, int(raw_id))
    except (TypeError, ValueError):
        return
    callback(identity)


class _CoverageHandler(osmium.SimpleHandler):
    """libosmium callbacks that emit only raw polygon identities."""

    def __init__(self, *, callback: ResultCallback) -> None:
        super().__init__()
        self._callback = callback

    def way(self, way: Any) -> None:
        if _is_structurally_closed(way):
            _emit(self._callback, "way", getattr(way, "id", None))

    def relation(self, relation: Any) -> None:
        if _relation_type(getattr(relation, "tags", ())):
            _emit(self._callback, "relation", getattr(relation, "id", None))


def _validate_pbf_path(pbf_path: str | Path) -> Path:
    path = Path(pbf_path)
    if not path.exists():
        raise PBFReadError(f"PBF file does not exist: {path}")
    if not path.is_file():
        raise PBFReadError(f"PBF path is not a file: {path}")
    return path


def _build_coverage_handler(
    callback: ResultCallback, handler_factory: HandlerFactory | None
) -> Any:
    handler_type = _CoverageHandler if handler_factory is None else handler_factory
    return handler_type(callback=callback)


def scan_pbf_keys(
    pbf_path: str | Path,
    callback: ResultCallback,
    *,
    handler_factory: HandlerFactory | None = None,
) -> None:
    """Stream raw closed-way and supported-relation identities to ``callback``."""

    path = _validate_pbf_path(pbf_path)
    try:
        handler = _build_coverage_handler(callback, handler_factory)
        handler.apply_file(str(path), locations=False)
    except PBFReadError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise PBFReadError(f"Failed to read PBF {path}: {exc}") from exc
