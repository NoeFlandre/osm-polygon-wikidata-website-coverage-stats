"""Streaming raw-PBF polygon extraction backed by libosmium."""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import osmium
import osmium.geom

from osm_polygon_wikidata_website_coverage.domain.geometry import (
    GeometryError,
    normalize_geometry,
    relation_kind,
)
from osm_polygon_wikidata_website_coverage.domain.identity import (
    GeometryFailure,
    Occurrence,
    OsmIdentity,
)

Result = Occurrence | GeometryFailure
ResultCallback = Callable[[Result], None]
HandlerFactory = Callable[..., Any]

_REGION_RE = re.compile(r"^(?P<region>.+)-latest\.osm\.pbf$")


class PBFReadError(RuntimeError):
    """Raised when a raw PBF cannot be read."""


def region_from_filename(pbf_path: str | Path) -> str:
    """Extract a Geofabrik region slug from a raw PBF filename."""

    name = Path(pbf_path).name
    match = _REGION_RE.match(name)
    if match is None:
        raise ValueError(f"Filename {name!r} does not match the Geofabrik filename pattern")
    return match.group("region")


def _tags(tags: Any) -> dict[str, str]:
    result: dict[str, str] = {}
    for tag in tags:
        if hasattr(tag, "k"):
            result[str(tag.k)] = str(tag.v)
        else:
            key, value = tag
            result[str(key)] = str(value)
    return result


def _area_kind(area: Any) -> tuple[str, str | None] | None:
    if area.is_multipolygon():
        kind = relation_kind(_tags(area.tags))
        return ("relation", kind) if kind is not None else None
    if area.from_way():
        return "way", None
    return None


def _candidate_kind(osm_type: str, relation_type: str | None) -> str:
    if osm_type == "way":
        return "closed_way"
    return f"{relation_type}_relation"


def _failure(
    *,
    identity: OsmIdentity | None,
    source_pbf: str,
    candidate_kind: str,
    failure_kind: str,
    message: str,
) -> GeometryFailure:
    return GeometryFailure(identity, source_pbf, candidate_kind, failure_kind, message)


def classify_area(
    area: Any,
    *,
    source_pbf: str,
    region: str,
    factory: Any | None = None,
) -> Result | None:
    """Convert one assembled libosmium area into an occurrence or failure."""

    kind = _area_kind(area)
    if kind is None:
        return None
    osm_type, relation_type = kind
    candidate_kind = _candidate_kind(osm_type, relation_type)
    try:
        identity = OsmIdentity(osm_type, int(area.id))
    except (TypeError, ValueError) as exc:
        return _failure(
            identity=None,
            source_pbf=source_pbf,
            candidate_kind=candidate_kind,
            failure_kind="invalid_identity",
            message=str(exc),
        )

    geometry_factory = factory if factory is not None else osmium.geom.GeoJSONFactory()
    try:
        raw_geometry = str(geometry_factory.create_multipolygon(area))
    except (RuntimeError, TypeError, ValueError) as exc:
        return _failure(
            identity=identity,
            source_pbf=source_pbf,
            candidate_kind=candidate_kind,
            failure_kind="geometry_serialization",
            message=f"{type(exc).__name__}: {exc}",
        )
    try:
        geometry = normalize_geometry(raw_geometry)
    except GeometryError as exc:
        return _failure(
            identity=identity,
            source_pbf=source_pbf,
            candidate_kind=candidate_kind,
            failure_kind="invalid_geometry",
            message=str(exc),
        )

    return Occurrence(
        identity=identity,
        source_pbf=source_pbf,
        region=region,
        osm_version=getattr(area, "version", None),
        osm_timestamp=getattr(area, "timestamp", None),
        relation_kind=relation_type,
        geometry_type=geometry.geometry_type,
        geometry=geometry.geojson,
        centroid_lon=geometry.centroid_lon,
        centroid_lat=geometry.centroid_lat,
        bbox_min_lon=geometry.bbox[0],
        bbox_min_lat=geometry.bbox[1],
        bbox_max_lon=geometry.bbox[2],
        bbox_max_lat=geometry.bbox[3],
        area_m2=geometry.area_m2,
        area_bucket=geometry.area_bucket,
        geometry_hash=geometry.geometry_hash,
    )


class _PolygonHandler(osmium.SimpleHandler):
    """libosmium area callback that emits results without retaining rows."""

    def __init__(self, *, callback: ResultCallback, source_pbf: str, region: str) -> None:
        super().__init__()
        self._callback = callback
        self._source_pbf = source_pbf
        self._region = region
        self._factory = osmium.geom.GeoJSONFactory()

    def area(self, area: Any) -> None:
        result = classify_area(
            area,
            source_pbf=self._source_pbf,
            region=self._region,
            factory=self._factory,
        )
        if result is not None:
            self._callback(result)


def scan_pbf(
    pbf_path: str | Path,
    callback: ResultCallback,
    *,
    handler_factory: HandlerFactory | None = None,
) -> None:
    """Stream polygon results from one raw PBF into ``callback``."""

    path = Path(pbf_path)
    if not path.exists():
        raise PBFReadError(f"PBF file does not exist: {path}")
    if not path.is_file():
        raise PBFReadError(f"PBF path is not a file: {path}")
    try:
        handler_type = _PolygonHandler if handler_factory is None else handler_factory
        handler = handler_type(
            callback=callback,
            source_pbf=path.name,
            region=region_from_filename(path),
        )
        handler.apply_file(str(path), locations=True)
    except PBFReadError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise PBFReadError(f"Failed to read PBF {path}: {exc}") from exc
