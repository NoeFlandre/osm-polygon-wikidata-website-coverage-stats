"""Canonical polygon geometry and geodesic metrics."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pyproj import CRS, Geod, Transformer
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from shapely.geometry.base import BaseGeometry
from shapely.geometry.polygon import orient
from shapely.ops import transform


class GeometryError(ValueError):
    """Raised when an input cannot be represented as a valid polygon."""


@dataclass(frozen=True, slots=True)
class NormalizedGeometry:
    """Canonical geometry and stable descriptive metrics."""

    geojson: str
    geometry_type: str
    centroid_lon: float
    centroid_lat: float
    bbox: tuple[float, float, float, float]
    area_m2: float
    area_bucket: str
    geometry_hash: str


_GEOD = Geod(ellps="WGS84")
_POLYGON_TYPES = ("Polygon", "MultiPolygon")
_AREA_BUCKETS = (
    (1_000, "under_1e3_m2"),
    (10_000, "1e3_to_1e4_m2"),
    (100_000, "1e4_to_1e5_m2"),
    (1_000_000, "1e5_to_1e6_m2"),
    (10_000_000, "1e6_to_1e7_m2"),
    (100_000_000, "1e7_to_1e8_m2"),
    (1_000_000_000, "1e8_to_1e9_m2"),
)


def is_closed_way(node_refs: Sequence[int]) -> bool:
    """Return whether an OSM way has at least three distinct closed nodes."""

    refs = tuple(node_refs)
    return len(refs) >= 4 and refs[0] == refs[-1] and len(set(refs[:-1])) >= 3


def relation_kind(tags: Mapping[str, Any]) -> str | None:
    """Return the supported polygon relation type, if present."""

    value = tags.get("type")
    return value if value in {"multipolygon", "boundary"} else None


def _round_coordinates(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return round(float(value), 7)
    if isinstance(value, (list, tuple)):
        return [_round_coordinates(item) for item in value]
    return value


def _rounded_geojson(geometry: BaseGeometry) -> dict[str, Any]:
    result = mapping(geometry)
    return {
        "type": result["type"],
        "coordinates": _round_coordinates(result["coordinates"]),
    }


def _polygonal(geometry: BaseGeometry) -> BaseGeometry:
    if geometry.geom_type not in _POLYGON_TYPES:
        raise GeometryError("geometry must be Polygon or MultiPolygon")
    if geometry.is_empty:
        raise GeometryError("geometry is empty")
    if not geometry.is_valid:
        geometry = geometry.buffer(0)
    if geometry.is_empty or geometry.geom_type not in _POLYGON_TYPES:
        raise GeometryError("geometry repair did not remain polygonal")
    if geometry.area <= 0:
        raise GeometryError("geometry is degenerate")
    return geometry


def _oriented(geometry: BaseGeometry) -> BaseGeometry:
    if isinstance(geometry, Polygon):
        return orient(geometry, sign=1.0)
    return MultiPolygon(tuple(orient(polygon, sign=1.0) for polygon in geometry.geoms))


def _parse_geometry(value: str | Mapping[str, Any]) -> BaseGeometry:
    try:
        payload = json.loads(value) if isinstance(value, str) else dict(value)
        geometry = shape(payload)
    except (TypeError, ValueError, KeyError) as exc:
        raise GeometryError("invalid GeoJSON geometry") from exc
    return _polygonal(geometry)


def _reject_antimeridian(geometry: BaseGeometry) -> None:
    min_lon, _, max_lon, _ = geometry.bounds
    if min_lon < -180 or max_lon > 180 or max_lon - min_lon > 180:
        raise GeometryError("antimeridian geometry is not supported")
    _, min_lat, _, max_lat = geometry.bounds
    if min_lat < -90 or max_lat > 90:
        raise GeometryError("latitude is outside WGS84 bounds")


def _centroid(geometry: BaseGeometry) -> tuple[float, float]:
    seed = geometry.centroid
    local_crs = CRS.from_proj4(
        f"+proj=laea +lat_0={seed.y} +lon_0={seed.x} +datum=WGS84 +units=m +no_defs"
    )
    forward = Transformer.from_crs("EPSG:4326", local_crs, always_xy=True)
    inverse = Transformer.from_crs(local_crs, "EPSG:4326", always_xy=True)
    projected = transform(forward.transform, geometry)
    projected_centroid = projected.centroid
    longitude, latitude = inverse.transform(projected_centroid.x, projected_centroid.y)
    return float(longitude), float(latitude)


def _area_bucket(area_m2: float) -> str:
    for upper_bound, label in _AREA_BUCKETS:
        if area_m2 < upper_bound:
            return label
    return "at_least_1e9_m2"


def normalize_geometry(value: str | Mapping[str, Any]) -> NormalizedGeometry:
    """Normalize polygon GeoJSON and calculate stable WGS84 metrics."""

    geometry = _parse_geometry(value)
    _reject_antimeridian(geometry)
    rounded = shape(_rounded_geojson(_oriented(geometry)))
    rounded = _oriented(_polygonal(rounded))
    _reject_antimeridian(rounded)
    canonical = _rounded_geojson(rounded)
    geojson = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    area_m2 = abs(_GEOD.geometry_area_perimeter(rounded)[0])
    if area_m2 <= 0:
        raise GeometryError("geodesic area is zero")
    centroid_lon, centroid_lat = _centroid(rounded)
    min_lon, min_lat, max_lon, max_lat = rounded.bounds
    return NormalizedGeometry(
        geojson=geojson,
        geometry_type=rounded.geom_type,
        centroid_lon=centroid_lon,
        centroid_lat=centroid_lat,
        bbox=(float(min_lon), float(min_lat), float(max_lon), float(max_lat)),
        area_m2=float(area_m2),
        area_bucket=_area_bucket(float(area_m2)),
        geometry_hash=hashlib.sha256(geojson.encode("utf-8")).hexdigest(),
    )
