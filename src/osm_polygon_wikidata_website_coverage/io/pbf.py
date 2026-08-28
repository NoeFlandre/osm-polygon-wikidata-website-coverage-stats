"""Streaming raw-PBF polygon extraction backed by libosmium."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import osmium
import osmium.geom

from osm_polygon_wikidata_website_coverage.domain.geometry import (
    GeometryError,
    NormalizedGeometry,
    is_closed_way,
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
    if area.from_way():
        return "way", None
    kind = relation_kind(_tags(area.tags))
    return ("relation", kind) if kind is not None else None


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


def _area_identity(
    area: Any, *, osm_type: str, source_pbf: str, candidate_kind: str
) -> OsmIdentity | GeometryFailure:
    return _identity_from_id(
        area.orig_id(),
        osm_type=osm_type,
        source_pbf=source_pbf,
        candidate_kind=candidate_kind,
    )


def _identity_from_id(
    raw_id: Any, *, osm_type: str, source_pbf: str, candidate_kind: str
) -> OsmIdentity | GeometryFailure:
    try:
        return OsmIdentity(osm_type, int(raw_id))
    except (TypeError, ValueError) as exc:
        return _failure(
            identity=None,
            source_pbf=source_pbf,
            candidate_kind=candidate_kind,
            failure_kind="invalid_identity",
            message=str(exc),
        )


def _structural_occurrence(
    raw_id: Any,
    *,
    osm_type: str,
    source_pbf: str,
    region: str,
    relation_type: str | None,
    osm_version: Any,
    osm_timestamp: Any,
) -> Result:
    candidate_kind = _candidate_kind(osm_type, relation_type)
    identity_result = _identity_from_id(
        raw_id,
        osm_type=osm_type,
        source_pbf=source_pbf,
        candidate_kind=candidate_kind,
    )
    if isinstance(identity_result, GeometryFailure):
        return identity_result
    return Occurrence(
        identity=identity_result,
        source_pbf=source_pbf,
        region=region,
        osm_version=osm_version,
        osm_timestamp=osm_timestamp,
        relation_kind=relation_type,
    )


def _serialize_area(
    area: Any,
    *,
    factory: Any,
    identity: OsmIdentity,
    source_pbf: str,
    candidate_kind: str,
) -> str | GeometryFailure:
    try:
        return str(factory.create_multipolygon(area))
    except (RuntimeError, TypeError, ValueError) as exc:
        return _failure(
            identity=identity,
            source_pbf=source_pbf,
            candidate_kind=candidate_kind,
            failure_kind="geometry_serialization",
            message=f"{type(exc).__name__}: {exc}",
        )


def _normalize_area(
    raw_geometry: str,
    *,
    identity: OsmIdentity,
    source_pbf: str,
    candidate_kind: str,
) -> NormalizedGeometry | GeometryFailure:
    try:
        return normalize_geometry(raw_geometry)
    except GeometryError as exc:
        return _failure(
            identity=identity,
            source_pbf=source_pbf,
            candidate_kind=candidate_kind,
            failure_kind="invalid_geometry",
            message=str(exc),
        )


def _serialize_way(
    nodes: tuple[Any, ...],
    *,
    identity: OsmIdentity,
    source_pbf: str,
    candidate_kind: str,
) -> str | GeometryFailure:
    try:
        coordinates = [[float(node.lon), float(node.lat)] for node in nodes]
        return json.dumps(
            {"type": "Polygon", "coordinates": [coordinates]},
            sort_keys=True,
            separators=(",", ":"),
        )
    except (osmium.InvalidLocationError, RuntimeError, TypeError, ValueError) as exc:
        return _failure(
            identity=identity,
            source_pbf=source_pbf,
            candidate_kind=candidate_kind,
            failure_kind="geometry_serialization",
            message=f"{type(exc).__name__}: {exc}",
        )


def _geometry_factory(factory: Any | None) -> Any:
    return factory if factory is not None else osmium.geom.GeoJSONFactory()


def _finalize_candidate(
    raw_geometry: str,
    *,
    identity: OsmIdentity,
    source_pbf: str,
    candidate_kind: str,
    region: str,
    relation_type: str | None,
    osm_version: Any,
    osm_timestamp: Any,
) -> Result:
    geometry_result = _normalize_area(
        raw_geometry,
        identity=identity,
        source_pbf=source_pbf,
        candidate_kind=candidate_kind,
    )
    if isinstance(geometry_result, GeometryFailure):
        return geometry_result
    geometry = geometry_result
    return Occurrence(
        identity=identity,
        source_pbf=source_pbf,
        region=region,
        osm_version=osm_version,
        osm_timestamp=osm_timestamp,
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


def _classify_candidate(
    area: Any,
    *,
    osm_type: str,
    relation_type: str | None,
    candidate_kind: str,
    source_pbf: str,
    region: str,
    factory: Any | None,
) -> Result:
    identity_result = _area_identity(
        area,
        osm_type=osm_type,
        source_pbf=source_pbf,
        candidate_kind=candidate_kind,
    )
    if isinstance(identity_result, GeometryFailure):
        return identity_result
    identity = identity_result
    serialized = _serialize_area(
        area,
        factory=_geometry_factory(factory),
        identity=identity,
        source_pbf=source_pbf,
        candidate_kind=candidate_kind,
    )
    if isinstance(serialized, GeometryFailure):
        return serialized
    return _finalize_candidate(
        serialized,
        identity=identity,
        source_pbf=source_pbf,
        candidate_kind=candidate_kind,
        region=region,
        relation_type=relation_type,
        osm_version=getattr(area, "version", None),
        osm_timestamp=getattr(area, "timestamp", None),
    )


def _closed_way_nodes(way: Any) -> tuple[Any, ...] | None:
    is_closed = getattr(way, "is_closed", None)
    if is_closed is not None and not is_closed():
        return None
    nodes = tuple(way.nodes)
    return nodes if is_closed_way(tuple(int(node.ref) for node in nodes)) else None


def classify_way(way: Any, *, source_pbf: str, region: str) -> Result | None:
    """Convert a structurally closed raw way into an occurrence or failure."""

    nodes = _closed_way_nodes(way)
    if nodes is None:
        return None
    candidate_kind = "closed_way"
    identity_result = _identity_from_id(
        getattr(way, "id", None),
        osm_type="way",
        source_pbf=source_pbf,
        candidate_kind=candidate_kind,
    )
    if isinstance(identity_result, GeometryFailure):
        return identity_result
    identity = identity_result
    serialized = _serialize_way(
        nodes,
        identity=identity,
        source_pbf=source_pbf,
        candidate_kind=candidate_kind,
    )
    if isinstance(serialized, GeometryFailure):
        return serialized
    return _finalize_candidate(
        serialized,
        identity=identity,
        source_pbf=source_pbf,
        candidate_kind=candidate_kind,
        region=region,
        relation_type=None,
        osm_version=getattr(way, "version", None),
        osm_timestamp=getattr(way, "timestamp", None),
    )


def classify_area(
    area: Any,
    *,
    source_pbf: str,
    region: str,
    factory: Any | None = None,
    relation_type_override: str | None = None,
) -> Result | None:
    """Convert one assembled libosmium area into an occurrence or failure."""

    kind = _area_kind(area)
    if relation_type_override is not None and not area.from_way():
        kind = ("relation", relation_type_override)
    if kind is None:
        return None
    osm_type, relation_type = kind
    candidate_kind = _candidate_kind(osm_type, relation_type)
    return _classify_candidate(
        area,
        osm_type=osm_type,
        relation_type=relation_type,
        candidate_kind=candidate_kind,
        source_pbf=source_pbf,
        region=region,
        factory=factory,
    )


class _RelationKindHandler(osmium.SimpleHandler):
    """Collect relation types before the area assembler emits relation areas."""

    def __init__(self, relation_kinds: dict[int, str]) -> None:
        super().__init__()
        self._relation_kinds = relation_kinds

    def relation(self, relation: Any) -> None:
        kind = relation_kind(_tags(relation.tags))
        if kind is not None:
            self._relation_kinds[int(relation.id)] = kind


def _relation_kinds(pbf_path: Path) -> dict[int, str]:
    relation_kinds: dict[int, str] = {}
    try:
        _RelationKindHandler(relation_kinds).apply_file(str(pbf_path))
    except (OSError, RuntimeError, ValueError) as exc:
        raise PBFReadError(f"Failed to read PBF {pbf_path}: {exc}") from exc
    return relation_kinds


class _PolygonHandler(osmium.SimpleHandler):
    """libosmium area callback that emits results without retaining rows."""

    def __init__(
        self,
        *,
        callback: ResultCallback,
        source_pbf: str,
        region: str,
        relation_kinds: Mapping[int, str] | None = None,
    ) -> None:
        super().__init__()
        self._callback = callback
        self._source_pbf = source_pbf
        self._region = region
        self._relation_kinds = dict(relation_kinds or {})
        self._factory = osmium.geom.GeoJSONFactory()

    def way(self, way: Any) -> None:
        result = classify_way(way, source_pbf=self._source_pbf, region=self._region)
        if result is not None:
            self._callback(result)

    def relation(self, relation: Any) -> None:
        kind = relation_kind(_tags(relation.tags))
        if kind is not None:
            self._relation_kinds[int(relation.id)] = kind

    def area(self, area: Any) -> None:
        if area.from_way():
            return
        relation_type = self._relation_kinds.get(int(area.orig_id()))
        if relation_type is None:
            return
        result = classify_area(
            area,
            source_pbf=self._source_pbf,
            region=self._region,
            factory=self._factory,
            relation_type_override=relation_type,
        )
        if result is not None:
            self._callback(result)


class _CoverageHandler(osmium.SimpleHandler):
    """libosmium handler for the identity-only coverage scan."""

    def __init__(self, *, callback: ResultCallback, source_pbf: str, region: str) -> None:
        super().__init__()
        self._callback = callback
        self._source_pbf = source_pbf
        self._region = region

    def way(self, way: Any) -> None:
        is_closed = getattr(way, "is_closed", None)
        if is_closed is not None and not is_closed():
            return
        node_refs = tuple(int(node.ref) for node in way.nodes)
        if not is_closed_way(node_refs):
            return
        self._callback(
            _structural_occurrence(
                getattr(way, "id", None),
                osm_type="way",
                source_pbf=self._source_pbf,
                region=self._region,
                relation_type=None,
                osm_version=getattr(way, "version", None),
                osm_timestamp=getattr(way, "timestamp", None),
            )
        )

    def relation(self, relation: Any) -> None:
        kind = relation_kind(_tags(relation.tags))
        if kind is None:
            return
        self._callback(
            _structural_occurrence(
                getattr(relation, "id", None),
                osm_type="relation",
                source_pbf=self._source_pbf,
                region=self._region,
                relation_type=kind,
                osm_version=getattr(relation, "version", None),
                osm_timestamp=getattr(relation, "timestamp", None),
            )
        )


def _validate_pbf_path(pbf_path: str | Path) -> Path:
    path = Path(pbf_path)
    if not path.exists():
        raise PBFReadError(f"PBF file does not exist: {path}")
    if not path.is_file():
        raise PBFReadError(f"PBF path is not a file: {path}")
    return path


def _build_coverage_handler(
    path: Path,
    callback: ResultCallback,
    handler_factory: HandlerFactory | None,
) -> Any:
    handler_type = _CoverageHandler if handler_factory is None else handler_factory
    return handler_type(
        callback=callback,
        source_pbf=path.name,
        region=region_from_filename(path),
    )


def scan_pbf_keys(
    pbf_path: str | Path,
    callback: ResultCallback,
    *,
    handler_factory: HandlerFactory | None = None,
) -> None:
    """Stream polygon identities without assembling or serializing geometry."""

    path = _validate_pbf_path(pbf_path)
    try:
        handler = _build_coverage_handler(path, callback, handler_factory)
        handler.apply_file(str(path), locations=False)
    except PBFReadError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise PBFReadError(f"Failed to read PBF {path}: {exc}") from exc


def scan_pbf(
    pbf_path: str | Path,
    callback: ResultCallback,
    *,
    handler_factory: HandlerFactory | None = None,
) -> None:
    """Stream polygon results from one raw PBF into ``callback``."""

    path = _validate_pbf_path(pbf_path)
    try:
        handler = _build_handler(path, callback, handler_factory, {})
        handler.apply_file(str(path), locations=True)
    except PBFReadError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise PBFReadError(f"Failed to read PBF {path}: {exc}") from exc


def _build_handler(
    path: Path,
    callback: ResultCallback,
    handler_factory: HandlerFactory | None,
    relation_kinds: Mapping[int, str],
) -> Any:
    handler_type = _PolygonHandler if handler_factory is None else handler_factory
    return handler_type(
        callback=callback,
        source_pbf=path.name,
        region=region_from_filename(path),
        relation_kinds=relation_kinds,
    )
