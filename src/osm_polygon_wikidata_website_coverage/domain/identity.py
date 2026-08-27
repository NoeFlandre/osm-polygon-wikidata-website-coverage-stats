"""Stable OSM identities and occurrence provenance."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

PolygonOsmType = str


@dataclass(frozen=True, slots=True)
class OsmIdentity:
    """The globally stable identity of a way or relation."""

    osm_type: PolygonOsmType
    osm_id: int

    def __post_init__(self) -> None:
        if self.osm_type not in {"way", "relation"}:
            raise ValueError("OSM identity must be a way or relation")
        if self.osm_id <= 0:
            raise ValueError("OSM ID must be positive")

    @property
    def key(self) -> tuple[str, int]:
        return self.osm_type, self.osm_id


@dataclass(frozen=True, slots=True)
class Occurrence:
    """One valid geometry occurrence from one raw PBF."""

    identity: OsmIdentity
    source_pbf: str
    region: str = ""
    osm_version: int | None = None
    osm_timestamp: str | datetime | None = None
    relation_kind: str | None = None
    geometry_type: str | None = None
    geometry: str | None = None
    centroid_lon: float | None = None
    centroid_lat: float | None = None
    bbox_min_lon: float | None = None
    bbox_min_lat: float | None = None
    bbox_max_lon: float | None = None
    bbox_max_lat: float | None = None
    area_m2: float | None = None
    area_bucket: str | None = None
    geometry_hash: str | None = None

    def __post_init__(self) -> None:
        if not self.source_pbf:
            raise ValueError("source PBF name cannot be empty")

    @property
    def timestamp_value(self) -> datetime:
        """Return a timezone-aware timestamp for deterministic ordering."""

        if self.osm_timestamp is None:
            return datetime.min.replace(tzinfo=UTC)
        if isinstance(self.osm_timestamp, datetime):
            value = self.osm_timestamp
        else:
            value = datetime.fromisoformat(self.osm_timestamp.replace("Z", "+00:00"))
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class GeometryFailure:
    """An area candidate that could not produce a valid polygon geometry."""

    identity: OsmIdentity | None
    source_pbf: str
    candidate_kind: str
    failure_kind: str
    message: str


def canonical_occurrence(occurrences: tuple[Occurrence, ...] | list[Occurrence]) -> Occurrence:
    """Choose the highest-version, newest, then lexicographically first occurrence."""

    if not occurrences:
        raise ValueError("at least one occurrence is required")
    highest_version = max(
        occurrence.osm_version if occurrence.osm_version is not None else -1
        for occurrence in occurrences
    )
    version_matches = tuple(
        occurrence
        for occurrence in occurrences
        if (occurrence.osm_version if occurrence.osm_version is not None else -1) == highest_version
    )
    newest_timestamp = max(occurrence.timestamp_value for occurrence in version_matches)
    timestamp_matches = tuple(
        occurrence
        for occurrence in version_matches
        if occurrence.timestamp_value == newest_timestamp
    )
    return min(timestamp_matches, key=lambda occurrence: occurrence.source_pbf)
