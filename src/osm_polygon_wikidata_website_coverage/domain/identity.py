"""Stable OSM identities used by the overlap calculation."""

from __future__ import annotations

from dataclasses import dataclass

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
