import json
from pathlib import Path

import pytest

from osm_polygon_wikidata_website_coverage.domain.identity import (
    GeometryFailure,
    Occurrence,
    OsmIdentity,
)
from osm_polygon_wikidata_website_coverage.io.pbf import (
    PBFReadError,
    classify_area,
    region_from_filename,
    scan_pbf,
)

GEOMETRY = json.dumps(
    {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
    }
)


class FakeTag:
    def __init__(self, key: str, value: str) -> None:
        self.k = key
        self.v = value


class FakeArea:
    def __init__(
        self,
        osm_id: int,
        *,
        from_way: bool,
        multipolygon: bool,
        tags: list[FakeTag],
    ) -> None:
        self.id = osm_id
        self._from_way = from_way
        self._multipolygon = multipolygon
        self.tags = tags
        self.version = 3
        self.timestamp = "2026-01-01T00:00:00Z"

    def from_way(self) -> bool:
        return self._from_way

    def is_multipolygon(self) -> bool:
        return self._multipolygon


class FakeFactory:
    def __init__(self, geometry: str = GEOMETRY) -> None:
        self.geometry = geometry

    def create_multipolygon(self, area: FakeArea) -> str:
        return self.geometry


def test_classify_area_accepts_closed_way_without_content_tags() -> None:
    area = FakeArea(
        11,
        from_way=True,
        multipolygon=False,
        tags=[FakeTag("name", "Unrelated")],
    )

    result = classify_area(
        area, source_pbf="fixture-latest.osm.pbf", region="fixture", factory=FakeFactory()
    )

    assert isinstance(result, Occurrence)
    assert result.identity.key == ("way", 11)
    assert result.relation_kind is None
    assert result.geometry_type == "Polygon"


def test_classify_area_accepts_multipolygon_and_boundary_relations() -> None:
    area = FakeArea(
        12,
        from_way=False,
        multipolygon=True,
        tags=[FakeTag("type", "boundary")],
    )

    result = classify_area(
        area, source_pbf="fixture-latest.osm.pbf", region="fixture", factory=FakeFactory()
    )

    assert isinstance(result, Occurrence)
    assert result.identity.key == ("relation", 12)
    assert result.relation_kind == "boundary"


def test_classify_area_ignores_unsupported_relations() -> None:
    area = FakeArea(
        13,
        from_way=False,
        multipolygon=True,
        tags=[FakeTag("type", "route")],
    )

    assert (
        classify_area(
            area, source_pbf="fixture-latest.osm.pbf", region="fixture", factory=FakeFactory()
        )
        is None
    )


def test_classify_area_returns_geometry_failure_for_invalid_geometry() -> None:
    area = FakeArea(
        14,
        from_way=True,
        multipolygon=False,
        tags=[],
    )

    result = classify_area(
        area,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        factory=FakeFactory('{"type":"Point","coordinates":[0,0]}'),
    )

    assert isinstance(result, GeometryFailure)
    assert result.failure_kind == "invalid_geometry"
    assert result.identity is not None
    assert result.identity.key == ("way", 14)


def test_region_from_filename_requires_geofabrik_shape() -> None:
    assert region_from_filename("france-latest.osm.pbf") == "france"
    with pytest.raises(ValueError, match="Geofabrik"):
        region_from_filename("france.osm.pbf")


def test_scan_pbf_uses_area_locations_and_streams_callback(tmp_path: Path) -> None:
    pbf = tmp_path / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    calls: list[tuple[str, bool]] = []

    class FakeHandler:
        def __init__(self, *, callback, source_pbf: str, region: str) -> None:
            assert source_pbf == pbf.name
            assert region == "fixture"
            self.callback = callback

        def apply_file(self, filename: str, *, locations: bool) -> None:
            calls.append((filename, locations))
            self.callback(
                Occurrence(
                    identity=OsmIdentity("way", 1),
                    source_pbf=pbf.name,
                    region="fixture",
                )
            )

    results: list[Occurrence | GeometryFailure] = []
    scan_pbf(pbf, results.append, handler_factory=FakeHandler)

    assert calls == [(str(pbf), True)]
    assert len(results) == 1


def test_scan_pbf_rejects_missing_input(tmp_path: Path) -> None:
    with pytest.raises(PBFReadError, match="does not exist"):
        scan_pbf(tmp_path / "missing-latest.osm.pbf", lambda result: None)
