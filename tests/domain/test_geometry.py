import json

import pytest

from osm_polygon_wikidata_website_coverage.domain.geometry import (
    GeometryError,
    is_closed_way,
    normalize_geometry,
    relation_kind,
)


def _square() -> list[list[float]]:
    return [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]


def test_normalize_geometry_returns_polygon_metrics() -> None:
    result = normalize_geometry(json.dumps({"type": "Polygon", "coordinates": [_square()]}))

    assert result.geometry_type == "Polygon"
    assert result.area_m2 > 12_000_000_000
    assert result.centroid_lon == pytest.approx(0.5, abs=1e-6)
    assert result.centroid_lat == pytest.approx(0.5, abs=1e-4)
    assert result.bbox == (0.0, 0.0, 1.0, 1.0)
    assert len(result.geometry_hash) == 64
    assert json.loads(result.geojson)["type"] == "Polygon"


def test_normalize_geometry_accepts_multipolygon_and_rounds_coordinates() -> None:
    geometry = {
        "type": "MultiPolygon",
        "coordinates": [
            [
                [
                    [0.123456789, 0.0],
                    [1.0, 0.0],
                    [1.0, 1.0],
                    [0.123456789, 0.0],
                ]
            ]
        ],
    }

    result = normalize_geometry(geometry)

    assert result.geometry_type == "MultiPolygon"
    assert json.loads(result.geojson)["coordinates"][0][0][0][0] == 0.1234568


def test_normalize_geometry_repairs_invalid_polygon_when_result_stays_polygonal() -> None:
    bowtie = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]],
    }

    result = normalize_geometry(bowtie)

    assert result.geometry_type == "Polygon"
    assert result.area_m2 > 0


@pytest.mark.parametrize(
    "geometry",
    [
        {"type": "Point", "coordinates": [0, 0]},
        {"type": "Polygon", "coordinates": []},
        {
            "type": "Polygon",
            "coordinates": [[[179, 0], [-179, 0], [-179, 1], [179, 1], [179, 0]]],
        },
    ],
)
def test_normalize_geometry_rejects_non_polygon_empty_and_antimeridian(geometry: dict) -> None:
    with pytest.raises(GeometryError):
        normalize_geometry(geometry)


def test_closed_way_requires_three_distinct_nodes_and_repeated_first_node() -> None:
    assert is_closed_way((1, 2, 3, 1)) is True
    assert is_closed_way((1, 2, 1)) is False
    assert is_closed_way((1, 2, 3, 4)) is False


def test_relation_candidate_accepts_multipolygon_and_boundary_only() -> None:
    assert relation_kind({"type": "multipolygon"}) == "multipolygon"
    assert relation_kind({"type": "boundary"}) == "boundary"
    assert relation_kind({"type": "route"}) is None
