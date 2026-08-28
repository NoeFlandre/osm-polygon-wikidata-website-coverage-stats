import json
from typing import cast

import pytest
from shapely.geometry import MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry

import osm_polygon_wikidata_website_coverage.domain.geometry as geometry_module
from osm_polygon_wikidata_website_coverage.domain.geometry import (
    GeometryError,
    _area_bucket,
    _centroid,
    _check_polygon_type,
    _latitude_is_unsupported,
    _longitude_is_unsupported,
    _oriented,
    _parse_geometry,
    _polygonal,
    _reject_antimeridian,
    _round_coordinates,
    _validate_repaired,
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


def test_normalize_geometry_rejects_invalid_json_and_latitude() -> None:
    with pytest.raises(GeometryError, match="invalid GeoJSON"):
        normalize_geometry("not-json")
    with pytest.raises(GeometryError, match="latitude"):
        normalize_geometry(
            {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 91], [0, 0]]],
            }
        )


def test_geometry_helpers_cover_non_coordinate_and_degenerate_inputs() -> None:
    assert _round_coordinates({"not": "coordinates"}) == {"not": "coordinates"}
    assert _area_bucket(1.0) == "under_1e3_m2"

    class RepairToPoint:
        geom_type = "Polygon"
        is_empty = False
        is_valid = False
        area = 1.0

        def buffer(self, distance: int) -> Point:
            assert distance == 0
            return Point(0, 0)

    class ZeroAreaPolygon:
        geom_type = "Polygon"
        is_empty = False
        is_valid = True
        area = 0.0

    class RepairToEmpty:
        geom_type = "Polygon"
        is_empty = False
        is_valid = False
        area = 1.0

        def buffer(self, distance: int) -> Point:
            assert distance == 0
            return Point()

    with pytest.raises(GeometryError, match="repair"):
        _polygonal(cast(BaseGeometry, RepairToPoint()))
    with pytest.raises(GeometryError, match="repair"):
        _polygonal(cast(BaseGeometry, RepairToEmpty()))
    with pytest.raises(GeometryError, match="degenerate"):
        _polygonal(cast(BaseGeometry, ZeroAreaPolygon()))


def test_geometry_helpers_preserve_their_error_contracts() -> None:
    class EmptyPolygon:
        geom_type = "Polygon"
        is_empty = True
        is_valid = True
        area = 1.0

    class Line:
        geom_type = "LineString"
        is_empty = False
        is_valid = True
        area = 1.0

    with pytest.raises(GeometryError, match="^geometry is empty$"):
        _polygonal(cast(BaseGeometry, EmptyPolygon()))
    with pytest.raises(GeometryError, match="^geometry must be Polygon or MultiPolygon$"):
        _check_polygon_type(cast(BaseGeometry, Line()))
    with pytest.raises(GeometryError, match="^geometry repair did not remain polygonal$"):
        _validate_repaired(cast(BaseGeometry, EmptyPolygon()))
    with pytest.raises(GeometryError, match="^geometry repair did not remain polygonal$"):
        _validate_repaired(cast(BaseGeometry, Line()))

    with pytest.raises(GeometryError, match="^geometry is degenerate$"):
        _validate_repaired(
            cast(
                BaseGeometry,
                type(
                    "Zero",
                    (),
                    {
                        "geom_type": "Polygon",
                        "is_empty": False,
                        "area": 0.0,
                    },
                )(),
            )
        )


def test_geometry_helpers_cover_boundaries_and_repair_paths() -> None:
    valid = Polygon(_square())
    invalid = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])

    assert _validate_repaired(valid) is valid
    assert geometry_module._repair(valid) is valid
    assert geometry_module._repair(invalid).is_valid is True
    assert _longitude_is_unsupported(-90.0, 90.0) is False
    assert _longitude_is_unsupported(-180.0, 0.0) is False
    assert _longitude_is_unsupported(0.0, 180.0) is False
    assert _longitude_is_unsupported(-180.1, -0.2) is True
    assert _longitude_is_unsupported(0.2, 180.1) is True
    assert _longitude_is_unsupported(-180.5, -0.1) is True
    assert _longitude_is_unsupported(-180.1, 0.0) is True
    assert _longitude_is_unsupported(0.0, 180.1) is True
    assert _longitude_is_unsupported(-100.0, 100.0) is True
    assert _longitude_is_unsupported(-100.0, 81.0) is True
    assert _latitude_is_unsupported(-90.0, 90.0) is False
    assert _latitude_is_unsupported(-90.1, 0.0) is True
    assert _latitude_is_unsupported(0.0, 90.1) is True

    with pytest.raises(GeometryError, match="^antimeridian geometry is not supported$"):
        _reject_antimeridian(Polygon([(179, 0), (-179, 0), (-179, 1), (179, 1), (179, 0)]))
    with pytest.raises(GeometryError, match="^latitude is outside WGS84 bounds$"):
        _reject_antimeridian(Polygon([(0, 91), (1, 91), (1, 92), (0, 91)]))


def test_geometry_orientation_passes_explicit_positive_sign_to_every_polygon(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_orient(value: Polygon, **kwargs: object) -> Polygon:
        calls.append({"geometry": value, **kwargs})
        return value

    monkeypatch.setattr(geometry_module, "orient", fake_orient)
    polygon = Polygon(_square())
    multipolygon = MultiPolygon([polygon])

    assert _oriented(polygon) is polygon
    assert _oriented(multipolygon).geom_type == "MultiPolygon"
    assert calls == [
        {"geometry": polygon, "sign": 1.0},
        {"geometry": polygon, "sign": 1.0},
    ]


def test_geometry_parser_and_centroid_use_local_laea_projection_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(GeometryError, match="^invalid GeoJSON geometry$"):
        _parse_geometry("not-json")

    calls: list[dict[str, object]] = []
    projection_calls: list[dict[str, object]] = []

    class FakeProjection:
        def __init__(self, **kwargs: object) -> None:
            projection_calls.append(kwargs)
            self.kwargs = kwargs

        def __call__(self, x: float, y: float, *, inverse: bool = False) -> tuple[float, float]:
            calls.append({"x": x, "y": y, "inverse": inverse})
            return x, y

    monkeypatch.setattr(geometry_module, "Proj", FakeProjection)
    longitude, latitude = _centroid(Polygon(_square()))

    assert (longitude, latitude) == pytest.approx((0.5, 0.5), abs=1e-6)
    assert projection_calls == [
        {
            "proj": "laea",
            "lat_0": 0.5,
            "lon_0": 0.5,
            "datum": "WGS84",
            "units": "m",
        }
    ]
    assert calls[0] == {
        "x": (0.0, 1.0, 1.0, 0.0, 0.0),
        "y": (0.0, 0.0, 1.0, 1.0, 0.0),
        "inverse": False,
    }
    assert calls[-1]["inverse"] is True


@pytest.mark.parametrize(
    ("area", "label"),
    [
        (0.0, "under_1e3_m2"),
        (999.999, "under_1e3_m2"),
        (1_000.0, "1e3_to_1e4_m2"),
        (10_000.0, "1e4_to_1e5_m2"),
        (100_000.0, "1e5_to_1e6_m2"),
        (1_000_000.0, "1e6_to_1e7_m2"),
        (10_000_000.0, "1e7_to_1e8_m2"),
        (100_000_000.0, "1e8_to_1e9_m2"),
        (1_000_000_000.0, "at_least_1e9_m2"),
    ],
)
def test_area_bucket_uses_exclusive_upper_bounds(area: float, label: str) -> None:
    assert _area_bucket(area) == label


def test_normalize_geometry_rejects_zero_geodesic_area(monkeypatch: pytest.MonkeyPatch) -> None:
    class ZeroGeod:
        def geometry_area_perimeter(self, value: object) -> tuple[float, float]:
            return 0.0, 0.0

    monkeypatch.setattr(geometry_module, "_GEOD", ZeroGeod())

    with pytest.raises(GeometryError, match="^geodesic area is zero$"):
        normalize_geometry({"type": "Polygon", "coordinates": [_square()]})


def test_normalize_geometry_keeps_small_positive_geodesic_area_and_canonical_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SmallGeod:
        def geometry_area_perimeter(self, value: object) -> tuple[float, float]:
            return 0.5, 0.0

    monkeypatch.setattr(geometry_module, "_GEOD", SmallGeod())
    result = normalize_geometry({"type": "Polygon", "coordinates": [_square()]})

    assert result.area_m2 == 0.5
    assert result.area_bucket == "under_1e3_m2"
    assert result.geojson == (
        '{"coordinates":[[[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,1.0],[0.0,0.0]]],"type":"Polygon"}'
    )


def test_normalize_geometry_preserves_json_and_hash_encoding_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dump_calls: list[dict[str, object]] = []
    encodings: list[str] = []

    class EncodableText(str):
        def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
            encodings.append(encoding)
            return super().encode(encoding, errors)

    def fake_dumps(value: object, **kwargs: object) -> EncodableText:
        dump_calls.append(kwargs)
        return EncodableText('{"label":"café"}')

    monkeypatch.setattr(geometry_module.json, "dumps", fake_dumps)
    result = normalize_geometry({"type": "Polygon", "coordinates": [_square()]})

    assert result.geojson == '{"label":"café"}'
    assert dump_calls == [{"ensure_ascii": False, "sort_keys": True, "separators": (",", ":")}]
    assert encodings == ["utf-8"]


def test_closed_way_requires_three_distinct_nodes_and_repeated_first_node() -> None:
    assert is_closed_way((1, 2, 3, 1)) is True
    assert is_closed_way((1, 2, 1)) is False
    assert is_closed_way((1, 2, 3, 4)) is False


def test_relation_candidate_accepts_multipolygon_and_boundary_only() -> None:
    assert relation_kind({"type": "multipolygon"}) == "multipolygon"
    assert relation_kind({"type": "boundary"}) == "boundary"
    assert relation_kind({"type": "route"}) is None
