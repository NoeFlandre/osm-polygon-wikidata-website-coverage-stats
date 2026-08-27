import json
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import pytest

import osm_polygon_wikidata_website_coverage.io.pbf as pbf_module
from osm_polygon_wikidata_website_coverage.domain.geometry import NormalizedGeometry
from osm_polygon_wikidata_website_coverage.domain.identity import (
    GeometryFailure,
    Occurrence,
    OsmIdentity,
)
from osm_polygon_wikidata_website_coverage.io.pbf import (
    PBFReadError,
    _area_identity,
    _area_kind,
    _candidate_kind,
    _classify_candidate,
    _failure,
    _geometry_factory,
    _normalize_area,
    _PolygonHandler,
    _serialize_area,
    _tags,
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
        tags: Sequence[FakeTag | tuple[str, str]],
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


def test_classify_area_ignores_non_way_non_relation_areas_and_supports_tuple_tags() -> None:
    area = FakeArea(
        13,
        from_way=False,
        multipolygon=False,
        tags=[("type", "boundary")],  # type: ignore[list-item]
    )

    assert _tags([("type", "boundary")]) == {"type": "boundary"}
    assert _area_kind(area) is None
    assert (
        classify_area(
            area, source_pbf="fixture-latest.osm.pbf", region="fixture", factory=FakeFactory()
        )
        is None
    )


def test_classify_area_reports_invalid_identity_and_serialization_failures() -> None:
    invalid = FakeArea(
        0,
        from_way=True,
        multipolygon=False,
        tags=[],
    )
    result = classify_area(
        invalid, source_pbf="fixture-latest.osm.pbf", region="fixture", factory=FakeFactory()
    )
    assert isinstance(result, GeometryFailure)
    assert result.failure_kind == "invalid_identity"

    class FailingFactory:
        def create_multipolygon(self, area: FakeArea) -> str:
            raise RuntimeError("serialization failed")

    result = classify_area(
        FakeArea(15, from_way=True, multipolygon=False, tags=[]),
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        factory=FailingFactory(),
    )
    assert isinstance(result, GeometryFailure)
    assert result.failure_kind == "geometry_serialization"


def test_pbf_helpers_preserve_candidate_and_failure_provenance() -> None:
    area = FakeArea(18, from_way=True, multipolygon=False, tags=[])
    identity = OsmIdentity("way", 18)

    assert _candidate_kind("way", None) == "closed_way"
    assert _candidate_kind("relation", "multipolygon") == "multipolygon_relation"
    failure = _failure(
        identity=identity,
        source_pbf="fixture-latest.osm.pbf",
        candidate_kind="closed_way",
        failure_kind="invalid_geometry",
        message="bad shape",
    )
    assert failure == GeometryFailure(
        identity,
        "fixture-latest.osm.pbf",
        "closed_way",
        "invalid_geometry",
        "bad shape",
    )
    assert (
        _area_identity(
            area,
            osm_type="way",
            source_pbf="fixture-latest.osm.pbf",
            candidate_kind="closed_way",
        )
        == identity
    )

    invalid = FakeArea(0, from_way=True, multipolygon=False, tags=[])
    invalid_result = _area_identity(
        invalid,
        osm_type="way",
        source_pbf="fixture-latest.osm.pbf",
        candidate_kind="closed_way",
    )
    assert invalid_result == GeometryFailure(
        None,
        "fixture-latest.osm.pbf",
        "closed_way",
        "invalid_identity",
        "OSM ID must be positive",
    )


def test_pbf_helpers_preserve_serialization_and_normalization_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    area = FakeArea(19, from_way=True, multipolygon=False, tags=[])
    identity = OsmIdentity("way", 19)

    class FailingFactory:
        def create_multipolygon(self, value: FakeArea) -> str:
            assert value is area
            raise RuntimeError("serializer exploded")

    serialized = _serialize_area(
        area,
        factory=FailingFactory(),
        identity=identity,
        source_pbf="fixture-latest.osm.pbf",
        candidate_kind="closed_way",
    )
    assert serialized == GeometryFailure(
        identity,
        "fixture-latest.osm.pbf",
        "closed_way",
        "geometry_serialization",
        "RuntimeError: serializer exploded",
    )

    def failing_normalizer(value: str) -> NormalizedGeometry:
        assert value == GEOMETRY
        raise pbf_module.GeometryError("geometry rejected")

    monkeypatch.setattr(pbf_module, "normalize_geometry", failing_normalizer)
    normalized = _normalize_area(
        GEOMETRY,
        identity=identity,
        source_pbf="fixture-latest.osm.pbf",
        candidate_kind="closed_way",
    )
    assert normalized == GeometryFailure(
        identity,
        "fixture-latest.osm.pbf",
        "closed_way",
        "invalid_geometry",
        "geometry rejected",
    )


def test_pbf_geometry_factory_and_candidate_mapping_are_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = object()
    monkeypatch.setattr(pbf_module.osmium.geom, "GeoJSONFactory", lambda: sentinel)
    assert _geometry_factory(sentinel) is sentinel
    assert _geometry_factory(None) is sentinel

    normalized = NormalizedGeometry(
        geojson=GEOMETRY,
        geometry_type="Polygon",
        centroid_lon=0.5,
        centroid_lat=0.5,
        bbox=(0.0, 0.0, 1.0, 1.0),
        area_m2=12.0,
        area_bucket="under_1e3_m2",
        geometry_hash="b" * 64,
    )
    monkeypatch.setattr(pbf_module, "normalize_geometry", lambda value: normalized)
    result = _classify_candidate(
        FakeArea(20, from_way=True, multipolygon=False, tags=[]),
        osm_type="way",
        relation_type=None,
        candidate_kind="closed_way",
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        factory=FakeFactory(),
    )
    assert result == Occurrence(
        identity=OsmIdentity("way", 20),
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        osm_version=3,
        osm_timestamp="2026-01-01T00:00:00Z",
        relation_kind=None,
        geometry_type="Polygon",
        geometry=GEOMETRY,
        centroid_lon=0.5,
        centroid_lat=0.5,
        bbox_min_lon=0.0,
        bbox_min_lat=0.0,
        bbox_max_lon=1.0,
        bbox_max_lat=1.0,
        area_m2=12.0,
        area_bucket="under_1e3_m2",
        geometry_hash="b" * 64,
    )


def test_polygon_handler_emits_classified_area(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbf_module.osmium.geom, "GeoJSONFactory", lambda: FakeFactory())
    results: list[Occurrence | GeometryFailure] = []
    handler = _PolygonHandler(
        callback=results.append,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
    )

    handler.area(FakeArea(16, from_way=True, multipolygon=False, tags=[]))

    assert len(results) == 1
    assert isinstance(results[0], Occurrence)

    handler.area(FakeArea(17, from_way=False, multipolygon=True, tags=[FakeTag("type", "route")]))
    assert len(results) == 1


def test_polygon_handler_forwards_its_callback_context_and_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = object()
    monkeypatch.setattr(pbf_module.osmium.geom, "GeoJSONFactory", lambda: factory)
    calls: list[dict[str, object]] = []
    result = Occurrence(
        identity=OsmIdentity("way", 21),
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
    )

    def fake_classify(area: object, **kwargs: object) -> Occurrence:
        calls.append({"area": area, **kwargs})
        return result

    monkeypatch.setattr(pbf_module, "classify_area", fake_classify)
    emitted: list[Occurrence | GeometryFailure] = []
    callback = emitted.append
    handler = _PolygonHandler(
        callback=callback,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
    )
    area = FakeArea(21, from_way=True, multipolygon=False, tags=[])
    handler.area(area)

    assert handler._callback is callback
    assert handler._source_pbf == "fixture-latest.osm.pbf"
    assert handler._region == "fixture"
    assert handler._factory is factory
    assert calls == [
        {
            "area": area,
            "source_pbf": "fixture-latest.osm.pbf",
            "region": "fixture",
            "factory": factory,
        }
    ]
    assert emitted == [result]


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


def test_classify_candidate_forwards_all_provenance_to_deep_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    area = FakeArea(22, from_way=True, multipolygon=False, tags=[])
    identity = OsmIdentity("way", 22)
    normalized = NormalizedGeometry(
        geojson=GEOMETRY,
        geometry_type="MultiPolygon",
        centroid_lon=10.0,
        centroid_lat=11.0,
        bbox=(12.0, 13.0, 14.0, 15.0),
        area_m2=16.0,
        area_bucket="1e4_to_1e5_m2",
        geometry_hash="c" * 64,
    )
    calls: list[tuple[str, object]] = []
    factory = object()

    def fake_identity(
        value: object, *, osm_type: str, source_pbf: str, candidate_kind: str
    ) -> OsmIdentity:
        calls.append(("identity", (value, osm_type, source_pbf, candidate_kind)))
        return identity

    def fake_serialize(
        value: object,
        *,
        factory: object,
        identity: OsmIdentity,
        source_pbf: str,
        candidate_kind: str,
    ) -> str:
        calls.append(("serialize", (value, factory, identity, source_pbf, candidate_kind)))
        return "serialized"

    def fake_normalize(
        raw_geometry: str,
        *,
        identity: OsmIdentity,
        source_pbf: str,
        candidate_kind: str,
    ) -> NormalizedGeometry:
        calls.append(("normalize", (raw_geometry, identity, source_pbf, candidate_kind)))
        return normalized

    monkeypatch.setattr(pbf_module, "_area_identity", fake_identity)
    monkeypatch.setattr(pbf_module, "_geometry_factory", lambda value: factory)
    monkeypatch.setattr(pbf_module, "_serialize_area", fake_serialize)
    monkeypatch.setattr(pbf_module, "_normalize_area", fake_normalize)

    result = _classify_candidate(
        area,
        osm_type="way",
        relation_type=None,
        candidate_kind="closed_way",
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        factory=object(),
    )

    assert calls == [
        (
            "identity",
            (area, "way", "fixture-latest.osm.pbf", "closed_way"),
        ),
        (
            "serialize",
            (area, factory, identity, "fixture-latest.osm.pbf", "closed_way"),
        ),
        (
            "normalize",
            ("serialized", identity, "fixture-latest.osm.pbf", "closed_way"),
        ),
    ]
    assert result == Occurrence(
        identity=identity,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        osm_version=3,
        osm_timestamp="2026-01-01T00:00:00Z",
        relation_kind=None,
        geometry_type="MultiPolygon",
        geometry=GEOMETRY,
        centroid_lon=10.0,
        centroid_lat=11.0,
        bbox_min_lon=12.0,
        bbox_min_lat=13.0,
        bbox_max_lon=14.0,
        bbox_max_lat=15.0,
        area_m2=16.0,
        area_bucket="1e4_to_1e5_m2",
        geometry_hash="c" * 64,
    )


def test_classify_area_forwards_candidate_and_context_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    area = FakeArea(23, from_way=True, multipolygon=False, tags=[])
    calls: list[tuple[str, object]] = []
    expected = Occurrence(identity=OsmIdentity("way", 23), source_pbf="fixture.osm.pbf")

    def fake_candidate_kind(osm_type: str, relation_type: str | None) -> str:
        calls.append(("kind", (osm_type, relation_type)))
        return "closed_way"

    def fake_classify(area_value: object, **kwargs: object) -> Occurrence:
        calls.append(("candidate", (area_value, kwargs)))
        return expected

    monkeypatch.setattr(pbf_module, "_candidate_kind", fake_candidate_kind)
    monkeypatch.setattr(pbf_module, "_classify_candidate", fake_classify)

    result = classify_area(
        area,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        factory="factory",
    )

    assert result is expected
    assert calls == [
        ("kind", ("way", None)),
        (
            "candidate",
            (
                area,
                {
                    "osm_type": "way",
                    "relation_type": None,
                    "candidate_kind": "closed_way",
                    "source_pbf": "fixture-latest.osm.pbf",
                    "region": "fixture",
                    "factory": "factory",
                },
            ),
        ),
    ]


def test_classify_candidate_uses_none_for_missing_optional_metadata() -> None:
    class AreaWithoutMetadata:
        id = 24

    result = _classify_candidate(
        AreaWithoutMetadata(),
        osm_type="way",
        relation_type=None,
        candidate_kind="closed_way",
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        factory=FakeFactory(),
    )

    assert isinstance(result, Occurrence)
    assert result.osm_version is None
    assert result.osm_timestamp is None


def test_classify_area_forwards_relation_kind_to_candidate_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    area = FakeArea(25, from_way=False, multipolygon=True, tags=[("type", "boundary")])
    calls: list[tuple[str, object]] = []
    expected = Occurrence(identity=OsmIdentity("relation", 25), source_pbf="fixture.osm.pbf")

    def fake_candidate_kind(osm_type: str, relation_type: str | None) -> str:
        calls.append(("kind", (osm_type, relation_type)))
        return "boundary_relation"

    def fake_classify(area_value: object, **kwargs: object) -> Occurrence:
        calls.append(("candidate", (area_value, kwargs)))
        return expected

    monkeypatch.setattr(pbf_module, "_candidate_kind", fake_candidate_kind)
    monkeypatch.setattr(pbf_module, "_classify_candidate", fake_classify)

    assert (
        classify_area(
            area,
            source_pbf="fixture-latest.osm.pbf",
            region="fixture",
            factory="factory",
        )
        is expected
    )
    assert calls[0] == ("kind", ("relation", "boundary"))
    candidate_kwargs = cast(tuple[object, dict[str, object]], calls[1][1])[1]
    assert candidate_kwargs["relation_type"] == "boundary"
    assert candidate_kwargs["candidate_kind"] == "boundary_relation"


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


def test_scan_pbf_rejects_directories_and_wraps_handler_errors(tmp_path: Path) -> None:
    directory = tmp_path / "directory-latest.osm.pbf"
    directory.mkdir()
    with pytest.raises(PBFReadError, match="not a file"):
        scan_pbf(directory, lambda result: None)

    pbf = tmp_path / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")

    def failing_handler(**kwargs: object) -> object:
        raise RuntimeError("handler failed")

    with pytest.raises(PBFReadError, match="Failed to read PBF"):
        scan_pbf(pbf, lambda result: None, handler_factory=failing_handler)

    invalid_name = tmp_path / "not-geofabrik.pbf"
    invalid_name.write_bytes(b"fixture")
    with pytest.raises(PBFReadError, match="Failed to read PBF"):
        scan_pbf(invalid_name, lambda result: None)


def test_scan_pbf_preserves_explicit_read_errors(tmp_path: Path) -> None:
    pbf = tmp_path / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")

    class ExplicitReadFailure:
        def __init__(self, **kwargs: object) -> None:
            pass

        def apply_file(self, filename: str, *, locations: bool) -> None:
            raise PBFReadError("already classified")

    with pytest.raises(PBFReadError, match="already classified"):
        scan_pbf(pbf, lambda result: None, handler_factory=ExplicitReadFailure)
