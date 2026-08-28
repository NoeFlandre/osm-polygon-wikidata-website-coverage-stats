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
    _finalize_candidate,
    _geometry_factory,
    _identity_from_id,
    _normalize_area,
    _PolygonHandler,
    _relation_kinds,
    _RelationKindHandler,
    _serialize_area,
    _serialize_way,
    _tags,
    _validate_pbf_path,
    classify_area,
    region_from_filename,
    scan_pbf,
    scan_pbf_keys,
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
        self.id = osm_id * 2 + (0 if from_way else 1)
        self._orig_id = osm_id
        self._from_way = from_way
        self._multipolygon = multipolygon
        self.tags = tags
        self.version = 3
        self.timestamp = "2026-01-01T00:00:00Z"

    def from_way(self) -> bool:
        return self._from_way

    def orig_id(self) -> int:
        return self._orig_id

    def is_multipolygon(self) -> bool:
        return self._multipolygon


class FakeFactory:
    def __init__(self, geometry: str = GEOMETRY) -> None:
        self.geometry = geometry

    def create_multipolygon(self, area: FakeArea) -> str:
        return self.geometry


class FakeNode:
    def __init__(self, ref: int, lon: float, lat: float) -> None:
        self.ref = ref
        self.lon = lon
        self.lat = lat


class FakeWay:
    def __init__(self, osm_id: int, nodes: Sequence[FakeNode]) -> None:
        self.id = osm_id
        self.nodes = nodes
        self.version = 4
        self.timestamp = "2026-01-02T00:00:00Z"


class FakeRelation:
    def __init__(self, osm_id: int, tags: Sequence[FakeTag | tuple[str, str]]) -> None:
        self.id = osm_id
        self.tags = tags
        self.version = 7
        self.timestamp = "2026-01-03T00:00:00Z"


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


def test_area_identity_uses_the_original_osm_id_not_encoded_area_id() -> None:
    area = FakeArea(12, from_way=False, multipolygon=False, tags=[])

    result = _area_identity(
        area,
        osm_type="relation",
        source_pbf="fixture-latest.osm.pbf",
        candidate_kind="multipolygon_relation",
    )

    assert result == OsmIdentity("relation", 12)


def test_classify_way_accepts_an_untagged_structurally_closed_way() -> None:
    way = FakeWay(
        31,
        [
            FakeNode(1, 0, 0),
            FakeNode(2, 1, 0),
            FakeNode(3, 1, 1),
            FakeNode(1, 0, 0),
        ],
    )

    result = pbf_module.classify_way(way, source_pbf="fixture-latest.osm.pbf", region="fixture")

    assert isinstance(result, Occurrence)
    assert result.identity == OsmIdentity("way", 31)
    assert result.source_pbf == "fixture-latest.osm.pbf"
    assert result.region == "fixture"
    assert result.osm_version == 4
    assert result.osm_timestamp == "2026-01-02T00:00:00Z"
    assert result.geometry_type == "Polygon"


def test_classify_way_rejects_open_libosmium_ways_before_loading_nodes() -> None:
    class OpenWay:
        id = 31
        version = 1
        timestamp = "2026-01-01T00:00:00Z"

        def is_closed(self) -> bool:
            return False

        @property
        def nodes(self) -> Sequence[FakeNode]:
            raise AssertionError("open way nodes should not be materialized")

    assert (
        pbf_module.classify_way(OpenWay(), source_pbf="fixture.osm.pbf", region="fixture") is None
    )


def test_classify_way_skips_open_ways_and_reports_invalid_or_unlocated_ways() -> None:
    open_way = FakeWay(
        32,
        [FakeNode(1, 0, 0), FakeNode(2, 1, 0), FakeNode(3, 1, 1)],
    )
    assert pbf_module.classify_way(open_way, source_pbf="fixture.osm.pbf", region="fixture") is None

    invalid_way = FakeWay(
        0,
        [FakeNode(1, 0, 0), FakeNode(2, 1, 0), FakeNode(3, 1, 1), FakeNode(1, 0, 0)],
    )
    invalid_result = pbf_module.classify_way(
        invalid_way, source_pbf="fixture.osm.pbf", region="fixture"
    )
    assert isinstance(invalid_result, GeometryFailure)
    assert invalid_result.candidate_kind == "closed_way"
    assert invalid_result.source_pbf == "fixture.osm.pbf"
    assert invalid_result.message == "OSM ID must be positive"
    assert invalid_result.failure_kind == "invalid_identity"

    class BadNode:
        def __init__(self, ref: int) -> None:
            self.ref = ref
            self.lat = 0.0

        @property
        def lon(self) -> float:
            raise RuntimeError("location unavailable")

    bad_way = FakeWay(
        33,
        cast(Sequence[FakeNode], [BadNode(1), BadNode(2), BadNode(3), BadNode(1)]),
    )
    bad_result = pbf_module.classify_way(bad_way, source_pbf="fixture.osm.pbf", region="fixture")
    assert isinstance(bad_result, GeometryFailure)
    assert bad_result.identity == OsmIdentity("way", 33)
    assert bad_result.source_pbf == "fixture.osm.pbf"
    assert bad_result.candidate_kind == "closed_way"
    assert bad_result.failure_kind == "geometry_serialization"
    assert bad_result.message == "RuntimeError: location unavailable"

    class MissingIdWay:
        nodes = bad_way.nodes
        version = 1
        timestamp = "2026-01-03T00:00:00Z"

    missing_id_result = pbf_module.classify_way(
        MissingIdWay(), source_pbf="fixture.osm.pbf", region="fixture"
    )
    assert isinstance(missing_id_result, GeometryFailure)
    assert missing_id_result.failure_kind == "invalid_identity"

    class MissingMetadataWay:
        id = 35
        nodes = invalid_way.nodes

    missing_metadata_result = pbf_module.classify_way(
        MissingMetadataWay(), source_pbf="fixture.osm.pbf", region="fixture"
    )
    assert isinstance(missing_metadata_result, Occurrence)
    assert missing_metadata_result.osm_version is None
    assert missing_metadata_result.osm_timestamp is None


def test_pbf_way_serialization_and_identity_failures_preserve_exact_diagnostics() -> None:
    identity = OsmIdentity("way", 26)
    nodes = (
        FakeNode(1, 1.25, 2.5),
        FakeNode(2, 3.75, 4.0),
        FakeNode(1, 1.25, 2.5),
    )
    assert (
        _serialize_way(
            nodes,
            identity=identity,
            source_pbf="fixture.osm.pbf",
            candidate_kind="closed_way",
        )
        == '{"coordinates":[[[1.25,2.5],[3.75,4.0],[1.25,2.5]]],"type":"Polygon"}'
    )

    class BadNode:
        @property
        def lon(self) -> float:
            raise ValueError("bad longitude")

        lat = 0.0

    serialized = _serialize_way(
        (BadNode(),),
        identity=identity,
        source_pbf="fixture.osm.pbf",
        candidate_kind="closed_way",
    )
    assert serialized == GeometryFailure(
        identity,
        "fixture.osm.pbf",
        "closed_way",
        "geometry_serialization",
        "ValueError: bad longitude",
    )

    invalid = _identity_from_id(
        "not-an-id",
        osm_type="way",
        source_pbf="fixture.osm.pbf",
        candidate_kind="closed_way",
    )
    assert invalid == GeometryFailure(
        None,
        "fixture.osm.pbf",
        "closed_way",
        "invalid_identity",
        "invalid literal for int() with base 10: 'not-an-id'",
    )


def test_classify_way_passes_its_candidate_kind_to_geometry_normalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalized = NormalizedGeometry(
        geojson=GEOMETRY,
        geometry_type="Polygon",
        centroid_lon=0.5,
        centroid_lat=0.5,
        bbox=(0.0, 0.0, 1.0, 1.0),
        area_m2=1.0,
        area_bucket="under_1e3_m2",
        geometry_hash="d" * 64,
    )
    calls: list[str] = []

    def fake_normalize(
        raw_geometry: str,
        *,
        identity: OsmIdentity,
        source_pbf: str,
        candidate_kind: str,
    ) -> NormalizedGeometry:
        del raw_geometry, identity, source_pbf
        calls.append(candidate_kind)
        return normalized

    monkeypatch.setattr(pbf_module, "_normalize_area", fake_normalize)
    way = FakeWay(
        34,
        [
            FakeNode(1, 0, 0),
            FakeNode(2, 1, 0),
            FakeNode(3, 1, 1),
            FakeNode(1, 0, 0),
        ],
    )

    result = pbf_module.classify_way(way, source_pbf="fixture.osm.pbf", region="fixture")

    assert isinstance(result, Occurrence)
    assert calls == ["closed_way"]


def test_relation_kind_collection_accepts_only_supported_polygon_relations(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    relation_kinds: dict[int, str] = {}
    handler = _RelationKindHandler(relation_kinds)
    handler.relation(FakeRelation(1, [("type", "boundary")]))
    handler.relation(FakeRelation(2, [("type", "route")]))
    assert relation_kinds == {1: "boundary"}

    class GoodHandler:
        def __init__(self, target: dict[int, str]) -> None:
            target[3] = "multipolygon"

        def apply_file(self, filename: str) -> None:
            assert filename == str(tmp_path / "fixture.osm.pbf")

    monkeypatch.setattr(pbf_module, "_RelationKindHandler", GoodHandler)
    assert _relation_kinds(tmp_path / "fixture.osm.pbf") == {3: "multipolygon"}

    class FailingHandler:
        def __init__(self, target: dict[int, str]) -> None:
            del target

        def apply_file(self, filename: str) -> None:
            raise RuntimeError("relation pass failed")

    monkeypatch.setattr(pbf_module, "_RelationKindHandler", FailingHandler)
    with pytest.raises(PBFReadError, match="relation pass failed"):
        _relation_kinds(tmp_path / "fixture.osm.pbf")


def test_polygon_handler_classifies_relation_area_with_one_outer_ring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results: list[Occurrence | GeometryFailure] = []
    monkeypatch.setattr(pbf_module.osmium.geom, "GeoJSONFactory", lambda: FakeFactory())
    handler = _PolygonHandler(
        callback=results.append,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
    )

    handler.relation(FakeRelation(32, [("type", "boundary")]))
    handler.area(FakeArea(32, from_way=False, multipolygon=False, tags=[]))

    assert len(results) == 1
    assert isinstance(results[0], Occurrence)
    assert results[0].identity == OsmIdentity("relation", 32)
    assert results[0].relation_kind == "boundary"


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


def test_classify_area_supports_tuple_tags_for_relations() -> None:
    area = FakeArea(
        13,
        from_way=False,
        multipolygon=False,
        tags=[("type", "boundary")],  # type: ignore[list-item]
    )

    assert _tags([("type", "boundary")]) == {"type": "boundary"}
    assert _area_kind(area) == ("relation", "boundary")
    result = classify_area(
        area, source_pbf="fixture-latest.osm.pbf", region="fixture", factory=FakeFactory()
    )
    assert isinstance(result, Occurrence)
    assert result.relation_kind == "boundary"


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

    handler.way(
        FakeWay(
            16,
            [
                FakeNode(1, 0, 0),
                FakeNode(2, 1, 0),
                FakeNode(3, 1, 1),
                FakeNode(1, 0, 0),
            ],
        )
    )

    assert len(results) == 1
    assert isinstance(results[0], Occurrence)

    handler.area(FakeArea(17, from_way=False, multipolygon=True, tags=[FakeTag("type", "route")]))
    assert len(results) == 1


def test_polygon_handler_way_forwards_context_and_only_emits_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    way = object()
    result = Occurrence(identity=OsmIdentity("way", 27), source_pbf="fixture.osm.pbf")
    calls: list[tuple[object, str, str]] = []

    def fake_classify(value: object, *, source_pbf: str, region: str) -> Occurrence:
        calls.append((value, source_pbf, region))
        return result

    monkeypatch.setattr(pbf_module, "classify_way", fake_classify)
    emitted: list[Occurrence | GeometryFailure] = []
    handler = _PolygonHandler(
        callback=emitted.append,
        source_pbf="fixture.osm.pbf",
        region="fixture",
        relation_kinds={},
    )
    handler.way(way)
    assert calls == [(way, "fixture.osm.pbf", "fixture")]
    assert emitted == [result]

    monkeypatch.setattr(pbf_module, "classify_way", lambda *args, **kwargs: None)
    handler.way(way)
    assert emitted == [result]


def test_polygon_handler_skips_non_candidates_and_unclassified_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pbf_module.osmium.geom, "GeoJSONFactory", lambda: FakeFactory())
    monkeypatch.setattr(pbf_module, "classify_area", lambda *args, **kwargs: None)
    results: list[Occurrence | GeometryFailure] = []
    handler = _PolygonHandler(
        callback=results.append,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        relation_kinds={21: "boundary"},
    )
    handler.relation(FakeRelation(21, [("type", "route")]))
    handler.way(FakeWay(21, [FakeNode(1, 0, 0), FakeNode(2, 1, 0), FakeNode(3, 1, 1)]))
    handler.area(FakeArea(20, from_way=True, multipolygon=False, tags=[]))
    handler.area(FakeArea(21, from_way=False, multipolygon=False, tags=[]))
    assert results == []


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
    area = FakeArea(21, from_way=False, multipolygon=False, tags=[])
    handler = _PolygonHandler(
        callback=callback,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        relation_kinds={21: "boundary"},
    )
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
            "relation_type_override": "boundary",
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
        def orig_id(self) -> int:
            return 24

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


def test_finalize_candidate_preserves_normalized_geometry_and_all_provenance() -> None:
    identity = OsmIdentity("relation", 28)
    normalized = pbf_module.normalize_geometry(GEOMETRY)
    result = _finalize_candidate(
        GEOMETRY,
        identity=identity,
        source_pbf="fixture-latest.osm.pbf",
        candidate_kind="boundary_relation",
        region="fixture",
        relation_type="boundary",
        osm_version=7,
        osm_timestamp="2026-01-03T00:00:00Z",
    )

    assert result == Occurrence(
        identity=identity,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
        osm_version=7,
        osm_timestamp="2026-01-03T00:00:00Z",
        relation_kind="boundary",
        geometry_type=normalized.geometry_type,
        geometry=normalized.geojson,
        centroid_lon=normalized.centroid_lon,
        centroid_lat=normalized.centroid_lat,
        bbox_min_lon=normalized.bbox[0],
        bbox_min_lat=normalized.bbox[1],
        bbox_max_lon=normalized.bbox[2],
        bbox_max_lat=normalized.bbox[3],
        area_m2=normalized.area_m2,
        area_bucket=normalized.area_bucket,
        geometry_hash=normalized.geometry_hash,
    )


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


def test_scan_pbf_uses_area_locations_and_streams_callback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pbf = tmp_path / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    calls: list[tuple[str, bool]] = []
    relation_paths: list[Path] = []

    def relation_kinds(path: Path) -> dict[int, str]:
        relation_paths.append(path)
        return {}

    monkeypatch.setattr(pbf_module, "_relation_kinds", relation_kinds)

    class FakeHandler:
        def __init__(
            self, *, callback, source_pbf: str, region: str, relation_kinds: object
        ) -> None:
            assert source_pbf == pbf.name
            assert region == "fixture"
            assert relation_kinds == {}
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
    assert relation_paths == []
    assert len(results) == 1


def test_coverage_handler_emits_structural_way_and_relation_occurrences() -> None:
    results: list[Occurrence | GeometryFailure] = []
    handler = pbf_module._CoverageHandler(
        callback=results.append,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
    )

    handler.way(
        FakeWay(
            41,
            [
                FakeNode(1, 0, 0),
                FakeNode(2, 1, 0),
                FakeNode(3, 1, 1),
                FakeNode(1, 0, 0),
            ],
        )
    )
    handler.way(
        FakeWay(
            0,
            [
                FakeNode(1, 0, 0),
                FakeNode(2, 1, 0),
                FakeNode(3, 1, 1),
                FakeNode(1, 0, 0),
            ],
        )
    )
    handler.relation(FakeRelation(42, [("type", "multipolygon")]))
    handler.relation(FakeRelation(43, [("type", "boundary")]))
    handler.relation(FakeRelation(44, [("type", "route")]))
    handler.relation(FakeRelation(0, [("type", "boundary")]))

    assert [result.identity.key for result in results if isinstance(result, Occurrence)] == [
        ("way", 41),
        ("relation", 42),
        ("relation", 43),
    ]
    assert isinstance(results[1], GeometryFailure)
    assert results[1].failure_kind == "invalid_identity"
    way_result = results[0]
    assert isinstance(way_result, Occurrence)
    assert way_result.source_pbf == "fixture-latest.osm.pbf"
    assert way_result.region == "fixture"
    assert way_result.osm_version == 4
    assert way_result.osm_timestamp == "2026-01-02T00:00:00Z"
    relation_results = [
        result
        for result in results
        if isinstance(result, Occurrence) and result.identity.osm_type == "relation"
    ]
    assert [
        (result.relation_kind, result.osm_version, result.osm_timestamp)
        for result in relation_results
    ] == [
        ("multipolygon", 7, "2026-01-03T00:00:00Z"),
        ("boundary", 7, "2026-01-03T00:00:00Z"),
    ]
    assert all(result.region == "fixture" for result in relation_results)
    assert results[1].source_pbf == "fixture-latest.osm.pbf"
    assert results[1].candidate_kind == "closed_way"
    assert results[1].message == "OSM ID must be positive"
    relation_failure = results[4]
    assert isinstance(relation_failure, GeometryFailure)
    assert relation_failure.candidate_kind == "boundary_relation"
    assert relation_failure.source_pbf == "fixture-latest.osm.pbf"
    assert all(
        isinstance(result, Occurrence)
        and result.geometry is None
        and result.geometry_type is None
        and result.area_m2 is None
        for result in results
        if isinstance(result, Occurrence)
    )

    class MissingIdWay:
        nodes = (
            FakeNode(1, 0, 0),
            FakeNode(2, 1, 0),
            FakeNode(3, 1, 1),
            FakeNode(1, 0, 0),
        )

    class MissingMetadataWay:
        id = 47
        nodes = MissingIdWay.nodes

    handler.way(MissingIdWay())
    handler.way(MissingMetadataWay())
    missing_id_way = results[-2]
    assert isinstance(missing_id_way, GeometryFailure)
    assert missing_id_way.identity is None
    assert missing_id_way.failure_kind == "invalid_identity"
    missing_metadata_way = results[-1]
    assert isinstance(missing_metadata_way, Occurrence)
    assert missing_metadata_way.identity == OsmIdentity("way", 47)
    assert missing_metadata_way.osm_version is None
    assert missing_metadata_way.osm_timestamp is None

    class MissingIdRelation:
        tags = [("type", "boundary")]

    class MissingMetadataRelation:
        id = 48
        tags = [("type", "boundary")]

    handler.relation(MissingIdRelation())
    handler.relation(MissingMetadataRelation())
    missing_id_relation = results[-2]
    assert isinstance(missing_id_relation, GeometryFailure)
    assert missing_id_relation.identity is None
    assert missing_id_relation.candidate_kind == "boundary_relation"
    missing_metadata_relation = results[-1]
    assert isinstance(missing_metadata_relation, Occurrence)
    assert missing_metadata_relation.identity == OsmIdentity("relation", 48)
    assert missing_metadata_relation.osm_version is None
    assert missing_metadata_relation.osm_timestamp is None


def test_coverage_handler_rejects_open_ways_before_materializing_nodes() -> None:
    class OpenWay:
        id = 45

        def is_closed(self) -> bool:
            return False

        @property
        def nodes(self) -> Sequence[FakeNode]:
            raise AssertionError("open way nodes should not be materialized")

    results: list[Occurrence | GeometryFailure] = []
    handler = pbf_module._CoverageHandler(
        callback=results.append,
        source_pbf="fixture-latest.osm.pbf",
        region="fixture",
    )

    handler.way(OpenWay())
    handler.way(FakeWay(46, [FakeNode(1, 0, 0), FakeNode(2, 1, 0)]))

    assert results == []


def test_scan_pbf_keys_uses_locations_false_and_streams_callback(
    tmp_path: Path,
) -> None:
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
            self.callback(Occurrence(identity=OsmIdentity("way", 46), source_pbf=pbf.name))

    results: list[Occurrence | GeometryFailure] = []
    scan_pbf_keys(pbf, results.append, handler_factory=FakeHandler)

    assert calls == [(str(pbf), False)]
    assert len(results) == 1


def test_scan_pbf_keys_wraps_handler_errors(tmp_path: Path) -> None:
    pbf = tmp_path / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")

    class FailingHandler:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def apply_file(self, filename: str, *, locations: bool) -> None:
            del filename, locations
            raise RuntimeError("key handler failed")

    with pytest.raises(PBFReadError, match="Failed to read PBF"):
        scan_pbf_keys(pbf, lambda result: None, handler_factory=FailingHandler)

    class ExplicitReadFailure:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def apply_file(self, filename: str, *, locations: bool) -> None:
            del filename, locations
            raise PBFReadError("already classified")

    with pytest.raises(PBFReadError, match="already classified"):
        scan_pbf_keys(pbf, lambda result: None, handler_factory=ExplicitReadFailure)


def test_validate_pbf_path_reports_missing_and_non_file_inputs(tmp_path: Path) -> None:
    missing = tmp_path / "missing.osm.pbf"
    with pytest.raises(PBFReadError) as missing_error:
        _validate_pbf_path(missing)
    assert str(missing_error.value) == f"PBF file does not exist: {missing}"

    directory = tmp_path / "directory.osm.pbf"
    directory.mkdir()
    with pytest.raises(PBFReadError) as directory_error:
        _validate_pbf_path(directory)
    assert str(directory_error.value) == f"PBF path is not a file: {directory}"


def test_scan_pbf_rejects_missing_input(tmp_path: Path) -> None:
    with pytest.raises(PBFReadError, match="does not exist"):
        scan_pbf(tmp_path / "missing-latest.osm.pbf", lambda result: None)

    directory = tmp_path / "directory-latest.osm.pbf"
    directory.mkdir()
    with pytest.raises(PBFReadError, match="not a file"):
        scan_pbf(directory, lambda result: None)


def test_scan_pbf_rejects_directories_and_wraps_handler_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "directory-latest.osm.pbf"
    directory.mkdir()
    with pytest.raises(PBFReadError, match="not a file"):
        scan_pbf(directory, lambda result: None)

    pbf = tmp_path / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    monkeypatch.setattr(pbf_module, "_relation_kinds", lambda path: {})

    def failing_handler(**kwargs: object) -> object:
        raise RuntimeError("handler failed")

    with pytest.raises(PBFReadError, match="Failed to read PBF"):
        scan_pbf(pbf, lambda result: None, handler_factory=failing_handler)

    invalid_name = tmp_path / "not-geofabrik.pbf"
    invalid_name.write_bytes(b"fixture")
    with pytest.raises(PBFReadError, match="Failed to read PBF"):
        scan_pbf(invalid_name, lambda result: None)


def test_scan_pbf_preserves_explicit_read_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pbf = tmp_path / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    monkeypatch.setattr(pbf_module, "_relation_kinds", lambda path: {})

    class ExplicitReadFailure:
        def __init__(self, **kwargs: object) -> None:
            pass

        def apply_file(self, filename: str, *, locations: bool) -> None:
            raise PBFReadError("already classified")

    with pytest.raises(PBFReadError, match="already classified"):
        scan_pbf(pbf, lambda result: None, handler_factory=ExplicitReadFailure)
