from pathlib import Path
from typing import Any, cast

import osmium.osm.mutable
import pytest

import osm_polygon_wikidata_website_coverage.io.pbf as pbf_module
from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.io.pbf import PBFReadError, scan_pbf_keys


class Node:
    def __init__(self, ref: int) -> None:
        self.ref = ref


class Way:
    def __init__(self, refs: list[int], closed: bool | None) -> None:
        self.id = 11
        self.nodes: Any = [Node(ref) for ref in refs]
        if closed is not None:
            self._closed = closed

    def is_closed(self) -> bool:
        return self._closed


class Relation:
    id = 22

    def __init__(self, tags: Any) -> None:
        self.tags = tags


def test_handler_emits_only_structurally_closed_ways_and_supported_relations() -> None:
    results: list[OsmIdentity] = []
    handler = pbf_module._CoverageHandler(callback=results.append)

    handler.way(Way([1, 2, 3, 1], True))
    handler.way(Way([1, 2, 3], False))
    handler.way(Way([1, 2, 1], True))
    handler.relation(Relation([("type", "multipolygon")]))
    handler.relation(Relation([("type", "route")]))

    assert results == [OsmIdentity("way", 11), OsmIdentity("relation", 22)]


def test_closed_way_fast_path_stops_after_three_distinct_nodes() -> None:
    class CountingNodes:
        def __init__(self) -> None:
            self.seen = 0

        def __len__(self) -> int:
            return 6

        def __getitem__(self, index: int) -> Node:
            return Node((1, 2, 3, 4, 5, 1)[index])

        def __iter__(self):
            for ref in (1, 2, 3, 4, 5, 1):
                self.seen += 1
                yield Node(ref)

    nodes = CountingNodes()
    way = Way([], True)
    way.nodes = nodes

    assert pbf_module._is_structurally_closed(way) is True
    assert nodes.seen == 3


@pytest.mark.parametrize(
    ("refs", "expected"),
    [
        ((1, 2, 3, 1), True),
        ((1, 2, 3, 4, 1), True),
        ((1, 2, 3), False),
        ((1, 2, 1), False),
        ((1, 2, 3, 2), False),
        ((1, 2, 2, 1), False),
    ],
)
def test_closed_way_sequence_requires_a_simple_closed_ring(
    refs: tuple[int, ...], expected: bool
) -> None:
    assert pbf_module._closed_way_sequence([Node(ref) for ref in refs]) is expected


def test_closed_way_fallback_without_is_closed_preserves_exact_predicate() -> None:
    class NoClosedFlag:
        id = 1
        nodes = [Node(1), Node(2), Node(1)]

    assert pbf_module._is_structurally_closed(NoClosedFlag()) is False


def test_structural_helpers_reject_malformed_or_open_rings() -> None:
    assert pbf_module._has_closed_ring([]) is False
    assert pbf_module._has_closed_ring([object()]) is False
    assert pbf_module._is_structurally_closed(Way([1, 2, 3, 4], True)) is False
    assert pbf_module._relation_type([object(), ("type", "route")]) is None
    assert pbf_module._tag_pair(["only-one-value"]) is None
    assert pbf_module._emit(lambda identity: None, "way", None) is None

    class IterableOnly:
        def __iter__(self):
            return iter([Node(1), Node(2), Node(3)])

    assert pbf_module._has_closed_ring(IterableOnly()) is True


def test_structural_way_defaults_missing_nodes_to_an_empty_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[object] = []

    class MissingNodes:
        def is_closed(self) -> bool:
            return True

    def has_closed_ring(nodes: object) -> bool:
        seen.append(nodes)
        return True

    monkeypatch.setattr(pbf_module, "_has_closed_ring", has_closed_ring)

    assert pbf_module._is_structurally_closed(MissingNodes()) is True
    assert seen == [()]


def test_relation_type_reads_object_and_tuple_tags() -> None:
    class Tag:
        k = "type"
        v = "boundary"

    assert pbf_module._relation_type([Tag()]) == "boundary"
    assert pbf_module._relation_type([("type", "multipolygon")]) == "multipolygon"
    assert pbf_module._relation_type([("name", "ignored")]) is None


def test_handler_ignores_invalid_way_and_relation_ids() -> None:
    results: list[OsmIdentity] = []
    handler = pbf_module._CoverageHandler(callback=results.append)

    valid_way = Way([1, 2, 3, 1], True)
    valid_way.id = cast(Any, "bad")
    handler.way(valid_way)
    invalid_relation = Relation([("type", "boundary")])
    invalid_relation.id = cast(Any, object())
    handler.relation(invalid_relation)

    assert results == []


def test_handler_ignores_missing_way_id_and_relation_tags() -> None:
    results: list[OsmIdentity] = []
    handler = pbf_module._CoverageHandler(callback=results.append)

    class MissingWayId:
        nodes = [Node(1), Node(2), Node(3), Node(1)]

        def is_closed(self) -> bool:
            return True

    class MissingRelationTags:
        id = 1

    class MissingRelationId:
        tags = [("type", "boundary")]

    handler.way(MissingWayId())
    handler.relation(MissingRelationTags())
    handler.relation(MissingRelationId())

    assert results == []


def test_scan_pbf_keys_reads_polygons_without_node_locations(tmp_path: Path) -> None:
    pbf = tmp_path / "fixture.osm.pbf"
    with osmium.SimpleWriter(pbf) as writer:
        writer.add_way(osmium.osm.mutable.Way(id=11, nodes=[1, 2, 3, 1]))
        writer.add_way(osmium.osm.mutable.Way(id=12, nodes=[1, 2, 3]))
        writer.add_relation(osmium.osm.mutable.Relation(id=22, tags={"type": "multipolygon"}))
        writer.add_relation(osmium.osm.mutable.Relation(id=23, tags={"type": "route"}))
    results: list[OsmIdentity] = []

    scan_pbf_keys(pbf, results.append)

    assert results == [OsmIdentity("way", 11), OsmIdentity("relation", 22)]


def test_scan_pbf_keys_uses_locations_false_and_validates_paths(tmp_path: Path) -> None:
    pbf = tmp_path / "fixture.osm.pbf"
    pbf.write_bytes(b"fixture")
    calls: list[tuple[str, bool]] = []
    results: list[OsmIdentity] = []

    class Handler:
        def __init__(self, *, callback: pbf_module.ResultCallback) -> None:
            self.callback = callback

        def apply_file(self, filename: str, *, locations: bool) -> None:
            calls.append((filename, locations))
            self.callback(OsmIdentity("way", 11))

    scan_pbf_keys(pbf, results.append, handler_factory=Handler)

    assert calls == [(str(pbf), False)]
    assert results == [OsmIdentity("way", 11)]
    with pytest.raises(PBFReadError, match="does not exist"):
        scan_pbf_keys(tmp_path / "missing.osm.pbf", lambda _: None)
    directory = tmp_path / "directory.osm.pbf"
    directory.mkdir()
    with pytest.raises(PBFReadError, match="not a file"):
        scan_pbf_keys(directory, lambda _: None)

    def broken_factory(*, callback: pbf_module.ResultCallback) -> None:
        raise OSError("bad")

    with pytest.raises(PBFReadError, match="Failed to read PBF"):
        scan_pbf_keys(pbf, results.append, handler_factory=broken_factory)


def test_scan_pbf_keys_wraps_reader_errors(tmp_path: Path) -> None:
    pbf = tmp_path / "fixture.osm.pbf"
    pbf.write_bytes(b"fixture")

    class Handler:
        def __init__(self, *, callback: object) -> None:
            del callback

        def apply_file(self, filename: str, *, locations: bool) -> None:
            del filename, locations
            raise RuntimeError("broken")

    with pytest.raises(PBFReadError, match="broken"):
        scan_pbf_keys(pbf, lambda _: None, handler_factory=Handler)


def test_scan_pbf_keys_preserves_existing_reader_error(tmp_path: Path) -> None:
    pbf = tmp_path / "fixture.osm.pbf"
    pbf.write_bytes(b"fixture")

    class Handler:
        def __init__(self, *, callback: object) -> None:
            del callback

        def apply_file(self, filename: str, *, locations: bool) -> None:
            del filename, locations
            raise PBFReadError("already wrapped")

    with pytest.raises(PBFReadError, match="already wrapped"):
        scan_pbf_keys(pbf, lambda _: None, handler_factory=Handler)
