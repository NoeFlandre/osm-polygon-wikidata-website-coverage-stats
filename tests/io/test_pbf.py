from pathlib import Path
from typing import Any

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


def test_relation_type_reads_object_and_tuple_tags() -> None:
    class Tag:
        k = "type"
        v = "boundary"

    assert pbf_module._relation_type([Tag()]) == "boundary"
    assert pbf_module._relation_type([("type", "multipolygon")]) == "multipolygon"
    assert pbf_module._relation_type([("name", "ignored")]) is None


def test_scan_pbf_keys_uses_locations_false_and_validates_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pbf = tmp_path / "fixture.osm.pbf"
    pbf.write_bytes(b"fixture")
    calls: list[tuple[str, bool]] = []

    class Handler:
        def __init__(self, *, callback: object) -> None:
            del callback

        def apply_file(self, filename: str, *, locations: bool) -> None:
            calls.append((filename, locations))

    scan_pbf_keys(pbf, lambda _: None, handler_factory=Handler)

    assert calls == [(str(pbf), False)]
    with pytest.raises(PBFReadError, match="does not exist"):
        scan_pbf_keys(tmp_path / "missing.osm.pbf", lambda _: None)
    directory = tmp_path / "directory.osm.pbf"
    directory.mkdir()
    with pytest.raises(PBFReadError, match="not a file"):
        scan_pbf_keys(directory, lambda _: None)
    monkeypatch.setattr(
        pbf_module,
        "_build_coverage_handler",
        lambda *args: (_ for _ in ()).throw(OSError("bad")),
    )
    with pytest.raises(PBFReadError, match="Failed to read PBF"):
        scan_pbf_keys(pbf, lambda _: None)


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
