from datetime import datetime

import pytest

from osm_polygon_wikidata_website_coverage.domain.identity import (
    Occurrence,
    OsmIdentity,
    canonical_occurrence,
)


def _occurrence(
    source_pbf: str,
    *,
    osm_version: int | None,
    osm_timestamp: str | None,
) -> Occurrence:
    return Occurrence(
        identity=OsmIdentity("way", 7),
        source_pbf=source_pbf,
        region="fixture",
        osm_version=osm_version,
        osm_timestamp=osm_timestamp,
    )


def test_canonical_occurrence_prefers_version_then_timestamp_then_pbf() -> None:
    older = _occurrence(
        "z.osm.pbf",
        osm_version=3,
        osm_timestamp="2026-01-02T00:00:00Z",
    )
    newer_version = _occurrence(
        "z.osm.pbf",
        osm_version=4,
        osm_timestamp="2025-01-01T00:00:00Z",
    )
    newest_tie = _occurrence(
        "a.osm.pbf",
        osm_version=4,
        osm_timestamp="2025-01-01T00:00:00Z",
    )

    assert canonical_occurrence((older, newer_version, newest_tie)) == newest_tie


def test_canonical_occurrence_parses_timestamp_before_comparing() -> None:
    early = _occurrence(
        "early.osm.pbf",
        osm_version=1,
        osm_timestamp="2026-01-01T00:00:00+01:00",
    )
    late = _occurrence(
        "late.osm.pbf",
        osm_version=1,
        osm_timestamp="2025-12-31T22:30:00Z",
    )

    assert canonical_occurrence((early, late)) == early


def test_identity_rejects_non_polygon_osm_types() -> None:
    with pytest.raises(ValueError, match="way or relation"):
        OsmIdentity("node", 7)


def test_occurrence_rejects_empty_source_name() -> None:
    with pytest.raises(ValueError, match="source PBF"):
        Occurrence(
            identity=OsmIdentity("relation", 8),
            source_pbf="",
            region="fixture",
        )


def test_occurrence_timestamp_is_available_as_datetime() -> None:
    occurrence = _occurrence(
        "fixture.osm.pbf",
        osm_version=1,
        osm_timestamp="2026-01-02T03:04:05Z",
    )

    assert occurrence.timestamp_value == datetime.fromisoformat("2026-01-02T03:04:05+00:00")
