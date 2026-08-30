import pytest

from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity


@pytest.mark.parametrize("osm_type", ["way", "relation"])
def test_osm_identity_accepts_supported_positive_ids(osm_type: str) -> None:
    identity = OsmIdentity(osm_type, 7)

    assert identity.osm_type == osm_type
    assert identity.osm_id == 7
    assert identity.key == (osm_type, 7)


@pytest.mark.parametrize(
    ("osm_type", "osm_id", "message"),
    [("node", 1, "way or relation"), ("way", 0, "positive"), ("relation", -1, "positive")],
)
def test_osm_identity_rejects_invalid_values(osm_type: str, osm_id: int, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        OsmIdentity(osm_type, osm_id)
