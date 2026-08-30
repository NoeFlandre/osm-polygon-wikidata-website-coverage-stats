import pytest

from osm_polygon_wikidata_website_coverage.domain.coverage import (
    OVERLAP_CATEGORIES,
    CoverageFlags,
    overlap_category,
)


@pytest.mark.parametrize(
    ("website", "wikidata", "expected"),
    [
        (False, False, "neither"),
        (True, False, "website_only"),
        (False, True, "wikidata_only"),
        (True, True, "both"),
    ],
)
def test_overlap_category_exhaustively_maps_two_sets(
    website: bool, wikidata: bool, expected: str
) -> None:
    assert overlap_category(website, wikidata) == expected


def test_coverage_flags_normalize_values_and_expose_category() -> None:
    flags = CoverageFlags(website=1, wikidata=object())

    assert flags == CoverageFlags(website=True, wikidata=True)
    assert flags.overlap_category == "both"
    assert OVERLAP_CATEGORIES == ("neither", "website_only", "wikidata_only", "both")
