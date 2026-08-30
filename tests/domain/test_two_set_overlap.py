from osm_polygon_wikidata_website_coverage.domain.coverage import (
    OVERLAP_CATEGORIES,
    overlap_category,
)


def test_two_set_overlap_has_exactly_four_categories() -> None:
    assert OVERLAP_CATEGORIES == (
        "neither",
        "website_only",
        "wikidata_only",
        "both",
    )
    assert [
        overlap_category(False, False),
        overlap_category(True, False),
        overlap_category(False, True),
        overlap_category(True, True),
    ] == list(OVERLAP_CATEGORIES)
