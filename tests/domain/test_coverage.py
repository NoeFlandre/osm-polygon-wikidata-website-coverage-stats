from typing import cast

import pytest

from osm_polygon_wikidata_website_coverage.domain.coverage import (
    EXPECTED_OVERLAP_CATEGORIES,
    SourceFlags,
    coverage_flags,
    overlap_category,
)


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ((False, False, False), "neither"),
        ((True, False, False), "website_only"),
        ((False, True, False), "wikipedia_only"),
        ((False, False, True), "wikivoyage_only"),
        ((True, True, False), "website_wikipedia_only"),
        ((True, False, True), "website_wikivoyage_only"),
        ((False, True, True), "wikipedia_wikivoyage_only"),
        ((True, True, True), "all_three"),
    ],
)
def test_overlap_category_is_exhaustive(flags: tuple[bool, bool, bool], expected: str) -> None:
    assert overlap_category(*flags) == expected


def test_expected_categories_are_unique_and_complete() -> None:
    assert len(EXPECTED_OVERLAP_CATEGORIES) == 8
    assert len(set(EXPECTED_OVERLAP_CATEGORIES)) == 8


def test_source_flags_expose_union_and_category() -> None:
    flags = coverage_flags(website=True, wikipedia=False, wikivoyage=True)

    assert flags == SourceFlags(website=True, wikipedia=False, wikivoyage=True)
    assert flags.covered_by_any_text is True
    assert flags.overlap_category == "website_wikivoyage_only"


def test_source_flags_for_no_sources_are_uncovered() -> None:
    flags = SourceFlags()

    assert flags.covered_by_any_text is False
    assert flags.overlap_category == "neither"


def test_coverage_flags_normalizes_every_boolean_input() -> None:
    assert coverage_flags(
        website=cast(bool, 1), wikipedia=cast(bool, object()), wikivoyage=cast(bool, [])
    ) == SourceFlags(
        website=True,
        wikipedia=True,
        wikivoyage=False,
    )
