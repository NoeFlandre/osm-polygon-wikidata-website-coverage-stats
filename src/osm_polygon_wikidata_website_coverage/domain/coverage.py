"""Pure source membership and overlap calculations."""

from __future__ import annotations

from dataclasses import dataclass

EXPECTED_OVERLAP_CATEGORIES = (
    "neither",
    "website_only",
    "wikipedia_only",
    "wikivoyage_only",
    "website_wikipedia_only",
    "website_wikivoyage_only",
    "wikipedia_wikivoyage_only",
    "all_three",
)

_OVERLAP_BY_FLAGS = dict(
    zip(
        (
            (False, False, False),
            (True, False, False),
            (False, True, False),
            (False, False, True),
            (True, True, False),
            (True, False, True),
            (False, True, True),
            (True, True, True),
        ),
        EXPECTED_OVERLAP_CATEGORIES,
        strict=True,
    )
)


@dataclass(frozen=True, slots=True)
class SourceFlags:
    """Independent successful-text membership flags."""

    website: bool = False
    wikipedia: bool = False
    wikivoyage: bool = False

    @property
    def covered_by_any_text(self) -> bool:
        return self.website or self.wikipedia or self.wikivoyage

    @property
    def overlap_category(self) -> str:
        return overlap_category(self.website, self.wikipedia, self.wikivoyage)


def overlap_category(website: bool, wikipedia: bool, wikivoyage: bool) -> str:
    """Return the mutually exclusive category for three source flags."""

    return _OVERLAP_BY_FLAGS[(website, wikipedia, wikivoyage)]


def coverage_flags(*, website: bool, wikipedia: bool, wikivoyage: bool) -> SourceFlags:
    """Build a normalized immutable flag value."""

    return SourceFlags(bool(website), bool(wikipedia), bool(wikivoyage))
