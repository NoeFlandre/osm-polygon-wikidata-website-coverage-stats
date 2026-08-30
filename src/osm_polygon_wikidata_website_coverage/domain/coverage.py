"""Pure two-set coverage and overlap calculations."""

from __future__ import annotations

from dataclasses import dataclass

OVERLAP_CATEGORIES = (
    "neither",
    "website_only",
    "wikidata_only",
    "both",
)


@dataclass(frozen=True, slots=True)
class CoverageFlags:
    """Independent successful-text membership flags."""

    website: bool = False
    wikidata: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "website", bool(self.website))
        object.__setattr__(self, "wikidata", bool(self.wikidata))

    @property
    def overlap_category(self) -> str:
        return overlap_category(self.website, self.wikidata)


def overlap_category(website: bool, wikidata: bool) -> str:
    """Return the mutually exclusive category for two source flags."""

    website = bool(website)
    wikidata = bool(wikidata)
    if website and wikidata:
        return "both"
    if website:
        return "website_only"
    if wikidata:
        return "wikidata_only"
    return "neither"
