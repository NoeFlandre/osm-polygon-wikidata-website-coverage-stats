# Methodology

## Universe and denominator

Let `U` be the unique set of `(osm_type, osm_id)` identities emitted from all
regular raw PBFs. `osm_type` is restricted to `way` and `relation`.

A way enters `U` when libosmium reports it as closed and its node sequence
contains at least three distinct node references. A relation enters `U` when
its structural `type` is `multipolygon` or `boundary`. These are structural
polygon rules, not content-tag filters. Duplicate appearances across PBFs are
collapsed by the identity key.

The denominator is `|U|`. Source memberships that do not occur in `U` do not
enlarge it and are not emitted as overlap rows.

## Website membership

An identity is in `W` when at least one of these independent fields has a
successful, non-empty value after trimming:

```text
website_text_status == "success" and website_text is non-empty
OR
contact_website_text_status == "success" and contact_website_text is non-empty
```

Only `way` and `relation` rows are considered. The source Parquets are queried
read-only and only the resulting identity set is materialized locally.

## Wikidata membership

An identity is in `D` when a row in
`processed_v2/polygon_document_links/` links it to a Wikipedia or Wikivoyage
document whose `fetch_status` is `ok` and whose `full_text` is non-empty after
trimming. Wikipedia and Wikivoyage are intentionally unioned into one
Wikidata coverage set.

The processed link schema stores the original OSM identity, so the join uses
`osm_type` and `osm_id` directly. No Area ID parity conversion is applied.
Multiple links and documents collapse to one membership key.

## Overlap

For every `u` in `U`, the pipeline computes `u in W` and `u in D` in one
DuckDB projection, then assigns the mutually exclusive category shown below:

| Category | Definition |
| --- | --- |
| `neither` | `u` is in neither `W` nor `D` |
| `website_only` | `u` is in `W` but not `D` |
| `wikidata_only` | `u` is in `D` but not `W` |
| `both` | `u` is in both `W` and `D` |

Counts sum to `|U|`. Percentages in the summary are counts divided by `|U|`;
an empty universe yields four zero percentages.

## Data boundary

The result contains identities, two Boolean flags, and the category. It does
not contain raw PBF bytes, geometry, coordinates, source tags, website text,
Wikipedia text, Wikivoyage text, or fetch caches. Code and documentation are
Apache-2.0. OSM-derived identities remain subject to OpenStreetMap
attribution and ODbL obligations.
