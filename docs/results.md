# Verified Wikidata coverage results

This note records the read-only verification of run
`20260829-website-wikidata-overlap-v1`. It explains why the full processed
Wikidata dataset is on the scale of more than one million polygon identities,
while the current successful-text overlap is on the scale of three hundred
thousand.

## Short answer

The million-scale intuition is correct for the processed Wikidata polygon
inventory. The approximately 300,000 figure is a narrower measure: successful
Wikipedia or Wikivoyage text whose OSM identity also occurs in the selected raw
PBF polygon universe.

The current run uses a broad Wikimedia-text membership predicate. Under that
predicate, `303,195` raw polygon identities are covered. A strict
Wikidata-sitelink interpretation gives `288,007` raw polygon identities.

## HF polygon-inventory identity overlap

A separate comparison of the local Parquet snapshots corresponding to the
published Hugging Face polygon inventories gives the following unique OSM
identity counts:

| Dataset or comparison | Unique polygons |
| --- | ---: |
| OSM Polygon–Wikidata–Wikipedia | 1,188,854 |
| OSM Polygon–Website Tag | 1,678,146 |
| Shared identities between both datasets | 1,309 |

These are inventory-level `(osm_type, osm_id)` identity counts. They are not
counts of polygons with non-empty text and do not use the raw-PBF universe.
They were computed from the local snapshots without downloading from Hugging
Face; the small reproducibility artifacts are stored in
`runs/20260831-hf-snapshot-overlap-v1/`.

## Verified counts

All counts below are unique `(osm_type, osm_id)` identities unless explicitly
labelled as regional rows or Wikidata entities.

| Measure | Count |
| --- | ---: |
| `processed_v2` retained polygon rows, including regional copies | 1,259,424 |
| Unique OSM polygon identities in `processed_v2` | 1,188,854 |
| Unique identities carrying a valid Wikidata tag | 1,157,841 |
| Unique Wikidata entities reported by the source README | 1,119,287 |
| Successful text identities reached through a Wikidata sitelink | 650,661 |
| Successful-text membership identities in the current run | 684,411 |
| Unique raw structural polygon identities | 856,623,414 |
| Processed Wikidata-tagged identities also present in the raw universe | 521,334 |
| Strict Wikidata-sitelink identities present in the raw universe | 288,007 |
| Current broad successful-text identities present in the raw universe | 303,195 |

The source README's `1,119,287` figure counts Wikidata entities (QIDs), not
OSM polygon identities. Several OSM polygons can refer to the same QID, and
regional extracts can contain repeated copies.

## Why the current run reports 303,195

The current membership query includes every `way` or `relation` linked to a
Wikipedia or Wikivoyage document with `fetch_status = "ok"` and non-empty
`full_text`. It does not require the link provenance to contain
`wikidata_sitelink`. Therefore it is a successful Wikimedia-text set labelled
as Wikidata coverage, rather than a strict Wikidata-sitelink set.

Within the raw universe, the broad result is:

| Category | Count |
| --- | ---: |
| Wikidata-only | 302,666 |
| Both website and Wikidata | 529 |
| Total broad Wikidata coverage | 303,195 |

The strict Wikidata-sitelink verification finds `288,007` of those raw
identities. The difference, `15,188`, comes from other link provenance in the
processed dataset.

If “Wikidata polygon” means an identity carrying a valid Wikidata tag and the
current successful-text rule is retained, the corresponding raw-universe
count is `289,854`. If it means that the successful text must be reached
specifically through a Wikidata sitelink, the count is `288,007`.

## Important source alignment finding

Only `521,334` of the `1,157,841` unique Wikidata-tagged identities in the
processed polygon inventory occur in the raw structural-polygon universe used
by this run. This is a substantial alignment gap. It means the two inputs do
not represent the same exact polygon identity snapshot or structural
selection, even though their regional filenames match.

Consequently, the strict `288,007` value should be understood as coverage of
the current raw-PBF universe, not as the total coverage of the processed
Wikidata dataset. The source/raw alignment should be investigated before
using the result as a global coverage claim.

## Definitions and evidence

- The raw denominator contains only structurally closed ways and relations
  with structural `type=multipolygon` or `type=boundary`. Nodes and open ways
  are excluded; content tags do not define the denominator.
- Successful text means `fetch_status = "ok"` and non-empty trimmed
  `full_text`.
- The join key is the original OSM pair `(osm_type, osm_id)`.
- The completed run manifest is stored at
  `/Volumes/Seagate M3/projects/osm-polygon-wikidata-website-coverage-stats/runs/20260829-website-wikidata-overlap-v1/manifests/manifest.json`.
- The processed dataset description is
  `/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/processed_v2/README.md`.
