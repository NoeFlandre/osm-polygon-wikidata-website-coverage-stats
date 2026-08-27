# Methodology

## Sets and denominator

Let `U` be the unique set of valid polygon identities assembled from all raw
PBFs. The identity key is `(osm_type, osm_id)`, with `osm_type` restricted to
`way` and `relation`. Closed ways are assembled by libosmium. Relations are
included only for `type=multipolygon` and `type=boundary`. Duplicate raw
occurrences collapse globally; the highest OSM version wins, then the newest
timestamp, then the lexicographically smallest source PBF. All contributing
PBF names remain in provenance.

The headline denominator is `|U|`, the count of valid successfully assembled
polygon geometries. A raw candidate whose geometry cannot be normalized is
recorded in a separate geometry-failure audit and is not classified as
uncovered text. Source membership keys that do not occur in `U` are also
reported separately and do not enlarge the denominator.

## Successful text predicates

Website membership is true when either independent field succeeds:

```text
website_text_status == "success" and website_text is nonempty after trim
OR
contact_website_text_status == "success" and contact_website_text is nonempty after trim
```

Wikipedia and Wikivoyage membership is evaluated separately. A linked document
must belong to the requested project, have `fetch_status == "ok"`, and have
nonempty `full_text` after trim. All successful V2 document links are eligible,
including direct Wikipedia-tag links and links discovered through Wikidata
sitelinks. Multiple successful documents still produce one Boolean membership
per source and polygon identity.

## Overlap categories

Every identity receives exactly one of these eight categories:

| Website | Wikipedia | Wikivoyage | Category |
| --- | --- | --- | --- |
| no | no | no | neither |
| yes | no | no | website only |
| no | yes | no | Wikipedia only |
| no | no | yes | Wikivoyage only |
| yes | yes | no | website + Wikipedia only |
| yes | no | yes | website + Wikivoyage only |
| no | yes | yes | Wikipedia + Wikivoyage only |
| yes | yes | yes | all three |

The category counts sum to the valid raw polygon universe. Pairwise and triple
intersections are reported in addition to the mutually exclusive categories.

## Geometry

The libosmium GeoJSON area is accepted only as Polygon or MultiPolygon. Invalid
nonempty polygonal shapes are repaired with Shapely `buffer(0)` only when the
result remains Polygon or MultiPolygon. Empty, degenerate, out-of-bounds, and
antimeridian-spanning shapes fail closed.

Coordinates are rounded to seven decimal places for a stable local geometry
representation and hash. Area uses `pyproj.Geod` on WGS84. Centroids use a
local Lambert azimuthal equal-area projection, then transform back to WGS84.
Outputs retain geometry type, centroid, bounding box, area, area bucket, and
geometry hash; full geometry is kept only in local run occurrence shards.

## Licensing and attribution

Code and documentation are Apache-2.0. Polygon identities and geometry-derived
descriptors are derived from OpenStreetMap data and are distributed with ODbL
and OpenStreetMap attribution obligations. Raw PBF provenance is recorded for
reproducibility. Website, Wikipedia, and Wikivoyage fetched text is used only
as a read-only success predicate and is not republished by this project.
