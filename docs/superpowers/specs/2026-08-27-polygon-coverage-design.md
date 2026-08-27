# OSM Polygon Website/Wikimedia Coverage Analysis

**Status:** Approved for implementation
**Date:** 2026-08-27

## Goal

Measure how much of the polygon universe in the supplied raw OpenStreetMap PBF
files is covered by successfully fetched website text, Wikipedia text, and
Wikivoyage text. Report each source separately, all mutually exclusive
overlap categories, the uncovered valid-polygon set, and descriptive geometry
statistics.

The analysis counts unique OpenStreetMap features, not source rows, documents,
URLs, or geometric intersections.

## Immutable input boundary

The implementation reads these existing paths without modifying them:

```text
Raw PBFs:
/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/raw

Wikidata V2 artifacts:
/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/processed_v2

Website V1 artifacts:
/Volumes/Seagate M3/projects/osm-polygon-website-tag-data/runs/geofabrik-website-v1
```

The project data root is:

```text
/Volumes/Seagate M3/projects/osm-polygon-wikidata-website-coverage-stats
```

All generated tables, reports, manifests, logs, caches, and temporary files
must be beneath that project data root. No input file is copied or opened for
writing. The run records source paths, file names, sizes, modification times,
and content fingerprints sufficient to identify the analyzed snapshot.

The raw universe includes every regular `*.osm.pbf` file directly beneath the
raw input directory, processed in deterministic filename order. A missing or
unreadable expected source fails the run closed.

## Set definitions

### Raw universe

The structural raw candidate set contains:

1. closed ways with at least three distinct node references, where the first
   node reference equals the last; and
2. relations tagged `type=multipolygon` or `type=boundary` that libosmium
   successfully assembles as an area.

No `wikidata`, `wikipedia`, `website`, or other content-bearing tag is used to
decide whether a raw feature belongs to the universe. The structural
`type=multipolygon` or `type=boundary` relation tag is used only to identify
polygon relations.

The main denominator is the set of unique candidates with successfully
assembled, valid, normalized polygon or multipolygon geometry. Structural
candidates with missing, malformed, unrepairable, unsupported, or otherwise
invalid geometry are recorded in a separate failure table and are not silently
classified as text-uncovered polygons.

Every identity is keyed globally by `(osm_type, osm_id)`, where `osm_type` is
`way` or `relation`. A feature appearing in overlapping PBFs is counted once.

### Source membership

For every valid raw-universe identity, the analysis computes these independent
Boolean flags:

```text
has_website_text
has_wikipedia_text
has_wikivoyage_text
```

`has_website_text` is true when at least one of the website row's independent
`website` or `contact:website` text results has both a `success` status and
non-empty text.

`has_wikipedia_text` and `has_wikivoyage_text` are true when at least one
project-specific V2 document linked to the polygon has `fetch_status == "ok"`
and non-empty `full_text`. Wikipedia includes both Wikidata-sitelink and direct
OSM `wikipedia=*` discoveries represented by the V2 link table.

Failed, empty, missing, or otherwise unusable source records do not disqualify
a polygon when another record for the same source succeeds. Source documents
and website text are used only to derive flags and diagnostics; their text is
not copied into project outputs or the public HF dataset.

### Overlap categories

The primary overlap table contains exactly eight mutually exclusive categories:

| Category | Membership expression |
| --- | --- |
| neither | no source flag |
| website only | website and neither Wikimedia flag |
| Wikipedia only | Wikipedia and neither website/Wikivoyage flag |
| Wikivoyage only | Wikivoyage and neither website/Wikipedia flag |
| website + Wikipedia only | website and Wikipedia, not Wikivoyage |
| website + Wikivoyage only | website and Wikivoyage, not Wikipedia |
| Wikipedia + Wikivoyage only | both Wikimedia flags, not website |
| all three | all flags true |

Headline coverage percentages use the valid raw-universe denominator. The
report also contains ordinary source totals and pairwise/triple intersections.
Global totals count each identity once; per-PBF and per-region tables preserve
source-level provenance.

## Geometry contract

Raw geometry is assembled with libosmium's `Area` callback and
`osmium.geom.GeoJSONFactory().create_multipolygon(area)`. The implementation
does not build relation polygons by manually concatenating member ways.

The canonical geometry stage follows the more rigorous website-project
approach:

1. parse the libosmium GeoJSON into Shapely `Polygon` or `MultiPolygon`;
2. preserve exterior/interior ring association;
3. repair a non-empty invalid geometry with Shapely's established repair path,
   accepting the result only when it remains a Polygon or MultiPolygon;
4. reject empty, degenerate, unsupported, or antimeridian-spanning geometry;
5. round coordinates deterministically to seven decimal places;
6. compute area with `pyproj.Geod` on WGS84, subtracting interior rings; and
7. compute a deterministic centroid with the local Lambert azimuthal equal-area
   projection used by the website project.

The Seagate analysis artifacts retain normalized geometry, geometry type,
centroid, bounding box, and area values for auditability. The public HF table
does not contain full polygon coordinate payloads; it contains the compact
geometry descriptors needed for the requested statistics.

When duplicate raw occurrences disagree, the canonical occurrence is selected
by highest OSM version, then newest OSM timestamp, then lexicographically
smallest source PBF name. All contributing source PBF names and conflicts are
retained in diagnostics.

## Data flow and module boundaries

The project uses focused deep modules with small public interfaces:

```text
config/paths       validate Seagate output and immutable input boundaries
domain/identity    OSM keys, source provenance, and duplicate resolution
domain/geometry   area assembly result normalization and metric calculation
domain/coverage    source flags and the eight overlap categories
io/pbf             streaming libosmium reader and failure records
io/parquet         schema-checked bounded readers and writers
sources/website   successful website-text membership reader
sources/wikimedia successful Wikipedia/Wikivoyage membership reader
pipeline/extract  per-PBF occurrence extraction
pipeline/join     source membership joins without loading text into output
pipeline/aggregate global deduplication and summary aggregation
reporting         JSON/Parquet summaries, Markdown, and static charts
cli               thin Typer command-line composition layer
```

The pipeline writes a run directory beneath the Seagate project data root:

```text
<data-root>/runs/<run-id>/
  occurrences/         per-PBF valid raw polygon shards
  geometry-failures/   structural candidates excluded from the denominator
  coverage/            global compact rows and source-level shards
  summaries/           JSON and Parquet summary tables
  reports/             Markdown report and static figures
  manifests/           input, schema, and completion manifests
  logs/                bounded run logs
```

The run never writes into a source tree. Intermediate files are bounded,
written atomically where practical, and deterministic under repeated runs.

## Output contracts

The compact global coverage row contains:

- `osm_type`, `osm_id`;
- contributing source PBF count and deterministic source-PBF list;
- canonical region/source provenance;
- `geometry_type`, centroid coordinates, area in square metres, and area
  bucket;
- `has_website_text`, `has_wikipedia_text`, `has_wikivoyage_text`;
- `covered_by_any_text`; and
- the mutually exclusive `overlap_category`.

Seagate-only outputs additionally retain normalized geometry and detailed
diagnostic fields. Summary outputs contain global totals, percentages,
per-source-PBF/region counts, OSM type counts, geometry type counts, area
totals/median/quantiles, overlap categories, geometry failures, source-row
duplicates, and unmatched source identities.

## Public repositories

The public GitHub repository is:

```text
https://github.com/NoeFlandre/osm-polygon-wikidata-website-coverage-stats
```

The public Hugging Face dataset repository is:

```text
https://huggingface.co/datasets/NoeFlandre/osm-polygon-wikidata-website-coverage-stats
```

The HF dataset contains deterministic Parquet shards of the compact global
coverage table and summary artifacts. It contains no raw PBFs, full polygon
coordinates, website text, Wikipedia text, Wikivoyage text, credentials, or
private run state. A dataset card documents the schema, source paths, snapshot
manifest, methodology, limitations, and attribution.

Code and documentation use Apache-2.0. OSM-derived public artifacts carry
the applicable ODbL 1.0 attribution and do not claim Apache-2.0 ownership over
the underlying source material. Third-party website and Wikimedia text is not
republished by this project.

## Quality and verification

The repository uses Python 3.12+, `uv`, Ruff, `ty`, pytest, coverage, pre-
commit, Just, MkDocs Material, Docker, and GitHub Actions. Implementation is
strict RED -> GREEN -> REFACTOR TDD.

Tests cover the structural polygon filter, relation assembly classification,
geometry normalization and failure paths, source-success predicates, global
deduplication, conflict resolution, overlap partitioning, deterministic
sharding, read-only input enforcement, schema contracts, report derivation,
HF staging, Docker, and documentation configuration.

Completion requires:

- all regression and acceptance tests passing;
- Ruff and `ty` passing;
- strict MkDocs build passing;
- CRAP score below 6 for every measured function;
- a fresh mutation campaign with zero surviving, suspicious, unchecked, or
  otherwise unresolved mutants;
- production smoke verification on a bounded fixture; and
- a complete real-data run whose manifests and output checks pass.

## Explicit non-goals

This project does not fetch new website or Wikimedia content, infer text from
OSM tags, perform geometric intersection matching, publish raw inputs, train
models, build an interactive web application, or add unrelated OSM enrichment.
