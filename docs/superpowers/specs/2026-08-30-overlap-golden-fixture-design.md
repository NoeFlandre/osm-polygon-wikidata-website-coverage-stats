# Overlap Golden Fixture Design

## Goal

Add one small, deterministic integration contract for the website/Wikidata
overlap stage. The contract must prove all four overlap categories, raw-identity
deduplication, and resumability without reading or writing the production data
trees.

## Design

The test creates a temporary raw-identity Parquet input and two temporary
membership Parquet inputs using the production schemas. The raw universe has
four unique identities and one duplicate occurrence: one identity is in
neither membership set, one is website-only, one is Wikidata-only, and one is
in both sets. The expected rows and four summary counts are fixed in a small
JSON fixture so the test is readable and independent of implementation details.

The first `compute_overlap` call writes the normal 64-shard output and summary.
The test reads the non-empty rows, sorts them by OSM identity, and compares the
exact flags and categories to the fixture. It also compares the exact summary
counts. The test then records output bytes, blocks the overlap query, and calls
`compute_overlap(..., resume=True)`. The resumed result must have the same
result object and unchanged output bytes, proving that a valid stage is reused
without recomputation.

## Boundaries

This is an overlap-stage contract, not a raw-PBF parser test. PBF structural
filtering remains covered by the existing `io.pbf` tests. The fixture contains
only OSM identity keys and membership booleans; it never copies source text,
geometry, or production data. No new production API or abstraction is needed.

## Success criteria

- The fixture explicitly covers `neither`, `website_only`, `wikidata_only`,
  and `both`.
- Duplicate raw occurrences produce one output identity.
- The first run has the expected rows, counts, and four-row summary.
- The resumed run does not call the overlap query and preserves all output
  bytes.
- Existing tests and all repository quality gates remain green.
