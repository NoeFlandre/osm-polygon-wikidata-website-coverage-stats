# Website and Wikidata overlap performance design

Date: 2026-08-29
Status: Accepted

## Goal

Compute only the coverage overlap between:

1. the raw OSM polygon universe, and
2. successful website text versus successful Wikidata-derived text.

The optimized run must remain local, resumable, and bounded to 5 GB of resident
memory. It must not download data or write into any source tree.

## Scope

The raw universe is the set of unique OSM identities found in the raw PBF
directory:

- structurally closed ways;
- relations whose structural `type` is `multipolygon` or `boundary`;
- no nodes;
- no tag/content filtering for ways;
- no geometry assembly or geometry metrics.

The two coverage sets are:

- **website**: an OSM identity with successful, non-empty `website` or
  `contact:website` text in the website source dataset;
- **wikidata**: an OSM identity linked to a successful, non-empty Wikipedia or
  Wikivoyage document in `processed_v2`. The processed link tables already
  contain the original `osm_type` and `osm_id`, so no Area ID conversion is
  applied.

Wikipedia and Wikivoyage are unioned into one Wikidata coverage set. They will
not appear as separate output columns or reports in this scoped run.

For every raw-universe identity, the result is exactly one of:

| Category | Website | Wikidata |
| --- | ---: | ---: |
| `neither` | no | no |
| `website_only` | yes | no |
| `wikidata_only` | no | yes |
| `both` | yes | yes |

The denominator is the unique raw-universe identity count. Duplicate appearances
of an identity in multiple regional PBFs count once.

Out of scope for this run: full geometry, geometry failures, by-PBF summaries,
separate Wikipedia/Wikivoyage statistics, area statistics, source text, raw PBF
copies, and unrelated publication artifacts.

## Output contract

The scoped derived data contains only the information needed to answer the
overlap questions:

- `coverage/overlap/shard-00.parquet` through `shard-63.parquet`, each with:
  `osm_type`, `osm_id`, `website`, `wikidata`, and `overlap_category`;
- `coverage/overlap-summary.parquet`, with the four categories, counts, and
  percentages;
- resumability/checkpoint metadata under the run directory, containing no
  source text or geometry.

The row-level shards are deterministic and sorted by `osm_type, osm_id` within
each shard. The public dataset contains no PBF bytes, full source text, or
geometry.

## Proposed pipeline

### 1. Raw identity extraction

The coverage-only libosmium pass remains the source of truth for the raw
universe. Each PBF is processed independently and writes one atomic Parquet
file containing only the identity columns needed by the join. Rows are buffered
as bounded Arrow batches; the complete file is first written to a Seagate-local
temporary path and renamed only after the writer closes successfully.

This changes the paused run's current pattern of thousands of 50,000-row files
into at most one extraction file per PBF while keeping memory bounded. A failed
PBF leaves only a temporary file, which is removed before that PBF is retried.
The exact structural closed-way predicate remains unchanged.

Each PBF keeps a checkpoint with source size, modification time, content hash,
row count, scanner mode, output schema, and output-file metadata. Existing
source mutation checks remain enabled; a new scan hashes the source once before
parsing and checks size/mtime after parsing. On resume, unchanged size/mtime
reuse the stored digest without rereading the PBF; changed or incomplete
metadata falls back to a full hash. This fast path is valid because the source
trees are strict read-only inputs.

### 2. Membership materialization

The source roots remain strict read-only inputs.

- Website successful keys are scanned once and materialized to a local,
  key-only `members/website.parquet`.
- Wikipedia and Wikivoyage successful keys are scanned through one combined
  union query and materialized once to a local key-only
  `members/wikidata.parquet`.

The two local membership tables are the only inputs used by the final overlap
join. They are not recomputed for every output shard. Materialization is
atomic, and a small stage manifest permits a matching completed stage to be
reused on resume.

### 3. Partition-first overlap join

DuckDB will read all local raw identity files once and calculate a stable hash
shard for each raw identity occurrence. It will not perform a global raw-
identity `DISTINCT` or join the membership tables for duplicate occurrences.

The raw occurrences will first be written into 64 hash buckets. Each bucket is
then deduplicated independently, joined to the two local membership tables,
classified, and sorted before atomic promotion to its final shard. Since every
occurrence of an identity has the same hash, local deduplication is exactly
equivalent to global deduplication while bounding the largest working set.
Membership tables are loaded once and are not rescanned from the source trees
for each shard. DuckDB will use four threads and a configured memory limit below
the 5 GB process budget; any external spill will be under
`<Seagate project root>/runs/<run-id>/scratch/` and cleaned after successful
stage completion or at the next safe resume boundary.

### 4. Single overlap summary

Counts and percentages for `neither`, `website_only`, `wikidata_only`, and
`both` will be computed from the final overlap export in one aggregate query.
No other summaries are generated. The summary is atomically promoted after
the row-level shards are complete.

## Resumption and failure boundaries

The run is resumable at these boundaries:

1. completed raw PBF extraction files can be reused after checkpoint validation;
2. completed website/Wikidata membership files can be reused after source
   inventory validation;
3. completed overlap shards and summary can be reused only when their stage
   manifest matches the extraction and membership manifests.

Temporary files, DuckDB spill directories, and staging directories are always
under the Seagate project data root. They are removed after their owning stage
closes successfully. The paused `20260828-coverage-v5` run is not modified by
this work.

## Memory and I/O budget

- resident-memory target: at most 5 GB;
- DuckDB memory limit: leave headroom for the interpreter and PBF workers;
- extraction workers and Arrow batch size are bounded configuration values;
- the default extraction worker count is one because the target Seagate disk
  loses throughput to concurrent PBF reads;
- no whole PBF, source dataset, or complete raw universe is loaded into Python;
- no temporary output is written to the system volume;
- no package, source, or dataset download is allowed during implementation or
  verification.

The implementation will benchmark batch and worker settings on small local
fixtures before selecting defaults. Timing assertions will not be used as
flaky unit-test gates; deterministic scan-count, row-count, schema, and output
equivalence tests will guard the optimization.

## TDD and quality gates

Implementation will proceed RED -> GREEN -> REFACTOR:

1. add failing tests for the four-category semantics, unioned Wikidata keys,
   one-file bounded extraction, atomic failure behavior, stage reuse, and
   unchanged source trees;
2. implement the smallest pipeline changes that make those tests pass;
3. refactor only after behavior is green;
4. run offline Ruff, ty, pytest with 100% branch coverage, CRAP below 6,
   mutation testing with zero surviving mutants, and strict MkDocs validation.

The existing code, source paths, paused run, and remote repositories will not be
altered until the implementation is verified. The final change will be committed
on `main`; no additional branch will be created.

## Alternatives considered

### Conservative tuning only

Increasing batch sizes and adjusting DuckDB settings would be low risk, but it
would retain the 17,828-file extraction shape and repeated membership scans. It
does not meet the requested end-to-end efficiency target.

### Full binary/staging rewrite

A custom identity index could reduce I/O further, but it would introduce a new
storage format and more failure modes without being necessary for this scoped
two-set join.

The proposed bounded Parquet plus partition-first DuckDB design removes the dominant
avoidable I/O while keeping the existing libraries, source predicates, atomic
boundaries, and integrity checks.
