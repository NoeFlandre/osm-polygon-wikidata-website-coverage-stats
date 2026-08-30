# Architecture

The code is organized as a few deep modules with narrow boundaries:

```text
raw PBFs -> libosmium structural identity stream -> one Parquet per PBF
source Parquets -> read-only DuckDB predicates -> two local membership tables
raw identity files + memberships -> DuckDB partition-first projection -> 64 shards
overlap shards -> four-category summary and completion manifests
```

`config.paths` owns the Seagate-only storage contract and prevents output/source
overlap. `domain.identity` and `domain.coverage` contain the stable key and
pure two-set category rules. `io.pbf` is the libosmium boundary and never
requests node locations. `io.parquet` provides bounded Arrow batches, while
`io.atomic` owns promotion and cleanup for ordinary generated artifacts.

The website and Wikimedia source modules validate only required Parquet
metadata, then expose read-only DuckDB query parameters. Website text is
successful non-empty `website` or `contact:website` text. Wikidata membership
is one union of successful non-empty Wikipedia and Wikivoyage documents. The
processed link files already use original OSM IDs.

`pipeline.extract` scans PBFs independently, keeps one bounded writer per
source, and checkpoints at PBF boundaries. `pipeline.join` materializes the two
membership tables once. `pipeline.overlap` hash-partitions raw identity
occurrences, deduplicates each partition independently, and then joins the two
membership tables once per unique identity in that partition to compute both
flags and the category. This bounds the largest deduplication working set and
avoids repeated joins for duplicate raw occurrences. No per-shard source
rescan, geometry assembler, report renderer, or publisher is in the run path.

DuckDB is limited to 3 GB and four threads. Its spill directory is under the
Seagate run root and is deleted after the connection closes. Python never loads
a whole PBF, source tree, or raw universe into memory.

The quality contract is RED→GREEN→REFACTOR with pytest, branch coverage,
Ruff, ty, CRAP below 6, mutation testing, strict MkDocs, and a Docker image
that contains code only. The source roots and the paused prior run are not
modified by this project.

Mutation testing is focused on the semantic contract surface: pure domain
rules, raw-PBF structural polygon filtering, and read-only source adapters.
The pipeline's filesystem, manifest, and DuckDB orchestration is verified by
the full branch-coverage suite and end-to-end fixtures; it is not included in
the mutation inventory because its serialization and resource-management
variants are often observationally equivalent. The mutation gate still fails
on every survivor in the configured semantic surface.
