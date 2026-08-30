# Website/Wikidata overlap implementation plan

> **Execution note:** implement this plan directly on `main`, without creating
> another branch or downloading dependencies/data.

**Objective:** reduce the project to a fast, resumable computation of the
website-versus-Wikidata overlap over the raw closed-way and supported-relation
universe, under a 5 GB resident-memory budget.

## Contracts to preserve

- raw source roots are read-only;
- only closed ways and `multipolygon`/`boundary` relations enter the universe;
- nodes, tags other than relation structural type, geometry, and source text are
  never output;
- website membership requires successful non-empty website/contact text;
- Wikidata membership is the union of successful non-empty Wikipedia and
  Wikivoyage documents. The processed link tables already contain original
  OSM IDs, so no Area ID normalization is applied;
- each identity is counted once across all raw PBFs;
- overlap categories are exactly `neither`, `website_only`, `wikidata_only`,
  and `both`;
- extraction is atomic and resumable per PBF;
- generated artifacts and DuckDB spill live under the Seagate run root;
- no network/download step is used by tests, tools, or the run command.

## Sequence

### 1. Establish RED tests

Replace tests for the removed geometry, reporting, three-source aggregation,
and HF staging behavior with focused tests for:

- two-boolean overlap classification and exhaustive four-category mapping;
- successful website predicate and successful Wikipedia/Wikivoyage union;
- original OSM IDs from the processed polygon-document link tables;
- raw PBF structural filtering without geometry assembly;
- one bounded identity Parquet per PBF and atomic temporary-file behavior;
- checkpoint validation and skip-on-resume behavior;
- one-pass membership reuse and partition-first output equivalence;
- four-category summary counts/percentages;
- output/schema/source immutability contracts;
- 5 GB memory/spill configuration and Seagate-only paths.

Run the focused suite and record the expected failures before production edits.

### 2. Remove dead surface area

Delete the unused geometry and reporting/publication modules and their tests:

- `domain/geometry.py`;
- geometry-bearing identity/failure models;
- geometry-capable PBF handlers;
- old aggregate/report/render/HF staging code;
- obsolete three-source schemas and summary helpers;
- now-unused scientific/plotting dependencies.

Retain only the thin CLI, path validation, raw identity scan, source readers,
bounded Parquet I/O, overlap aggregation, checkpoints, documentation, Docker,
and quality tooling.

### 3. Implement GREEN extraction and source membership modules

- Add a minimal `RawIdentity` model and two-set coverage model.
- Make the coverage PBF handler emit only `osm_type`, `osm_id`, and the source
  PBF name needed for extraction accounting.
- Implement a streaming writer that keeps one ParquetWriter open per source,
  writes bounded Arrow batches, and atomically promotes one final file.
- Preserve exact checkpoint content-integrity validation while recording the
  new one-file schema.
- Add one combined read-only Wikidata success query for Wikipedia and
  Wikivoyage, plus the existing website success query.
- Materialize only `members/website.parquet` and `members/wikidata.parquet`.

### 4. Implement GREEN partition-first overlap aggregation

- Configure DuckDB with a memory limit safely below 5 GB and four threads.
- Create/reuse local temporary membership tables once per run.
- Read all raw identity files once, left join the two membership sets, compute
  flags/category in one projection, and hash-partition occurrences.
- Deduplicate each hash bucket independently, then export deterministic 64
  overlap shards atomically. The hash invariant keeps every identity in one
  bucket, so this is equivalent to global deduplication with a smaller working
  set.
- Compute only the four-category summary from the final overlap rows.
- Add stage manifests so matching membership/overlap outputs can be reused on
  resume and mismatched artifacts are rebuilt safely.

### 5. Refactor and documentation

- Remove dead imports, schemas, options, docs, and dependency declarations.
- Make `coverage run` expose only the overlap workflow and its resumability
  options.
- Document exact denominator/category definitions, output layout, memory cap,
  no-download operation, and Seagate-only scratch behavior.
- Keep MkDocs, Docker, Apache-2.0, citation, and public repository metadata
  coherent with the reduced project.

### 6. Verification

Run focused tests through RED and GREEN, then the full offline gates:

```text
ruff format --check
ruff check
ty check
pytest --cov --cov-branch --cov-fail-under=100
MkDocs strict build
CRAP check (< 6)
mutmut (0 survivors/timeouts)
```

Use the existing local Python/tool installations and an isolated local cache
only if already populated; do not fetch packages. Run a small fixture benchmark
to compare row/file counts and output values, and inspect that the paused
`20260828-coverage-v5` Seagate run remains untouched. If the local Docker
daemon is unavailable, report that separately rather than pretending the Docker
build passed.

### 7. Publish the verified change

Commit only the intended repository files on `main`, verify the worktree and
remote have only `main`, and push the commit. Do not upload the full raw/source
datasets or use network access as part of computation.
