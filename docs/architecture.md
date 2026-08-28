# Architecture

The package is organized around deep modules with small interfaces:

```text
raw PBFs -> libosmium structural/area stream -> normalized occurrence shards
source Parquets -> read-only DuckDB predicates -> key-only memberships
occurrences + memberships -> DuckDB global deduplication -> compact coverage
compact summary -> JSON / Markdown / static charts / HF staging
```

`config.paths` owns the storage contract. `domain.identity`,
`domain.coverage`, and `domain.geometry` are pure or nearly pure values and
calculations. `io.pbf` is the libosmium boundary; `io.parquet` is the bounded
atomic writer boundary. `sources` never writes to the selected website or
Wikidata roots. `pipeline` coordinates stages, while `reporting` and the thin
CLI expose public artifacts and commands.

The default coverage-only raw stream uses libosmium with locations disabled and
emits every structurally closed way, including untagged ways. Its relation
candidate filter uses only the structural `type=multipolygon` or
`type=boundary` relation tag; this is geometry classification, not a content
tag filter. Nodes and open ways do not reach the occurrence writer. The
optional `--with-geometry` stream enables the area assembler and node
locations, then captures geometry failures with identity and diagnostic
context when available. Independent PBFs may be streamed by a bounded process
pool; results and source inventories are returned in deterministic filename
order, while each worker writes only its own source-stem shards.

Global aggregation partitions compact coverage rows into 64 deterministic
shards using a stable identity hash. It chooses one canonical occurrence but
retains the distinct source-PBF list and contributing-PBF count. Text is never
selected into an output query. Generated Parquet outputs are atomically
promoted, schema-checked, hashed, and listed in the completion manifest.
DuckDB spill files are kept under the run's Seagate-backed `scratch/` directory
with insertion-order preservation disabled, four execution threads, and a
100 GB spill ceiling so aggregation cannot silently consume the system volume.
The run removes the `scratch/duckdb-temp/` spill directory after DuckDB closes;
resume also clears stale spill files left by an abrupt termination.

The quality contract is RED→GREEN→REFACTOR with pytest, 100% branch/line
coverage, Ruff, ty, CRAP below 6, and mutation testing with no surviving or
unchecked mutants. Public documentation is built with strict MkDocs. Docker
contains the package only and never copies Seagate data.
