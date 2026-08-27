# Architecture

The package is organized around deep modules with small interfaces:

```text
raw PBFs -> libosmium area stream -> normalized occurrence shards
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

The raw stream uses libosmium's area assembler with node locations enabled.
Its area callback emits assembled closed ways and supported polygon relations;
nodes and open ways do not reach the occurrence writer. Geometry failures are
captured with identity and diagnostic context when available.

Global aggregation partitions compact coverage rows into deterministic shards
using a stable identity hash. It chooses one canonical occurrence but retains
all source PBF names. Text is never selected into an output query. Generated
Parquet outputs are atomically promoted, inspected, hashed, and listed in the
completion manifest.

The quality contract is RED→GREEN→REFACTOR with pytest, 100% branch/line
coverage, Ruff, ty, CRAP below 6, and mutation testing with no surviving or
unchecked mutants. Public documentation is built with strict MkDocs. Docker
contains the package only and never copies Seagate data.
