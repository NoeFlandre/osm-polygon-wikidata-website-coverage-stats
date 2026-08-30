# Operations

## Local-only execution

Use Python 3.12 with the project dependencies already available. Do not run a
package or data download as part of the analysis. The three input trees are
strictly read-only:

- `/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/raw`
- `/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/processed_v2`
- `/Volumes/Seagate M3/projects/osm-polygon-website-tag-data/runs/geofabrik-website-v1`

Preflight lists the sorted raw PBF inventory without touching source data:

```bash
PYTHONPATH=src python -m osm_polygon_wikidata_website_coverage.cli preflight
```

Run the overlap calculation with bounded resources:

```bash
PYTHONPATH=src python -m osm_polygon_wikidata_website_coverage.cli run \
  --run-id 20260829-website-wikidata-overlap-v1 \
  --workers 1 --batch-rows 100000
```

The default is resumable. `--fresh` refuses to overwrite an existing overlap
stage and is intended for a new run ID.

## Stage boundaries

Each regular raw PBF produces one atomic identity Parquet and one checkpoint
under `runs/<run-id>/`. A checkpoint is reused only when the source filename,
size, modification time, SHA-256, scanner mode, output schema, row count, and
output-file metadata still match.

For a new scan, the source PBF is hashed once before parsing; the post-scan
integrity check uses its size and modification time, avoiding a second full
PBF read. On resume, unchanged source size and modification time reuse the
stored digest without rereading the PBF; a changed or incomplete checkpoint
falls back to a full hash and is rejected unless it matches. This fast path
relies on the declared strict read-only source roots.

The source membership stage writes exactly two local key tables:
`members/website.parquet` and `members/wikidata.parquet`. Their source-file
inventory is recorded in `members/manifest.json` and is checked before reuse.

The overlap stage reads the raw identity files once, joins the two local key
tables, hash-partitions classified occurrences, deduplicates within each of 64
deterministic shards, and writes one four-row summary. The coverage manifest is
promoted only after all outputs validate.

## Resource and storage contract

Raw identity batches are bounded to `100000` rows by default and extraction is
single-worker by default because concurrent reads contend on the Seagate disk.
DuckDB is configured with a `3GB` memory limit, four threads, disabled
insertion-order preservation, and a run-local temporary directory. This leaves
headroom under the 5 GB resident memory budget.

All transient files are below the Seagate run root, including
`scratch/duckdb-temp/`. The scratch tree is removed after the DuckDB connection
closes, including failure cleanup. The local Git checkout contains no source
data or run output.

## Output layout

```text
runs/<run-id>/
├── raw-identities/<pbf-stem>.parquet
├── checkpoints/<pbf-stem>.json
├── members/{website,wikidata}.parquet
├── coverage/overlap/shard-00.parquet ... shard-63.parquet
├── coverage/overlap-summary.parquet
└── manifests/manifest.json
```

The row-level files contain only `osm_type`, `osm_id`, `website`, `wikidata`,
and `overlap_category`. No source text or geometry is copied.
