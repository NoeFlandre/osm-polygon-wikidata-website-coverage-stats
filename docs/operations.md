# Operations

## Environment

Use Python 3.12 with [uv](https://docs.astral.sh/uv/). The project is designed
for bounded local execution: a small bounded number of raw PBFs is streamed in
parallel, Parquet writes are batched, and DuckDB reads source Parquets through
read-only connections.
Do not copy, rewrite, compact, or delete the source trees.

```bash
uv sync --frozen
uv run coverage preflight
uv run coverage run --run-id 20260828-coverage-v5 --workers 8
```

The `run` command extracts all sorted regular raw PBFs, joins successful source
memberships, aggregates global/per-PBF/per-region coverage, renders reports,
and writes a manifest only after required generated Parquets, schemas, and
cross-counts validate. Global coverage is always materialized as 64 shards;
zero-result PBFs still have schema-only occurrence and failure shards. Use a
coverage-only run for the fast identity/provenance pass. Add
`--with-geometry` for assembled geometries and area metrics.

Aggregation uses the run-local `scratch/duckdb-temp/` directory on the Seagate
project volume, disables insertion-order preservation, uses four DuckDB
threads, and stops spilling at 100 GB. This keeps large joins recoverable
without filling the Mac system volume. The spill directory is removed after
DuckDB closes, including after an aggregation error; a resumed run also clears
any stale spill directory left by an abrupt process termination.

Extraction is resumable at PBF boundaries. The default `--resume` mode stores
one atomic JSON checkpoint per completed PBF under `checkpoints/`; a checkpoint
is accepted only when the source filename, size, mtime, occurrence/failure
counts, and both source shard families still agree. Missing or incomplete
outputs are cleaned for that PBF and rescanned. Membership, aggregation,
reports, and the completion manifest are also rebuilt atomically on resume, so
an interruption in any later stage can reuse the completed extraction. Use
`--fresh` with a new run ID to force a clean run.

For example, rerun the same interrupted run with:

```bash
uv run coverage run --run-id 20260828-coverage-v5 --workers 8 --resume
```

Use a new ID and `--fresh` when inputs or processing options intentionally
change. An existing aggregate output is never overwritten during a fresh run.

## Storage contract

Input roots are read-only. All run outputs must be below
`/Volumes/Seagate M3/projects/osm-polygon-wikidata-website-coverage-stats/runs/`.
The local Git repository contains code, documentation, tests, and tiny
fixtures only. Raw PBFs, source Parquets, fetched text, credentials, caches,
and run outputs are excluded from Git.

## Hugging Face staging

After a run is complete, stage only its compact coverage and summary outputs:

```bash
uv run coverage stage-hf \
  /Volumes/Seagate\ M3/projects/osm-polygon-wikidata-website-coverage-stats/runs/20260828-coverage-v5 \
  --destination /private/tmp/osm-polygon-coverage-hf
```

Inspect every staged Parquet schema before upload. The staging boundary rejects
full geometry, text fields, raw inputs, cache directories, credentials, and
arbitrary paths. The public dataset is
`NoeFlandre/osm-polygon-wikidata-website-coverage-stats`.

## Troubleshooting

If a PBF is missing, unreadable, or changes size/mtime while it is being
scanned, the run fails closed and no complete manifest is written. If source
schemas do not contain the required status/text columns, source membership
loading stops before any source write. A busy Hugging Face Dataset Viewer does
not by itself mean files are missing; verify the remote file inventory and
Parquet schemas directly.
