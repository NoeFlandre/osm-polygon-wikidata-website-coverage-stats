# Operations

## Environment

Use Python 3.12 with [uv](https://docs.astral.sh/uv/). The project is designed
for bounded local execution: one raw PBF is streamed at a time, Parquet writes
are batched, and DuckDB reads source Parquets through read-only connections.
Do not copy, rewrite, compact, or delete the source trees.

```bash
uv sync --frozen
uv run coverage preflight
uv run coverage run --run-id 20260827-coverage-v1
```

The `run` command extracts all sorted regular raw PBFs, joins successful source
memberships, aggregates global/per-PBF/per-region coverage, renders reports,
and writes a manifest only after generated Parquets validate. Use a fresh run
ID for each attempt; an existing aggregate output is never overwritten.

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
  /Volumes/Seagate\ M3/projects/osm-polygon-wikidata-website-coverage-stats/runs/20260827-coverage-v1 \
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
