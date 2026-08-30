# OSM polygon website/Wikidata overlap

This project computes the overlap between two successful-text datasets over one
raw OpenStreetMap polygon universe. The universe contains every structurally
closed way and every `type=multipolygon` or `type=boundary` relation found in
the raw PBFs. Nodes, open ways, geometry assembly, and content-tag filtering
are out of scope.

The source trees are strict read-only inputs. Code stays in this checkout;
generated data and temporary spill stay under the Seagate project directory.

Project documentation is published at
<https://noeflandre.github.io/osm-polygon-wikidata-website-coverage-stats/>.
Source code is at
<https://github.com/NoeFlandre/osm-polygon-wikidata-website-coverage-stats>.

## Inputs

- raw PBFs: `/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/raw`
- Wikidata processed V2: `/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/processed_v2`
- website processed V1: `/Volumes/Seagate M3/projects/osm-polygon-website-tag-data/runs/geofabrik-website-v1`

The processed Wikidata link files already store the original OSM IDs. The
pipeline therefore joins on their `osm_type` and `osm_id` directly.

## Run

Use an already provisioned Python 3.12 environment. The run itself performs
no downloads and writes only to the configured Seagate data root:

```bash
PYTHONPATH=src python -m osm_polygon_wikidata_website_coverage.cli preflight
PYTHONPATH=src python -m osm_polygon_wikidata_website_coverage.cli run \
  --run-id 20260829-website-wikidata-overlap-v1 \
  --workers 1 --batch-rows 100000
```

Rerun the same command after an interruption. Completed PBF checkpoints,
membership tables, and overlap outputs are validated and reused. No input
directory is modified.

One extraction worker is the default because concurrent reads contend on the
Seagate disk; the later DuckDB join still uses four bounded threads. Increase
`--workers` only when the storage device benefits from concurrent reads.

## Outputs

Run artifacts are under:

`/Volumes/Seagate M3/projects/osm-polygon-wikidata-website-coverage-stats/runs/<run-id>/`

The useful results are 64 deterministic Parquet shards under
`coverage/overlap/` and `coverage/overlap-summary.parquet`. Each row has the
OSM identity, the two membership flags, and exactly one of `neither`,
`website_only`, `wikidata_only`, or `both`. Raw PBFs and source text are never
copied into the project outputs.

See [Methodology](docs/methodology.md) for the exact predicates and
[Operations](docs/operations.md) for the storage and resume contract.

Code and documentation are Apache-2.0. OSM-derived identities are subject to
OpenStreetMap attribution and ODbL obligations.
