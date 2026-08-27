# OSM polygon text coverage

This project measures how much of the raw OpenStreetMap polygon universe has
successful text in the website, Wikipedia, and Wikivoyage datasets. It treats
the website and Wikimedia datasets as subsets of the same raw polygon universe.

The universe is the set of unique `(osm_type, osm_id)` identities represented by
valid assembled closed ways and supported polygon relations in every regular
raw `*.osm.pbf` file. Nodes are never included. A relation is supported when its
structural `type` is `multipolygon` or `boundary`. Content tags such as
`wikidata`, `wikipedia`, and `website` do not decide whether a raw polygon is in
the denominator.

The analysis uses the read-only inputs selected for this project:

- raw PBFs: `/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/raw`
- Wikidata V2 artifacts: `/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/processed_v2`
- website V1 artifacts: `/Volumes/Seagate M3/projects/osm-polygon-website-tag-data/runs/geofabrik-website-v1`

Run artifacts live only under
`/Volumes/Seagate M3/projects/osm-polygon-wikidata-website-coverage-stats`.
The source trees are never modified.

## Outputs

Each completed run contains a compact global coverage table, per-PBF and
per-region summaries, overlap counts, area/type statistics, geometry-failure
audits, a Markdown report, deterministic static charts, and a completion
manifest. The public derived dataset is
[the Hugging Face dataset](https://huggingface.co/datasets/NoeFlandre/osm-polygon-wikidata-website-coverage-stats).

The public artifact contains identity, provenance, geometry descriptors, source
flags, and overlap categories. It does not contain raw PBF bytes, full geometry,
full text from any source, website text, Wikipedia text, Wikivoyage text, fetch
caches, or private run state.

See [Methodology](methodology.md) for the set definitions and predicates and
[Operations](operations.md) for reproducible commands.
