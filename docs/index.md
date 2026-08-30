# OSM polygon website/Wikidata overlap

This project measures which members of one raw OpenStreetMap polygon universe
have successful text in the website dataset, the Wikidata-derived dataset, or
both.

The raw universe is built from all regular `*.osm.pbf` files under:

`/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/raw`

It includes structurally closed ways and relations with structural
`type=multipolygon` or `type=boundary`. Nodes and open ways are excluded. The
presence or absence of `wikidata`, `wikipedia`, or `website` tags does not
affect the denominator.

The two read-only processed inputs are:

- `/Volumes/Seagate M3/projects/osm-polygon-wikidata-only/processed_v2`
- `/Volumes/Seagate M3/projects/osm-polygon-website-tag-data/runs/geofabrik-website-v1`

All generated files, checkpoints, and transient DuckDB spill are kept under:

`/Volumes/Seagate M3/projects/osm-polygon-wikidata-website-coverage-stats`

## Result

Every unique raw identity is assigned exactly one category:

| Category | Website | Wikidata |
| --- | ---: | ---: |
| `neither` | no | no |
| `website_only` | yes | no |
| `wikidata_only` | no | yes |
| `both` | yes | yes |

The [methodology](methodology.md) defines membership and the denominator. The
[operations](operations.md) page documents the resumable local run. The
[architecture](architecture.md) page explains the efficient streaming and
partition-first join design.
