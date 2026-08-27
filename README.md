# OSM polygon Wikidata and website coverage

This project measures coverage of the complete valid raw OpenStreetMap polygon
universe by successful text in separate website, Wikipedia, and Wikivoyage
datasets. It includes assembled closed ways and `multipolygon`/`boundary`
relations only; nodes and content-tag filtering are out of scope.

Project documentation is published at
<https://noeflandre.github.io/osm-polygon-wikidata-website-coverage-stats/>.
Source code is at
<https://github.com/NoeFlandre/osm-polygon-wikidata-website-coverage-stats>.
The compact derived dataset is at
<https://huggingface.co/datasets/NoeFlandre/osm-polygon-wikidata-website-coverage-stats>.

## Quick start

```bash
uv sync --frozen
uv run coverage preflight
uv run coverage run --run-id 20260827-coverage-v1
```

The configured source trees are read-only. Run data belongs on the Seagate
volume and is never committed to Git. The public dataset contains only OSM
identity/provenance, geometry descriptors, successful-text source flags,
overlap categories, and compact summaries. It contains no raw PBF, full
geometry, or source text.

See `docs/methodology.md` for exact predicates and denominator rules. Code and
documentation are Apache-2.0; OSM-derived artifacts carry ODbL and
OpenStreetMap attribution.
