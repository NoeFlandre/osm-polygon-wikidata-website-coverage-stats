# OSM polygon Wikidata and website coverage

This project measures coverage of the complete raw structural OpenStreetMap
polygon universe by successful text in separate website, Wikipedia, and
Wikivoyage datasets. It includes structurally closed ways and
`multipolygon`/`boundary` relations only; nodes and content-tag filtering are
out of scope. An optional geometry mode adds assembled geometry validation.

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
uv run coverage run --run-id 20260828-coverage-v5 --workers 8
```

The configured source trees are read-only. Run data belongs on the Seagate
volume and is never committed to Git. The public dataset contains only OSM
identity/provenance (including the distinct contributing-PBF count/list),
geometry descriptors, successful-text source flags, overlap categories, and
compact summaries. It contains no raw PBF, full geometry, or source text.

The default run is coverage-only: it reads polygon identity and provenance
from the raw PBFs without assembling geometry, which makes the long extraction
stage substantially faster. Runs are resumable by default. Reusing the same
run ID with `--resume` skips PBFs whose checkpoint and output shards are
complete; `--fresh` requires a new run ID. Use `--with-geometry` when full
geometry metrics are needed.

See `docs/methodology.md` for exact predicates and denominator rules. Code and
documentation are Apache-2.0; OSM-derived artifacts carry ODbL and
OpenStreetMap attribution.
