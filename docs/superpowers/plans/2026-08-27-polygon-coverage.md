# Polygon Coverage Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a streaming, reproducible analysis of all valid raw OSM polygon features and publish compact coverage Parquet artifacts for website, Wikipedia, and Wikivoyage coverage.

**Architecture:** Stream all raw PBFs through libosmium’s area assembler, normalize geometry with Shapely and pyproj, write bounded per-PBF occurrence shards, and use DuckDB for external-table deduplication and source joins. Keep source trees read-only, write every run artifact under the Seagate project data root, and stage only compact derived coverage rows for Hugging Face.

**Tech Stack:** Python 3.12, uv, libosmium, PyArrow, DuckDB, Shapely, pyproj, Typer, Rich, Matplotlib, pytest, pytest-cov, Ruff, ty, mutmut, radon, pre-commit, Just, MkDocs Material, Docker, GitHub Actions.

---

## File map

Create the following focused files:

- `pyproject.toml` — package metadata, runtime/dev dependencies, Ruff, ty, pytest, coverage, mutmut, and build configuration.
- `justfile` — reproducible sync, test, lint, typecheck, docs, CRAP, mutation, and full QA commands.
- `LICENSE` — Apache-2.0 for code and documentation.
- `CITATION.cff` — GitHub citation metadata.
- `.pre-commit-config.yaml` — Ruff, formatting, YAML, TOML, and whitespace hooks.
- `Dockerfile` — small Python runtime with the locked package installed.
- `.github/workflows/ci.yml` — locked quality checks without real-data scans.
- `.github/workflows/docs.yml` — strict MkDocs build and GitHub Pages publication.
- `mkdocs.yml`, `docs/index.md`, `docs/methodology.md`, `docs/operations.md`, `docs/architecture.md` — public documentation.
- `src/osm_polygon_wikidata_website_coverage/config/paths.py` — validated immutable input paths and Seagate-only output paths.
- `src/osm_polygon_wikidata_website_coverage/domain/identity.py` — OSM identities, occurrence provenance, and duplicate ordering.
- `src/osm_polygon_wikidata_website_coverage/domain/coverage.py` — source flags and the eight overlap categories.
- `src/osm_polygon_wikidata_website_coverage/domain/geometry.py` — GeoJSON normalization and geometry metrics.
- `src/osm_polygon_wikidata_website_coverage/io/pbf.py` — streaming libosmium area extraction.
- `src/osm_polygon_wikidata_website_coverage/io/parquet.py` — bounded, schema-checked Parquet writers.
- `src/osm_polygon_wikidata_website_coverage/sources/website.py` — DuckDB website-success query contract.
- `src/osm_polygon_wikidata_website_coverage/sources/wikimedia.py` — DuckDB Wikipedia/Wikivoyage-success query contract.
- `src/osm_polygon_wikidata_website_coverage/pipeline/extract.py` — per-PBF occurrence and failure orchestration.
- `src/osm_polygon_wikidata_website_coverage/pipeline/aggregate.py` — global deduplication, joins, conflicts, and summaries.
- `src/osm_polygon_wikidata_website_coverage/reporting/render.py` — deterministic JSON, Markdown, and static chart artifacts.
- `src/osm_polygon_wikidata_website_coverage/cli.py` — thin Typer command composition.
- `tests/` — mirrored unit, contract, acceptance, architecture, and quality tests.

Modify only the new project files above and generated lock/docs artifacts. Never add source PBFs, source Parquets, raw text, credentials, or Seagate run outputs to Git.

### Task 1: Establish the package and storage contract

**Files:**
- Create: `pyproject.toml`, `justfile`, `LICENSE`, `CITATION.cff`, `.pre-commit-config.yaml`, `Dockerfile`.
- Create: `src/osm_polygon_wikidata_website_coverage/__init__.py` and package `__init__.py` files.
- Create: `src/osm_polygon_wikidata_website_coverage/config/paths.py`.
- Test: `tests/config/test_paths.py`, `tests/architecture/test_tooling.py`.

- [ ] **Step 1: Write RED tests for path validation and project metadata.**

```python
def test_paths_default_to_the_seagate_project_root() -> None:
    paths = DataPaths.from_values()
    assert paths.data_root == Path(
        "/Volumes/Seagate M3/projects/osm-polygon-wikidata-website-coverage-stats"
    )
    assert paths.raw_pbf_root.name == "raw"


def test_paths_reject_an_output_root_outside_seagate(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Seagate"):
        DataPaths.from_values(data_root=tmp_path)


def test_source_paths_are_read_only_and_must_be_directories(tmp_path: Path) -> None:
    source = tmp_path / "raw"
    source.mkdir()
    paths = DataPaths.from_values(
        data_root=DEFAULT_DATA_ROOT,
        raw_pbf_root=source,
        wikidata_root=source,
        website_root=source,
    )
    assert paths.raw_pbf_root == source.resolve()
    assert paths.source_paths == (source.resolve(), source.resolve(), source.resolve())
```

- [ ] **Step 2: Run the focused tests to verify RED.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/config/test_paths.py -q`

Expected: collection failure because `DataPaths` and the project package do not exist.

- [ ] **Step 3: Add the minimal package metadata and path implementation.**

Declare the package name `osm-polygon-wikidata-website-coverage`, Python `>=3.12`, runtime dependencies `duckdb`, `matplotlib`, `osmium`, `pyarrow`, `pyproj`, `rich`, `shapely`, and `typer`, and development dependencies `mkdocs-material`, `mutmut`, `pre-commit`, `pytest`, `pytest-cov`, `radon`, `ruff`, and `ty`.

Implement a frozen `DataPaths` object with the four `Path` fields
`data_root`, `raw_pbf_root`, `wikidata_root`, and `website_root`. Its
`from_values` classmethod accepts those four keyword-only `Path | str`
arguments, resolves them without dereferencing symlinks, validates the
Seagate output-root and read-only source contracts, and returns the object.
Its `run_root(run_id: str)` method returns
`data_root / "runs" / run_id` after rejecting empty or path-traversal run IDs.

Resolve paths without dereferencing source symlinks, reject a data root outside the required Seagate root, reject source/output overlap, and expose `source_paths` for read-only preflight.

- [ ] **Step 4: Run the focused tests to verify GREEN.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/config/test_paths.py -q`

Expected: all path-contract tests pass.

- [ ] **Step 5: Add the lockfile and baseline tooling contracts.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv lock`.

Add `just sync`, `just test`, `just lint`, `just typecheck`, `just docs`, `just crap`, `just mutation`, and `just qa` recipes. Add architecture tests that assert the Dockerfile, citation file, Apache license, MkDocs config, and package import exist.

- [ ] **Step 6: Commit the foundation.**

```bash
git add pyproject.toml uv.lock justfile LICENSE CITATION.cff .pre-commit-config.yaml Dockerfile src tests
git commit -m "build: establish coverage analysis foundation"
```

### Task 2: Implement pure identity, coverage, and overlap domains

**Files:**
- Create: `src/osm_polygon_wikidata_website_coverage/domain/identity.py`.
- Create: `src/osm_polygon_wikidata_website_coverage/domain/coverage.py`.
- Test: `tests/domain/test_identity.py`, `tests/domain/test_coverage.py`.

- [ ] **Step 1: Write RED tests for identity ordering and all eight categories.**

The identity test constructs three complete `Occurrence` values for
`("way", 7)`: version 3 from `z.osm.pbf`, version 4 with an older timestamp
from `z.osm.pbf`, and the same version/timestamp from `a.osm.pbf`. It asserts
that `canonical_occurrence` returns the last value. A parametrized test covers
these exact results from `overlap_category`: `(False, False, False)` is
`neither`, `(True, False, False)` is `website_only`, `(False, True, False)` is
`wikipedia_only`, `(False, False, True)` is `wikivoyage_only`, `(True, True,
False)` is `website_wikipedia_only`, `(True, False, True)` is
`website_wikivoyage_only`, `(False, True, True)` is
`wikipedia_wikivoyage_only`, and `(True, True, True)` is `all_three`.

- [ ] **Step 2: Run the tests to verify RED.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/domain/test_identity.py tests/domain/test_coverage.py -q`

Expected: collection failure for missing domain functions.

- [ ] **Step 3: Implement the minimal pure domain functions.**

Define `OsmIdentity(osm_type, osm_id)`, `Occurrence`, `GeometryFailure`, `SourceFlags`, `canonical_occurrence`, `overlap_category`, and `coverage_flags`. Restrict `osm_type` to `way` and `relation`; sort duplicate occurrences by `(-osm_version, -osm_timestamp, source_pbf)`; return stable snake-case category identifiers.

- [ ] **Step 4: Run the tests to verify GREEN.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/domain/test_identity.py tests/domain/test_coverage.py -q`

Expected: all identity and overlap tests pass.

- [ ] **Step 5: Commit the pure domain.**

```bash
git add src/osm_polygon_wikidata_website_coverage/domain tests/domain
git commit -m "feat: define polygon identity and coverage categories"
```

### Task 3: Implement geometry normalization and raw-PBF streaming

**Files:**
- Create: `src/osm_polygon_wikidata_website_coverage/domain/geometry.py`.
- Create: `src/osm_polygon_wikidata_website_coverage/io/pbf.py`.
- Create: `src/osm_polygon_wikidata_website_coverage/io/parquet.py`.
- Create: `src/osm_polygon_wikidata_website_coverage/pipeline/extract.py`.
- Test: `tests/domain/test_geometry.py`, `tests/io/test_pbf.py`, `tests/io/test_parquet.py`, `tests/pipeline/test_extract.py`.

- [ ] **Step 1: Write RED tests for geometry and candidate classification.**

```python
def test_normalize_geometry_returns_polygon_or_multipolygon_metrics() -> None:
    result = normalize_geometry(json.dumps({"type": "Polygon", "coordinates": [_square()]}))
    assert result.geometry_type == "Polygon"
    assert result.area_m2 > 0
    assert result.centroid_lon == pytest.approx(0.5)


def test_closed_way_requires_three_distinct_nodes_and_repeated_first_node() -> None:
    assert is_closed_way((1, 2, 3, 1)) is True
    assert is_closed_way((1, 2, 1)) is False
    assert is_closed_way((1, 2, 3, 4)) is False


def test_relation_candidate_accepts_multipolygon_and_boundary_only() -> None:
    assert relation_kind({"type": "multipolygon"}) == "multipolygon"
    assert relation_kind({"type": "boundary"}) == "boundary"
    assert relation_kind({"type": "route"}) is None
```

- [ ] **Step 2: Run the focused tests to verify RED.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/domain/test_geometry.py tests/io/test_pbf.py -q`

Expected: collection failure for missing geometry and PBF symbols.

- [ ] **Step 3: Implement the geometry module.**

Parse libosmium GeoJSON, accept only Polygon/MultiPolygon, orient rings, repair non-empty invalid shapes with `buffer(0)` only when the result remains Polygon/MultiPolygon, reject empty/degenerate/antimeridian geometry, round coordinates to seven decimals, calculate WGS84 geodesic area with `pyproj.Geod`, and calculate the local Lambert azimuthal equal-area centroid. Return a frozen `NormalizedGeometry` containing canonical GeoJSON, geometry type, centroid, bbox, area, and a SHA-256 geometry hash.

- [ ] **Step 4: Implement the streaming PBF reader and bounded Parquet writer.**

Define an osmium `SimpleHandler` with an `area()` callback. Classify an assembled area as a way when `area.from_way()` is true and as a relation when `area.is_multipolygon()` is true. For ways, rely on the assembled area and apply the closed-way structural rule. For relations, retain only `type=multipolygon` and `type=boundary`. Do not inspect content tags for inclusion. Call `GeoJSONFactory().create_multipolygon(area)`, normalize it, and emit an `Occurrence` or `GeometryFailure`.

Write batches with `pyarrow.parquet.ParquetWriter` to temporary files beneath the run directory, then atomically promote them. The occurrence schema must include `osm_type`, `osm_id`, `source_pbf`, `region`, OSM version/timestamp, relation kind, geometry type, normalized geometry, centroid, bbox, area, area bucket, and geometry hash. Failure rows must include identity when available, source PBF, candidate kind, failure kind, and diagnostic message.

- [ ] **Step 5: Add extraction orchestration and tests for no source mutation.**

`extract_all(paths, run_id)` must sort regular raw PBFs, reject missing/unreadable files, stream one PBF at a time by default, write one occurrence and one failure shard per source, and record source size/mtime before and after each scan. A changed input raises and leaves the run incomplete. Tests use a fake handler and temporary small Parquet outputs; they assert no source write calls and deterministic row order.

- [ ] **Step 6: Run geometry and extraction tests to verify GREEN.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/domain/test_geometry.py tests/io/test_pbf.py tests/io/test_parquet.py tests/pipeline/test_extract.py -q`

Expected: all focused geometry, writer, and extraction tests pass.

- [ ] **Step 7: Commit raw extraction.**

```bash
git add src/osm_polygon_wikidata_website_coverage/domain src/osm_polygon_wikidata_website_coverage/io src/osm_polygon_wikidata_website_coverage/pipeline tests/domain tests/io tests/pipeline
git commit -m "feat: stream and normalize raw polygon geometry"
```

### Task 4: Add read-only website and Wikimedia membership readers

**Files:**
- Create: `src/osm_polygon_wikidata_website_coverage/sources/website.py`.
- Create: `src/osm_polygon_wikidata_website_coverage/sources/wikimedia.py`.
- Create: `src/osm_polygon_wikidata_website_coverage/pipeline/join.py`.
- Test: `tests/sources/test_website.py`, `tests/sources/test_wikimedia.py`, `tests/pipeline/test_join.py`.

- [ ] **Step 1: Write RED tests for exact success predicates.**

Create a temporary website Parquet fixture with three complete rows: a
nonempty successful `website_text`, a nonempty successful
`contact_website_text` while the main fetch is empty, and an empty successful
main fetch. Assert that only the first two `(osm_type, osm_id)` identities are
returned. Create a processed-V2 fixture with one successful Wikipedia document,
one successful Wikivoyage document, one `fetch_status = 'ok'` document whose
`full_text` is empty, and one failed document; assert that a Wikipedia query
returns only the Wikipedia identity and never the Wikivoyage identity.

- [ ] **Step 2: Run the tests to verify RED.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/sources tests/pipeline/test_join.py -q`

Expected: collection failure for missing source readers.

- [ ] **Step 3: Implement the read-only DuckDB source queries.**

Website query: read only `polygons/*.parquet`; set membership when either website or contact text has status `success` and `length(trim(coalesce(text, ''))) > 0`.

Wikimedia query: read only `polygon_document_links/*.parquet` and the project-specific `wikipedia/documents/*.parquet` or `wikivoyage/documents/*.parquet`; join on `(project, document_id)`, filter `fetch_status = 'ok'` and non-empty `full_text`, and select distinct `(osm_type, osm_id)`. Never return text columns. Include direct Wikipedia-tag links because they are present in the V2 link table.

The readers must reject missing source directories, validate required columns before querying, use read-only DuckDB connections, and write no files under source roots.

- [ ] **Step 4: Implement the join facade.**

`load_source_membership(paths)` returns three sorted temporary Parquet membership tables beneath the run directory plus source diagnostics. It must record source row counts, successful-key counts, duplicate-key counts, and source keys not present in the raw global table after aggregation.

- [ ] **Step 5: Run focused tests to verify GREEN.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/sources tests/pipeline/test_join.py -q`

Expected: all source-predicate, project-separation, and read-only tests pass.

- [ ] **Step 6: Commit source membership.**

```bash
git add src/osm_polygon_wikidata_website_coverage/sources src/osm_polygon_wikidata_website_coverage/pipeline/join.py tests/sources tests/pipeline/test_join.py
git commit -m "feat: derive read-only text coverage memberships"
```

### Task 5: Implement global aggregation, conflicts, and summaries

**Files:**
- Modify: `src/osm_polygon_wikidata_website_coverage/pipeline/aggregate.py`.
- Create: `src/osm_polygon_wikidata_website_coverage/pipeline/run.py`.
- Test: `tests/pipeline/test_aggregate.py`, `tests/pipeline/test_run.py`.

- [ ] **Step 1: Write RED tests for deduplication and complete overlap accounting.**

The deduplication test writes two complete occurrence rows for `("way", 1)`:
`b.osm.pbf` and `a.osm.pbf`, with identical version and timestamp. It calls
`aggregate_run(occurrence_root=tmp_path, membership_root=tmp_path / "members")`
and asserts that the canonical row uses `a.osm.pbf` while its sorted source
list contains both PBF names. A summary fixture supplies one valid identity for
each of the eight categories and asserts that all eight category names are
present and their counts sum to `valid_universe_count`.

- [ ] **Step 2: Run the tests to verify RED.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/pipeline/test_aggregate.py tests/pipeline/test_run.py -q`

Expected: collection failure for aggregation symbols.

- [ ] **Step 3: Implement DuckDB aggregation.**

Create an external-table query over occurrence shards and membership shards. Deduplicate with `row_number() over (partition by osm_type, osm_id order by osm_version desc, osm_timestamp desc, source_pbf asc)`. Add sorted JSON source-PBF lists, conflict rows, three membership flags, `covered_by_any_text`, and `overlap_category`.

Write:

```text
`$RUN_ROOT/coverage/global/*.parquet`
`$RUN_ROOT/coverage/by-pbf/*.parquet`
`$RUN_ROOT/summaries/global.parquet`
`$RUN_ROOT/summaries/by-source-pbf.parquet`
`$RUN_ROOT/summaries/by-region.parquet`
`$RUN_ROOT/summaries/by-overlap.parquet`
`$RUN_ROOT/summaries/geometry-failures.parquet`
`$RUN_ROOT/summaries/conflicts.parquet`
```

Use one bounded set of global shards (64, selected by a stable hash of `(osm_type, osm_id)`) and atomic promotion. Per-PBF summaries use occurrence rows and may count one identity once per PBF; global summaries count identities once.

- [ ] **Step 4: Implement descriptive statistics.**

Compute valid-universe denominator, source totals/rates, all eight overlap categories, pairwise/triple intersections, OSM type counts, geometry type counts, area total/min/max/mean/median/25th/75th/95th percentiles, and per-PBF/per-region equivalents. Geometry-failure counts remain outside the valid denominator.

- [ ] **Step 5: Implement completion manifests.**

Record run ID, input roots, sorted PBF inventory with size/mtime/SHA-256, source Parquet inventories, schema versions, row counts, hash of every generated Parquet shard, summary totals, and a terminal status. Write the manifest atomically only after all outputs validate.

- [ ] **Step 6: Run aggregation tests to verify GREEN.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/pipeline/test_aggregate.py tests/pipeline/test_run.py -q`

Expected: all aggregation, conflict, summary, and manifest tests pass.

- [ ] **Step 7: Commit aggregation.**

```bash
git add src/osm_polygon_wikidata_website_coverage/pipeline tests/pipeline
git commit -m "feat: aggregate global polygon coverage"
```

### Task 6: Add reporting, CLI, and public documentation

**Files:**
- Create: `src/osm_polygon_wikidata_website_coverage/reporting/render.py`.
- Create: `src/osm_polygon_wikidata_website_coverage/cli.py`.
- Create: `mkdocs.yml`, `docs/index.md`, `docs/methodology.md`, `docs/operations.md`, `docs/architecture.md`.
- Test: `tests/reporting/test_render.py`, `tests/cli/test_cli.py`, `tests/architecture/test_docs.py`.

- [ ] **Step 1: Write RED tests for deterministic reports and CLI safety.**

The report test builds a complete summary mapping containing the valid
denominator and all eight categories, calls `render_markdown(summary, output)`
with a temporary output path, and asserts that the resulting text includes
`Valid raw polygon universe`, `website + Wikipedia only`, and `Wikivoyage`.
The CLI test uses Typer's `CliRunner` and a `DataPaths` fixture whose raw root
contains a fixture PBF; it invokes `preflight` with that root, expects exit code
zero, and asserts that no `*.coverage*` file appears under the input root.

- [ ] **Step 2: Run the tests to verify RED.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/reporting/test_render.py tests/cli/test_cli.py tests/architecture/test_docs.py -q`

Expected: collection failure for reporting, CLI, or docs contracts.

- [ ] **Step 3: Implement reports and charts.**

Generate deterministic `summary.json`, `report.md`, `coverage_categories.png`, and `area_distributions.png` under the run’s `reports/` directory. Use static Matplotlib output only; do not create an interactive web application. Do not place raw geometry or source text in reports.

- [ ] **Step 4: Implement the thin CLI.**

Provide:

```text
coverage preflight   validate roots and list source inventory
coverage run         extract, join, aggregate, render, and verify one run
coverage stage-hf    copy only compact coverage shards and summaries to HF staging
```

All commands accept explicit path overrides but default to the approved paths. `run` writes only to the Seagate data root. `stage-hf` refuses any input outside the completed run’s compact coverage and summary outputs.

- [ ] **Step 5: Write MkDocs documentation.**

Document the set definitions, geometry method, exact success predicates, eight overlap categories, denominator/failure treatment, source licensing, storage paths, CLI usage, bounded execution, and HF artifact contents. State explicitly that source trees are read-only and no full text/raw PBF/full geometry is published to HF.

- [ ] **Step 6: Run reporting, CLI, and docs tests to verify GREEN.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/reporting tests/cli tests/architecture/test_docs.py -q`.

Expected: all focused tests pass.

- [ ] **Step 7: Commit the CLI and documentation.**

```bash
git add src/osm_polygon_wikidata_website_coverage/reporting src/osm_polygon_wikidata_website_coverage/cli.py mkdocs.yml docs tests/reporting tests/cli tests/architecture/test_docs.py
git commit -m "feat: add coverage reports and public documentation"
```

### Task 7: Add HF staging, repository contracts, and CI

**Files:**
- Create: `tests/publishing/test_hf_staging.py`, `tests/architecture/test_ci.py`, `tests/architecture/test_docker.py`.
- Modify: `src/osm_polygon_wikidata_website_coverage/cli.py`, `README.md`, `docs/index.md`.

- [ ] **Step 1: Write RED tests for the public artifact boundary.**

Build a complete temporary run fixture with compact coverage shards and the
required summary files, call `stage_hf(completed_run, tmp_path / "hf")`, and
inspect every staged Parquet schema. Assert that no field name contains
`website_text` or `full_text`, that no field is named `geometry`, and that the
staging directory contains `README.md`.

- [ ] **Step 2: Run the tests to verify RED.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/publishing/test_hf_staging.py tests/architecture/test_ci.py tests/architecture/test_docker.py -q`

Expected: collection failure for missing staging and architecture symbols.

- [ ] **Step 3: Implement HF staging.**

Stage 64 compact coverage Parquet shards, summary Parquets/JSON, the dataset card, `CITATION.cff`, and an attribution/limitations file. Validate columns against the compact schema and reject raw geometry, text, cache, credentials, and arbitrary paths. The HF target is the public dataset `NoeFlandre/osm-polygon-wikidata-website-coverage-stats`.

- [ ] **Step 4: Add README, CI, Docker, and strict docs contracts.**

Document the project’s public GitHub and HF URLs, Apache code license versus ODbL-derived data attribution, and exact reproducibility commands. CI runs locked Ruff, ty, pytest, and strict MkDocs on fixtures only. Docker installs the wheel and runs `coverage --help`; it does not include or copy Seagate data.

- [ ] **Step 5: Run the publishing and architecture tests to verify GREEN.**

Run: `UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest tests/publishing tests/architecture -q`.

Expected: all public-boundary, Docker, CI, and documentation-configuration tests pass.

- [ ] **Step 6: Commit the public artifact contract.**

```bash
git add README.md docs src/osm_polygon_wikidata_website_coverage/cli.py tests/publishing tests/architecture
git commit -m "feat: define public HF coverage artifacts"
```

### Task 8: Execute complete local quality gates and real-data analysis

**Files:**
- Modify only implementation files found by fresh failing tests.
- Generate run artifacts only under `/Volumes/Seagate M3/projects/osm-polygon-wikidata-website-coverage-stats/runs/20260827-coverage-v1/`.

- [ ] **Step 1: Run the full regression and static gates.**

```bash
UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv sync --frozen
UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run ruff format --check .
UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run ruff check .
UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run ty check src tests
UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run pytest --cov=src/osm_polygon_wikidata_website_coverage --cov-branch --cov-report=term-missing --cov-fail-under=100
UV_CACHE_DIR=/private/tmp/osm-polygon-coverage-uv-cache uv run mkdocs build --strict
```

Expected: all tests pass, no Ruff/ty errors, 100% line and branch coverage for project code, and strict MkDocs succeeds.

- [ ] **Step 2: Run CRAP and mutation gates from fresh reports.**

Use the repository’s scripts to measure every production function and run mutmut with bounded workers. Reject any CRAP score `>= 6`, `survived`, `suspicious`, `timeout`, `unviable`, or unchecked mutant. Do not reuse stale mutation caches after source/test changes.

- [ ] **Step 3: Run a bounded CLI smoke fixture.**

Run the full pipeline against a temporary synthetic source fixture with one closed way, one multipolygon relation, one boundary relation, one rejected open way, all eight coverage combinations, one duplicate identity, and one geometry failure. Verify exact counts, report contents, deterministic rerun hashes, and no writes outside the fixture output root.

- [ ] **Step 4: Run the complete real-data analysis.**

Run all current raw PBFs in sorted order with one extraction worker and bounded Parquet batches. Read the source website V1 and Wikidata V2 artifacts through read-only DuckDB queries. Verify source mtimes/sizes after the run, manifest completeness, global/per-PBF totals, overlap sums, failure counts, and output hashes. Preserve all logs and reports under the Seagate run directory.

- [ ] **Step 5: Stage and independently inspect HF artifacts.**

Read every staged Parquet schema and summary, verify no text/full geometry/raw input appears, verify totals against the completed run, and inspect the generated dataset card before upload.

- [ ] **Step 6: Commit the completed implementation and evidence references.**

```bash
git add README.md CITATION.cff LICENSE Dockerfile mkdocs.yml justfile pyproject.toml uv.lock .pre-commit-config.yaml .github src tests docs
git commit -m "feat: complete polygon coverage analysis pipeline"
```

### Task 9: Create and publish the approved public repositories

- [ ] **Step 1: Create the Hugging Face dataset repository.**

Run the authenticated CLI command:

```bash
hf repos create NoeFlandre/osm-polygon-wikidata-website-coverage-stats --type dataset --exist-ok
```

- [ ] **Step 2: Upload only the reviewed HF staging directory.**

Run:

```bash
hf upload-large-folder NoeFlandre/osm-polygon-wikidata-website-coverage-stats /Volumes/Seagate\ M3/projects/osm-polygon-wikidata-website-coverage-stats/runs/20260827-coverage-v1/hf-staging --include 'data/**' --include 'README.md' --include 'CITATION.cff' --include 'LICENSE' --num-workers 2
```

Verify the remote Parquet inventory and Dataset Viewer metadata after indexing. Record the HF commit/revision and uploaded file hashes in the Seagate manifest.

- [ ] **Step 3: Create the GitHub repository and synchronize `main`.**

After GitHub authentication is available, run:

```bash
gh repo create NoeFlandre/osm-polygon-wikidata-website-coverage-stats --public --source . --remote origin --description "Coverage analysis of raw OSM polygons against website, Wikipedia, and Wikivoyage text datasets"
git push -u origin main
```

Verify `HEAD == origin/main`, `origin/HEAD -> origin/main`, the repository visibility, and a clean worktree. Do not upload Seagate data or generated run directories.

- [ ] **Step 4: Verify the public handoff.**

Check GitHub source/docs URLs, HF dataset files and schema, citation metadata, license/attribution text, and the final local/remote commit identities. Report any unavailable Docker daemon or remote indexing delay as an environment limitation rather than a passed gate.
