# End-to-End Quality Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the overlap pipeline around small, shared I/O boundaries while preserving every existing public API, output schema, checkpoint field, and overlap result.

**Architecture:** Keep the existing composition root and four-stage flow. Add one atomic-path adapter for ordinary generated artifacts, one shared source-schema validator for the read-only source adapters, and one immutable internal extraction context so orchestration functions do not pass unrelated arguments independently. Keep the identity Parquet writer separate because it intentionally refuses overwrites and has its own streaming lifecycle.

**Tech Stack:** Python 3.12, DuckDB, PyArrow, libosmium, Typer, pytest, pytest-cov, Ruff, ty, Radon/CRAP, mutmut, MkDocs Material, Docker, uv.

---

### Task 1: Establish the atomic-artifact contract

**Files:**
- Create: `src/osm_polygon_wikidata_website_coverage/io/atomic.py`
- Create: `tests/io/test_atomic.py`
- Modify: `src/osm_polygon_wikidata_website_coverage/io/duckdb.py`

- [ ] **Step 1: Write the failing tests**

Add tests for promotion, failure cleanup, stale temporary cleanup, and DuckDB Parquet export:

```python
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from osm_polygon_wikidata_website_coverage.io.atomic import atomic_path
from osm_polygon_wikidata_website_coverage.io.duckdb import export_query


def test_atomic_path_promotes_output_and_cleans_stale_temp(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "result.txt"
    temporary = output.with_name(".result.txt.tmp")
    temporary.parent.mkdir(parents=True)
    temporary.write_text("stale", encoding="utf-8")

    with atomic_path(output) as path:
        assert path == temporary
        path.write_text("fresh", encoding="utf-8")

    assert output.read_text(encoding="utf-8") == "fresh"
    assert not temporary.exists()


def test_atomic_path_keeps_old_output_when_producer_fails(tmp_path: Path) -> None:
    output = tmp_path / "result.txt"
    output.write_text("old", encoding="utf-8")

    with pytest.raises(RuntimeError, match="producer failed"):
        with atomic_path(output) as path:
            path.write_text("partial", encoding="utf-8")
            raise RuntimeError("producer failed")

    assert output.read_text(encoding="utf-8") == "old"
    assert not output.with_name(".result.txt.tmp").exists()


def test_export_query_writes_a_parquet_result(tmp_path: Path) -> None:
    connection = duckdb.connect(database=":memory:")
    try:
        output = tmp_path / "result.parquet"
        export_query(connection, "SELECT 7 AS value", [], output)
    finally:
        connection.close()

    assert pq.read_table(output).to_pylist() == [{"value": 7}]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=.:src /Applications/anaconda3/bin/python -m pytest tests/io/test_atomic.py -q`

Expected: collection fails because `atomic_path` and `export_query` do not yet exist.

- [ ] **Step 3: Write the minimal implementation**

Implement `atomic_path` as a context manager that creates the parent, removes only the sibling `.<name>.tmp`, yields that path, replaces the destination on success, and removes only the temporary path on any failure. Implement `export_query` in `io.duckdb` using that context manager and the existing DuckDB Parquet COPY settings.

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=.:src /Applications/anaconda3/bin/python -m pytest tests/io/test_atomic.py -q`

Expected: `3 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/osm_polygon_wikidata_website_coverage/io/atomic.py src/osm_polygon_wikidata_website_coverage/io/duckdb.py tests/io/test_atomic.py
git commit -m "refactor: centralize atomic artifact output"
```

### Task 2: Reuse atomic output and source-schema validation

**Files:**
- Modify: `src/osm_polygon_wikidata_website_coverage/sources/_files.py`
- Modify: `src/osm_polygon_wikidata_website_coverage/sources/website.py`
- Modify: `src/osm_polygon_wikidata_website_coverage/sources/wikimedia.py`
- Modify: `src/osm_polygon_wikidata_website_coverage/pipeline/join.py`
- Modify: `src/osm_polygon_wikidata_website_coverage/pipeline/overlap.py`
- Modify: `src/osm_polygon_wikidata_website_coverage/pipeline/run.py`
- Modify: `tests/sources/test_sources.py`
- Modify: `tests/pipeline/test_join.py`
- Modify: `tests/pipeline/test_overlap.py`

- [ ] **Step 1: Write the failing test**

Add a direct shared-validator contract:

```python
def test_shared_source_validator_reports_missing_columns(tmp_path: Path) -> None:
    path = tmp_path / "source.parquet"
    write_rows(path, [{"present": 1}])

    with pytest.raises(SourceDatasetError, match="source file .* missing columns: required"):
        validate_columns((path,), frozenset({"required"}), "source")
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `PYTHONPATH=.:src /Applications/anaconda3/bin/python -m pytest tests/sources/test_sources.py::test_shared_source_validator_reports_missing_columns -q`

Expected: collection or attribute failure because `validate_columns` is not yet defined.

- [ ] **Step 3: Implement and refactor minimally**

Add `read_column_names` and `validate_columns` to `sources/_files.py`, preserving existing error wording. Make both adapters delegate to those helpers while retaining private `_column_names` compatibility wrappers. Replace duplicate query-export implementations in `join.py` and `overlap.py` with `io.duckdb.export_query`, keeping module-level `_write_query` aliases so existing callers and tests remain compatible. Use `atomic_path` for JSON manifests, the empty overlap shard, and the summary Parquet. Remove imports made unused by the consolidation.

- [ ] **Step 4: Run focused and full tests**

Run:

```bash
PYTHONPATH=.:src /Applications/anaconda3/bin/python -m pytest tests/io tests/sources tests/pipeline/test_join.py tests/pipeline/test_overlap.py -q
PYTHONPATH=.:src /Applications/anaconda3/bin/python -m pytest --cov=osm_polygon_wikidata_website_coverage --cov-branch --cov-fail-under=100
```

Expected: all focused tests and the complete suite pass with 100% branch coverage.

- [ ] **Step 5: Commit**

```bash
git add src/osm_polygon_wikidata_website_coverage/sources/_files.py src/osm_polygon_wikidata_website_coverage/sources/website.py src/osm_polygon_wikidata_website_coverage/sources/wikimedia.py src/osm_polygon_wikidata_website_coverage/pipeline/join.py src/osm_polygon_wikidata_website_coverage/pipeline/overlap.py src/osm_polygon_wikidata_website_coverage/pipeline/run.py tests/sources/test_sources.py tests/pipeline/test_join.py tests/pipeline/test_overlap.py
git commit -m "refactor: share source validation and artifact writes"
```

### Task 3: Reduce extraction orchestration coupling

**Files:**
- Modify: `src/osm_polygon_wikidata_website_coverage/pipeline/extract.py`
- Modify: `tests/pipeline/test_extract.py`

- [ ] **Step 1: Establish the preservation test**

Run the existing deterministic extraction, resume, mutation-detection, and parallel-worker tests before changing implementation:

```bash
PYTHONPATH=.:src /Applications/anaconda3/bin/python -m pytest tests/pipeline/test_extract.py -q
```

Expected: all extraction tests pass; these tests are the regression contract for checkpoint contents, output names, worker validation, and source-integrity behavior.

- [ ] **Step 2: Refactor only after green**

Introduce a private frozen `_ExtractionContext` containing `run_root`, `batch_rows`, `scanner`, and `checkpoint_root`. Pass that context to `_extract_one`, `_extract_sources`, and `_extract_in_parallel), leaving `extract_all`, `regular_pbf_files`, `scanner_mode`, exception classes, checkpoint JSON, and output paths unchanged. Use the context’s checkpoint-root presence to preserve current checkpoint/no-checkpoint behavior.

- [ ] **Step 3: Run the extraction suite and full suite**

Run:

```bash
PYTHONPATH=.:src /Applications/anaconda3/bin/python -m pytest tests/pipeline/test_extract.py -q
PYTHONPATH=.:src /Applications/anaconda3/bin/python -m pytest --cov=osm_polygon_wikidata_website_coverage --cov-branch --cov-fail-under=100
```

Expected: all tests pass with 100% branch coverage and no checkpoint/output compatibility changes.

- [ ] **Step 4: Commit**

```bash
git add src/osm_polygon_wikidata_website_coverage/pipeline/extract.py tests/pipeline/test_extract.py
git commit -m "refactor: simplify extraction orchestration"
```

### Task 4: Verify quality, compatibility, and publication

**Files:**
- Modify only if a verification failure identifies a behavior-preserving defect: the exact file under test.

- [ ] **Step 1: Run formatting, lint, and type checks available locally**

Run:

```bash
/opt/homebrew/bin/ruff format --check .
/opt/homebrew/bin/ruff check .
/opt/homebrew/bin/ruff check --select C901 --config 'lint.mccabe.max-complexity=5' src
```

Run `ty`, Radon/CRAP, MkDocs, and mutmut only if their packages are already available locally; do not download dependencies.

- [ ] **Step 2: Verify behavior and stored output compatibility**

Run the full pytest/coverage command, inspect the completed Seagate run manifest and summary, and verify the four existing aggregate counts remain `neither=855127768`, `website_only=1192451`, `wikidata_only=302666`, and `both=529` with `856623414` total rows. Confirm no source tree is modified and no run scratch or temporary files remain.

- [ ] **Step 3: Review the diff and repository state**

Run:

```bash
git diff --check
git diff --stat origin/main...HEAD
git status --short --branch
git ls-remote --heads origin
```

Stage only intended tracked files, leave unrelated `.DS_Store` files untouched, and confirm the only remote branch is `main`.

- [ ] **Step 4: Commit and push the verified result**

```bash
git add docs/superpowers/plans/2026-08-30-end-to-end-quality-refactor.md
git commit -m "refactor: improve overlap pipeline maintainability"
git push origin main
```

- [ ] **Step 5: Verify the published commit**

Run `git status --short --branch`, `git rev-parse HEAD`, and `git ls-remote origin refs/heads/main` and report exact results, including any unavailable local quality tools.
