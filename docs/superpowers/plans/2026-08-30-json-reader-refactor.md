# JSON Reader Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove duplicated JSON-object loading from the join and overlap stages without changing stage behavior, manifests, resume semantics, or existing private wrapper names.

**Architecture:** Extend the existing `io.atomic` persistence module with one read-only `read_json_object` helper that returns a dictionary only for readable JSON objects and returns `None` for missing, malformed, or non-object JSON. Keep `_read_manifest` and `_read_json` as thin compatibility-preserving wrappers in their current modules, so stage code keeps its existing seams while sharing the parsing mechanics.

**Tech Stack:** Python 3.12, standard-library `json`, pytest, pytest-cov, Ruff, ty, CRAP, mutmut, MkDocs Material, uv.

---

### Task 1: Establish the shared JSON-reader contract

**Files:**
- Modify: `tests/io/test_atomic.py`

- [x] **Step 1: Write the failing tests**

Add the import and tests below to `tests/io/test_atomic.py`:

```python
from osm_polygon_wikidata_website_coverage.io.atomic import (
    atomic_path,
    read_json_object,
    write_json,
)


def test_read_json_object_returns_a_dictionary_for_valid_json(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text('{"status": "complete", "count": 2}', encoding="utf-8")

    assert read_json_object(path) == {"status": "complete", "count": 2}


@pytest.mark.parametrize("content", ["not json", "[]", "null"])
def test_read_json_object_returns_none_for_unusable_json(
    tmp_path: Path, content: str
) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(content, encoding="utf-8")

    assert read_json_object(path) is None


def test_read_json_object_returns_none_for_a_missing_file(tmp_path: Path) -> None:
    assert read_json_object(tmp_path / "missing.json") is None
```

- [x] **Step 2: Run the focused tests and verify the expected red failure**

Run:

```bash
env PYTHONPATH="$(/usr/bin/find /Users/noeflandre/.cache/uv/archive-v0 -mindepth 1 -maxdepth 1 -type d -print | /usr/bin/paste -sd: -):src:." /Applications/anaconda3/bin/python -m pytest -q tests/io/test_atomic.py -k read_json_object
```

Expected: collection fails with `ImportError` because `read_json_object` does not yet exist.

Observed: collection failed with the expected `ImportError` for the missing helper.

### Task 2: Implement the minimal shared reader

**Files:**
- Modify: `src/osm_polygon_wikidata_website_coverage/io/atomic.py`
- Test: `tests/io/test_atomic.py`

- [x] **Step 1: Add the smallest implementation that satisfies the contract**

Add `Any` to the typing imports and add this function after `atomic_path`:

```python
def read_json_object(path: Path) -> dict[str, Any] | None:
    """Read a JSON object, returning ``None`` for unusable files."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None
```

- [x] **Step 2: Run the focused tests and verify green**

Run:

```bash
env PYTHONPATH="$(/usr/bin/find /Users/noeflandre/.cache/uv/archive-v0 -mindepth 1 -maxdepth 1 -type d -print | /usr/bin/paste -sd: -):src:." /Applications/anaconda3/bin/python -m pytest -q tests/io/test_atomic.py -k read_json_object
```

Expected: all 5 initial JSON-reader tests pass.

Observed: all 5 initial JSON-reader tests passed. Mutation analysis then identified two equivalent encoding mutants, so the focused suite was strengthened with an explicit UTF-8 forwarding test; the final focused run passed 6 tests with 4 unrelated tests deselected.

### Task 3: Replace duplicated stage parsing while preserving seams

**Files:**
- Modify: `src/osm_polygon_wikidata_website_coverage/pipeline/join.py`
- Modify: `src/osm_polygon_wikidata_website_coverage/pipeline/overlap.py`
- Test: `tests/pipeline/test_join.py`
- Test: `tests/pipeline/test_overlap.py`

- [x] **Step 1: Delegate the existing wrappers to the shared reader**

In `pipeline/join.py`, replace the local JSON import and parser body with:

```python
from osm_polygon_wikidata_website_coverage.io.atomic import read_json_object, write_json


def _read_manifest(path: Path) -> dict[str, Any] | None:
    return read_json_object(path)
```

In `pipeline/overlap.py`, replace the local JSON import and parser body with:

```python
from osm_polygon_wikidata_website_coverage.io.atomic import (
    atomic_path,
    read_json_object,
    write_json,
)


def _read_json(path: Path) -> dict[str, Any] | None:
    return read_json_object(path)
```

Keep both wrapper names and all callers unchanged.

- [x] **Step 2: Run the stage suites and quality checks**

Run:

```bash
env PYTHONPATH="$(/usr/bin/find /Users/noeflandre/.cache/uv/archive-v0 -mindepth 1 -maxdepth 1 -type d -print | /usr/bin/paste -sd: -):src:." /Applications/anaconda3/bin/python -m pytest -q tests/io/test_atomic.py tests/pipeline/test_join.py tests/pipeline/test_overlap.py
/opt/homebrew/bin/ruff format --check .
/opt/homebrew/bin/ruff check .
```

Expected: all selected tests pass and Ruff reports no changes or violations.

Observed: 23 selected tests passed; Ruff formatting and lint checks passed.

### Task 4: Verify and publish

**Files:**
- Modify: `docs/superpowers/plans/2026-08-30-json-reader-refactor.md`

- [x] **Step 1: Run the complete local quality gates without downloading**

Run the full 100% branch-coverage suite, cached ty, strict MkDocs, CRAP, offline wheel build, and the configured mutation gate. The mutation result must contain only `killed` entries.

Observed: 128 tests passed with 100% coverage (954 statements and 188 branches); Ruff formatting and linting passed; cached `ty` passed; strict MkDocs passed; CRAP passed for 134 functions, all below 6; the offline wheel build succeeded; and the mutation inventory reported 347 killed and 0 non-killed mutants.

- [x] **Step 2: Remove generated verification artifacts**

Remove only the generated `site/`, `mutants/`, `.coverage`, and `.pytest_cache/` paths after recording results. Leave unrelated `.DS_Store` files untouched.

Observed: those generated paths, plus the temporary offline wheel directory under `/tmp`, were removed. The unrelated `.DS_Store` files were left untouched.

- [ ] **Step 3: Commit and push the verified change**

```bash
git add src/osm_polygon_wikidata_website_coverage/io/atomic.py src/osm_polygon_wikidata_website_coverage/pipeline/join.py src/osm_polygon_wikidata_website_coverage/pipeline/overlap.py tests/io/test_atomic.py docs/superpowers/plans/2026-08-30-json-reader-refactor.md
git commit -m "refactor: centralize JSON object reads"
git push origin main
```

- [ ] **Step 4: Verify the published commit**

Confirm the checkout has no project changes beyond pre-existing `.DS_Store` files, `origin/main` points to the new commit, the public repository remains on `main` only, and exact-SHA CI and Documentation workflows succeed.
