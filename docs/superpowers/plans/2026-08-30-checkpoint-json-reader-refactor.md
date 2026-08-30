# Checkpoint JSON Reader Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make checkpoint loading use the shared JSON-object boundary while preserving resume behavior and making invalid UTF-8 checkpoints safely unusable.

**Architecture:** Keep JSON parsing in `io.atomic.read_json_object`, including its explicit UTF-8 and unusable-file contract. Keep `_read_checkpoint` as the extraction-stage compatibility seam, but delegate parsing to the shared reader so join, overlap, and extraction do not duplicate file decoding and JSON validation.

**Tech Stack:** Python 3.12, standard-library `json`, pytest, pytest-cov, Ruff, ty, CRAP, mutmut, MkDocs Material, uv.

---

### Task 1: Specify invalid-encoding behavior at the shared I/O boundary

**Files:**
- Modify: `tests/io/test_atomic.py`

- [x] **Step 1: Write the failing test**

Add this test beside the existing `read_json_object` tests:

```python
def test_read_json_object_returns_none_for_invalid_utf8(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_bytes(b'{"text": "\xff"}')

    assert read_json_object(path) is None
```

- [x] **Step 2: Run the focused test and verify the expected red failure**

Run:

```bash
env PYTHONPATH="$(/usr/bin/find /Users/noeflandre/.cache/uv/archive-v0 -mindepth 1 -maxdepth 1 -type d -print | /usr/bin/paste -sd: -):src:." /Applications/anaconda3/bin/python -m pytest -q tests/io/test_atomic.py::test_read_json_object_returns_none_for_invalid_utf8
```

Expected: the test fails with `UnicodeDecodeError` because the reader does not yet translate decoding failures.

Observed: the focused test failed with the expected `UnicodeDecodeError` before the implementation change.

### Task 2: Implement the smallest shared-reader fix

**Files:**
- Modify: `src/osm_polygon_wikidata_website_coverage/io/atomic.py`
- Test: `tests/io/test_atomic.py`

- [x] **Step 1: Catch decoding failures as unusable input**

Change the existing exception tuple in `read_json_object` to:

```python
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
```

- [x] **Step 2: Run the focused reader tests and verify green**

Run:

```bash
env PYTHONPATH="$(/usr/bin/find /Users/noeflandre/.cache/uv/archive-v0 -mindepth 1 -maxdepth 1 -type d -print | /usr/bin/paste -sd: -):src:." /Applications/anaconda3/bin/python -m pytest -q tests/io/test_atomic.py -k read_json_object
```

Expected: all 7 reader cases pass and the four unrelated atomic tests are deselected.

Observed: 7 reader cases passed and 4 unrelated atomic tests were deselected.

### Task 3: Remove duplicate checkpoint JSON parsing

**Files:**
- Modify: `src/osm_polygon_wikidata_website_coverage/pipeline/extract.py`
- Test: `tests/pipeline/test_extract.py`

- [x] **Step 1: Delegate `_read_checkpoint` to the shared reader**

Import `read_json_object` from `io.atomic` and replace the JSON parsing block in `_read_checkpoint` with:

```python
    try:
        payload = read_json_object(checkpoint)
        if payload is None:
            return None
        current = _checkpoint_source_snapshot(payload, pbf_path)
    except (TypeError, ValueError, ExtractionError):
        return None
```

Keep `_read_checkpoint` and all checkpoint payload fields unchanged.

- [x] **Step 2: Run extraction and full regression tests**

Run:

```bash
env PYTHONPATH="$(/usr/bin/find /Users/noeflandre/.cache/uv/archive-v0 -mindepth 1 -maxdepth 1 -type d -print | /usr/bin/paste -sd: -):src:." /Applications/anaconda3/bin/python -m pytest -q tests/pipeline/test_extract.py
env PYTHONPATH="$(/usr/bin/find /Users/noeflandre/.cache/uv/archive-v0 -mindepth 1 -maxdepth 1 -type d -print | /usr/bin/paste -sd: -):src:." /Applications/anaconda3/bin/python -m pytest -q --cov=src/osm_polygon_wikidata_website_coverage --cov-branch --cov-fail-under=100
```

Expected: extraction tests and the full suite pass with 100% statement and branch coverage.

Observed: 13 extraction tests passed; the full suite passed 130 tests with 100% coverage across 953 statements and 188 branches.

### Task 4: Verify and publish

**Files:**
- Modify: `docs/superpowers/plans/2026-08-30-checkpoint-json-reader-refactor.md`

- [x] **Step 1: Run all quality gates without downloading**

Run cached `ty`, Ruff format/lint, strict MkDocs, CRAP, offline wheel build, and the configured mutation gate. Require every mutation result to be `killed`.

Observed: Ruff format/lint, cached `ty`, strict MkDocs, CRAP for 134 functions below 6, and the offline wheel build all passed; mutation results were 347 killed and 0 non-killed.

- [x] **Step 2: Remove generated verification artifacts**

Remove only `site/`, `mutants/`, `.coverage`, `.pytest_cache/`, and the temporary wheel directory created under `/tmp`; leave unrelated `.DS_Store` files untouched.

Observed: all listed generated paths were removed; unrelated `.DS_Store` files were left untouched.

- [ ] **Step 3: Commit and push**

```bash
git add src/osm_polygon_wikidata_website_coverage/io/atomic.py src/osm_polygon_wikidata_website_coverage/pipeline/extract.py tests/io/test_atomic.py docs/superpowers/plans/2026-08-30-checkpoint-json-reader-refactor.md
git commit -m "refactor: share checkpoint JSON parsing"
git push origin main
```

- [ ] **Step 4: Verify the published commit**

Confirm exact local/remote `main` synchronization, only `main` exists remotely, and the exact-SHA CI and Documentation workflows pass.
