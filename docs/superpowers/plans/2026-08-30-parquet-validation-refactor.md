# Parquet Validation Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove duplicated Parquet metadata/schema validation from the overlap pipeline without changing any public API, output, error handling, or resume behavior.

**Architecture:** Keep `_output_is_valid` and `_summary_is_valid` as compatibility-preserving semantic wrappers. Move their identical file-reading and schema-checking mechanics into one private `_parquet_matches_schema` function that accepts the expected Arrow schema.

**Tech Stack:** Python 3.12, PyArrow, pytest, pytest-cov, Ruff, ty, CRAP, mutmut, MkDocs Material, uv.

---

### Task 1: Establish the shared validation contract

**Files:**
- Modify: `tests/pipeline/test_overlap.py`

- [x] **Step 1: Write the failing test**

Add this test beside the existing overlap validator tests:

```python
def test_parquet_schema_validator_accepts_each_expected_schema(tmp_path: Path) -> None:
    overlap = tmp_path / "overlap.parquet"
    summary = tmp_path / "summary.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=OVERLAP_SCHEMA), overlap)
    pq.write_table(pa.Table.from_pylist([], schema=SUMMARY_SCHEMA), summary)

    assert overlap_module._parquet_matches_schema(overlap, OVERLAP_SCHEMA) is True
    assert overlap_module._parquet_matches_schema(summary, SUMMARY_SCHEMA) is True
```

- [x] **Step 2: Run the focused test to verify it fails**

Run:

```bash
/Applications/anaconda3/bin/python -m pytest -q tests/pipeline/test_overlap.py::test_parquet_schema_validator_accepts_each_expected_schema
```

Expected: fail during collection or attribute access because `_parquet_matches_schema` does not yet exist.

Observed: the focused test failed with `AttributeError` because the helper did not yet exist.

### Task 2: Implement and refactor the validator

**Files:**
- Modify: `src/osm_polygon_wikidata_website_coverage/pipeline/overlap.py:132-145`
- Modify: `tests/pipeline/test_overlap.py`

- [x] **Step 1: Write the minimal implementation**

Replace the duplicated bodies with one shared helper and preserve the wrapper names:

```python
def _parquet_matches_schema(path: Path, schema: pa.Schema) -> bool:
    try:
        metadata = pq.ParquetFile(path).metadata
        return metadata is not None and pq.read_schema(path) == schema
    except (OSError, ValueError, pa.ArrowException):
        return False


def _output_is_valid(path: Path) -> bool:
    return _parquet_matches_schema(path, OVERLAP_SCHEMA)


def _summary_is_valid(path: Path) -> bool:
    return _parquet_matches_schema(path, SUMMARY_SCHEMA)
```

- [x] **Step 2: Run focused tests to verify green**

Run:

```bash
/Applications/anaconda3/bin/python -m pytest -q tests/pipeline/test_overlap.py
```

Expected: all overlap tests pass.

Observed: all 8 overlap tests passed.

- [x] **Step 3: Refactor only while green**

Keep the helper private, retain both existing wrapper names, remove no other overlap behavior, and format the touched files with Ruff.

- [x] **Step 4: Run the focused suite again**

Run:

```bash
/Applications/anaconda3/bin/python -m pytest -q tests/pipeline/test_overlap.py
/opt/homebrew/bin/ruff format --check .
/opt/homebrew/bin/ruff check .
```

Expected: all tests and lint/format checks pass.

Observed: all focused tests, Ruff formatting, and Ruff lint checks passed.

### Task 3: Verify and publish

**Files:**
- Modify: `docs/superpowers/plans/2026-08-30-parquet-validation-refactor.md`

- [x] **Step 1: Run every local quality gate without downloading**

Run the full 100% branch-coverage suite, cached ty, strict MkDocs, CRAP, and the configured mutation gate. The mutation result must contain only `killed` entries.

Observed: 122 tests passed with 100% statement and branch coverage; cached ty, Ruff, strict MkDocs, and CRAP passed; the configured mutation inventory killed 342/342 mutants with no survivors, timeouts, or untested mutants.

- [x] **Step 2: Clean generated verification artifacts**

Remove only the generated `site/`, `mutants/`, `.coverage`, and `.pytest_cache/` paths after recording their results; leave unrelated `.DS_Store` files untouched.

- [x] **Step 3: Commit and push**

```bash
git add src/osm_polygon_wikidata_website_coverage/pipeline/overlap.py tests/pipeline/test_overlap.py docs/superpowers/plans/2026-08-30-parquet-validation-refactor.md
git commit -m "refactor: share overlap parquet validation"
git push origin main
```

- [x] **Step 4: Verify the published commit**

Confirm `git status --short --branch`, `git ls-remote --heads origin`, and successful CI and Documentation workflows for the pushed commit.
