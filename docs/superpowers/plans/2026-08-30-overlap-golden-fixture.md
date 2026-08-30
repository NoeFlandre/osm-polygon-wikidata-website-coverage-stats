# Overlap Golden Fixture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add a deterministic overlap-stage contract that proves all four categories, raw-identity deduplication, and resumable output reuse.

**Architecture:** Keep the production overlap API unchanged. A single integration test will create temporary Parquet inputs with the production schemas, compare the resulting non-empty rows and summary to a checked-in JSON fixture, then rerun with `resume=True` while blocking recomputation and comparing every output byte.

**Tech Stack:** Python 3.12, pytest, PyArrow, DuckDB, JSON fixtures, Ruff, ty, pytest-cov.

---

### Task 1: Write the red golden-contract test

**Files:**
- Create: `tests/pipeline/test_overlap_golden.py`
- Read: `src/osm_polygon_wikidata_website_coverage/pipeline/overlap.py`
- Read: `src/osm_polygon_wikidata_website_coverage/io/parquet.py`

- [ ] **Step 1: Write the failing test**

Add helpers to load `tests/fixtures/overlap-golden.json`, write rows with the
production schemas, read and sort all shard rows by `(osm_type, osm_id)`, and
capture all overlap Parquet files plus the summary and manifest bytes. The test
must perform this contract:

```python
def test_golden_overlap_contract_and_resume(tmp_path, monkeypatch):
    fixture = _load_fixture()
    raw = _write_parquet(tmp_path / "raw" / "universe.parquet", IDENTITY_SCHEMA, fixture["raw"])
    website = _write_parquet(
        tmp_path / "members" / "website.parquet", MEMBERSHIP_SCHEMA, fixture["website"]
    )
    wikidata = _write_parquet(
        tmp_path / "members" / "wikidata.parquet", MEMBERSHIP_SCHEMA, fixture["wikidata"]
    )

    first = compute_overlap(raw.parent, MembershipResult((website, wikidata)), tmp_path / "run")
    assert _read_rows(first.paths) == _sorted_rows(fixture["rows"])
    assert pq.read_table(first.summary_path).to_pylist() == fixture["summary"]
    assert first.summary == fixture["counts"]
    assert first.row_count == 4
    before = _artifact_bytes(tmp_path / "run")

    def fail_if_recomputed(*args, **kwargs):
        raise AssertionError("a valid golden stage was recomputed")

    monkeypatch.setattr(overlap_module, "_run_overlap_query", fail_if_recomputed)
    second = compute_overlap(
        raw.parent,
        MembershipResult((website, wikidata)),
        tmp_path / "run",
        resume=True,
    )
    assert second == first
    assert _artifact_bytes(tmp_path / "run") == before
```

The fixture is intentionally absent at this step so the test fails at the
explicit fixture boundary rather than passing without exercising the contract.

- [ ] **Step 2: Run the test to verify it fails for the intended reason**

Run:

```bash
PYTHONPATH=src:. /Applications/anaconda3/bin/python -m pytest -q tests/pipeline/test_overlap_golden.py
```

Expected: one failure caused by the missing
`tests/fixtures/overlap-golden.json`; no production source changes.

### Task 2: Add the minimal golden data and turn the test green

**Files:**
- Create: `tests/fixtures/overlap-golden.json`

- [ ] **Step 1: Add the four-case fixture**

The JSON must contain four unique raw identities plus one duplicate occurrence:

```json
{
  "raw": [
    {"osm_type": "way", "osm_id": 1001},
    {"osm_type": "way", "osm_id": 1002},
    {"osm_type": "way", "osm_id": 1002},
    {"osm_type": "relation", "osm_id": 2001},
    {"osm_type": "relation", "osm_id": 2002}
  ],
  "website": [
    {"osm_type": "way", "osm_id": 1002},
    {"osm_type": "relation", "osm_id": 2002}
  ],
  "wikidata": [
    {"osm_type": "relation", "osm_id": 2001},
    {"osm_type": "relation", "osm_id": 2002}
  ],
  "rows": [
    {"osm_type": "relation", "osm_id": 2001, "website": false, "wikidata": true, "overlap_category": "wikidata_only"},
    {"osm_type": "relation", "osm_id": 2002, "website": true, "wikidata": true, "overlap_category": "both"},
    {"osm_type": "way", "osm_id": 1001, "website": false, "wikidata": false, "overlap_category": "neither"},
    {"osm_type": "way", "osm_id": 1002, "website": true, "wikidata": false, "overlap_category": "website_only"}
  ],
  "summary": [
    {"overlap_category": "neither", "count": 1, "percentage": 25.0},
    {"overlap_category": "website_only", "count": 1, "percentage": 25.0},
    {"overlap_category": "wikidata_only", "count": 1, "percentage": 25.0},
    {"overlap_category": "both", "count": 1, "percentage": 25.0}
  ],
  "counts": {"neither": 1, "website_only": 1, "wikidata_only": 1, "both": 1}
}
```

- [ ] **Step 2: Run the focused test to verify it passes**

Run the same focused pytest command. Expected: one passing test, with no
production source changes.

### Task 3: Refactor the test without changing its contract

- [ ] **Step 1: Run focused Ruff and ty checks**

Run:

```bash
ruff format --check tests/pipeline/test_overlap_golden.py
ruff check tests/pipeline/test_overlap_golden.py
ty check --python /Applications/anaconda3 src tests
```

- [ ] **Step 2: Keep helpers cohesive and remove only duplication exposed by the test**

Retain one fixture loader, one Parquet writer, one row reader, and one
artifact-byte reader. Do not add production abstractions or alter the overlap
API.

### Task 4: Run the complete verification and publish

- [ ] **Step 1: Run the full local quality gates**

Run the full test suite with branch coverage, Ruff, ty, CRAP, strict MkDocs,
offline wheel build, and complete mutation testing. Expected: all tests pass,
100% branch coverage, every CRAP score below 6, and zero non-killed mutants.

- [ ] **Step 2: Remove generated artifacts and inspect the exact diff**

Remove only generated coverage, MkDocs, mutation, and temporary wheel output.
Preserve `.DS_Store` files and all Seagate input/output trees.

- [ ] **Step 3: Commit and push on `main`**

Use a Conventional Commit, push `main`, verify the remote contains only
`refs/heads/main`, and verify the GitHub Actions CI and Documentation runs for
the pushed commit.
