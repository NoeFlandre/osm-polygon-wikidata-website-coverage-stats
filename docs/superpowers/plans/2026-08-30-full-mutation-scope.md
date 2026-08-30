# Full Mutation Scope Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the mutation-testing gate cover every production Python module while preserving all existing behavior and keeping every mutant killed.

**Architecture:** Keep the existing modules and public APIs unchanged. Protect the mutation configuration with an architecture test, then let mutmut use the complete package source root instead of a hand-maintained subset. Any surviving mutant will be converted into a focused behavioral regression test before the configuration is finalized.

**Tech Stack:** Python 3.12, pytest, pytest-cov, mutmut, DuckDB, PyArrow, Ruff, ty, uv, MkDocs.

---

### Task 1: Establish the full-scope mutation contract

**Files:**
- Modify: `tests/architecture/test_tooling.py`
- Modify: `pyproject.toml:77-84`

- [x] **Step 1: Write the failing test**

Add this test to `tests/architecture/test_tooling.py`:

```python
def test_mutmut_targets_the_complete_production_source_root() -> None:
    config = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    mutation_config = config["tool"]["mutmut"]

    assert mutation_config["source_paths"] == ["src/osm_polygon_wikidata_website_coverage"]
    assert "only_mutate" not in mutation_config
```

- [x] **Step 2: Run the test to verify it fails**

Run:

```bash
PYTHONPATH=src:. python -m pytest -q tests/architecture/test_tooling.py::test_mutmut_targets_the_complete_production_source_root
```

Expected: `FAIL` because the current configuration still contains `only_mutate`.

- [x] **Step 3: Make the minimal configuration change**

Remove the `only_mutate` list from `[tool.mutmut]` in `pyproject.toml`. Keep `source_paths` unchanged so mutmut discovers every Python module below the package root.

- [x] **Step 4: Run the test to verify it passes**

Run the focused test again and then the architecture tests:

```bash
PYTHONPATH=src:. python -m pytest -q tests/architecture/test_tooling.py
```

Expected: all tooling architecture tests pass.

### Task 2: Use surviving mutants to strengthen behavior tests

**Files:**
- Modify: `tests/io/test_duckdb.py` if DuckDB mutants survive
- Modify: `tests/io/test_parquet.py` if Parquet-writer mutants survive
- Modify: `tests/pipeline/test_extract.py` if extraction mutants survive
- Modify: `tests/pipeline/test_join.py` if membership-stage mutants survive
- Modify: `tests/pipeline/test_overlap.py` if overlap mutants survive
- Modify: `tests/pipeline/test_run.py` if composition-root mutants survive
- Modify: `tests/config/test_paths.py` if path-safety mutants survive
- Modify: `tests/cli/test_cli.py` if CLI mutants survive

- [x] **Step 1: Run the complete mutation campaign**

Run mutmut with the project’s bounded runner:

```bash
PYTHONPATH=src:. python -m mutmut run --max-children 2
```

Expected: any `survived`, `timeout`, `no tests`, `suspicious`, or `unviable` result is a red quality gate and identifies the exact behavior needing a test.

- [x] **Step 2: Add one focused failing test per surviving behavior**

For each surviving mutant, add a test that asserts the externally observable contract at that branch or boundary. Run the new test against the mutated behavior to verify it fails, then restore the original source and verify the test passes.

- [x] **Step 3: Repeat mutation testing until no mutant survives**

Run the complete campaign again after each test group. Do not weaken assertions or exclude a module to make the result green.

### Task 3: Finalize the quality gate and documentation

**Files:**
- Modify: `docs/architecture.md` if the mutation-scope description needs clarification
- Modify: `docs/operations.md` if the mutation command or bounded execution guidance changes
- Modify: `tests/architecture/test_tooling.py` only if the final configuration contract needs a precise assertion

- [x] **Step 1: Run all local quality gates**

Run the complete test, coverage, formatting, lint, type, CRAP, documentation, package, and mutation commands already defined by the repository. Expected: all pass, branch coverage remains 100%, every relevant function remains below CRAP 6, and every mutant is killed.

- [x] **Step 2: Inspect the diff and generated artifacts**

Confirm only intentional source, test, configuration, and documentation files changed. Remove mutation databases, coverage files, MkDocs output, and temporary package artifacts; preserve the pre-existing `.DS_Store` files.

- [ ] **Step 3: Commit and push on `main`**

Use a Conventional Commit, verify the exact pushed commit, confirm the remote remains public with only `main`, and verify both final GitHub Actions workflows.
