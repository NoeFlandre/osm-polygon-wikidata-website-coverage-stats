import tomllib
from pathlib import Path

PROJECT_ROOT = next(
    candidate
    for candidate in (Path.cwd(), *Path(__file__).parents)
    if (candidate / "pyproject.toml").is_file()
)


def test_ci_workflows_run_locked_quality_and_docs_checks() -> None:
    ci = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    docs = (PROJECT_ROOT / ".github" / "workflows" / "docs.yml").read_text(encoding="utf-8")

    for token in ("uv sync --frozen", "ruff", "ty", "pytest", "mkdocs build --strict"):
        assert token in ci
    assert "mkdocs build --strict" in docs
    assert "mutmut results --all true" in ci
    assert "survived" in ci
    assert "not checked" in ci
    assert "uv build --wheel" in ci
    assert "docker build" in ci


def test_mutation_testing_runs_behavior_tests_only() -> None:
    config = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert config["tool"]["mutmut"]["pytest_add_cli_args_test_selection"] == [
        "tests",
        "--ignore=tests/architecture",
    ]


def test_mutation_result_gate_is_safe_with_pipefail() -> None:
    ci = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert (
        "grep -Eq ': (survived|timeout|no tests|not checked|suspicious|unviable)$' <<< \"$results\""
        in ci
    )


def test_ci_does_not_reference_seagate_inputs() -> None:
    text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (PROJECT_ROOT / ".github" / "workflows").glob("*.yml")
    )

    assert "/Volumes/Seagate" not in text
    assert "processed_v2" not in text
