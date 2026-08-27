from pathlib import Path

PROJECT_ROOT = next(
    candidate
    for candidate in (Path.cwd(), *Path(__file__).parents)
    if (candidate / "pyproject.toml").is_file()
)


def test_ci_workflows_run_locked_quality_and_docs_checks() -> None:
    ci = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    docs = (PROJECT_ROOT / ".github" / "workflows" / "docs.yml").read_text(encoding="utf-8")

    for token in ("uv sync --frozen", "ruff", "ty", "pytest"):
        assert token in ci
    assert "mkdocs build --strict" in docs


def test_ci_does_not_reference_seagate_inputs() -> None:
    text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (PROJECT_ROOT / ".github" / "workflows").glob("*.yml")
    )

    assert "/Volumes/Seagate" not in text
    assert "processed_v2" not in text
