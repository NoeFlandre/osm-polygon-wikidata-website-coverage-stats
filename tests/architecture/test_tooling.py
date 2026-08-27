from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]


def test_required_project_files_exist() -> None:
    required = (
        "pyproject.toml",
        "justfile",
        "LICENSE",
        "CITATION.cff",
        ".pre-commit-config.yaml",
        "Dockerfile",
    )

    assert all((PROJECT_ROOT / name).is_file() for name in required)


def test_project_uses_apache_license_and_quality_tools() -> None:
    metadata = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    license_text = (PROJECT_ROOT / "LICENSE").read_text(encoding="utf-8")

    assert "Apache-2.0" in metadata
    assert "ruff" in metadata
    assert "ty" in metadata
    assert "mutmut" in metadata
    assert "Apache License" in license_text
