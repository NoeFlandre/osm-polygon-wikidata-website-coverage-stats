import tomllib
from pathlib import Path

PROJECT_ROOT = next(
    candidate
    for candidate in (Path.cwd(), *Path(__file__).parents)
    if (candidate / "pyproject.toml").is_file()
)


def test_required_project_files_exist() -> None:
    required = (
        "pyproject.toml",
        "justfile",
        "LICENSE",
        "CITATION.cff",
        ".pre-commit-config.yaml",
        "Dockerfile",
        "scripts/check_crap.py",
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


def test_pytest_resolves_source_and_project_test_packages() -> None:
    config = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert config["tool"]["pytest"]["ini_options"]["pythonpath"] == ["src", "."]


def test_mutmut_targets_the_complete_production_source_root() -> None:
    config = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    mutation_config = config["tool"]["mutmut"]

    assert mutation_config["source_paths"] == ["src/osm_polygon_wikidata_website_coverage"]
    assert "only_mutate" not in mutation_config
    assert mutation_config["runner"] == "python -m pytest -q"
