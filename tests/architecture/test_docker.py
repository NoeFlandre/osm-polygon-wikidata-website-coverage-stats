from pathlib import Path

PROJECT_ROOT = next(
    candidate
    for candidate in (Path.cwd(), *Path(__file__).parents)
    if (candidate / "pyproject.toml").is_file()
)


def test_dockerfile_runs_the_cli_without_copying_project_data() -> None:
    dockerfile = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "python:3.12-slim" in dockerfile
    assert 'CMD ["--help"]' in dockerfile
    assert "/Volumes/Seagate" not in dockerfile
    assert "raw" not in dockerfile
    assert "COPY data" not in dockerfile
