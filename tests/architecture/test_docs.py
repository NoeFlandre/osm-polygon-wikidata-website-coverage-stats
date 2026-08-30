from pathlib import Path

PROJECT_ROOT = next(
    candidate
    for candidate in (Path.cwd(), *Path(__file__).parents)
    if (candidate / "pyproject.toml").is_file()
)


def test_mkdocs_site_has_public_methodology_and_operations_pages() -> None:
    assert (PROJECT_ROOT / "mkdocs.yml").is_file()
    for page in ("index.md", "methodology.md", "operations.md", "architecture.md"):
        assert (PROJECT_ROOT / "docs" / page).is_file()


def test_public_docs_state_the_read_only_data_boundary() -> None:
    text = "\n".join(
        (PROJECT_ROOT / "docs" / page).read_text(encoding="utf-8")
        for page in ("index.md", "methodology.md", "operations.md", "architecture.md")
    )

    assert "read-only" in text
    assert "successful" in text
    assert "ODbL" in text
    assert "full_text" in text
    assert "raw PBF" in text
    assert "64" in text
