import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

import osm_polygon_wikidata_website_coverage.reporting.render as render_module
from osm_polygon_wikidata_website_coverage.reporting.render import (
    _category_rows,
    _render_area_chart,
    _render_coverage_chart,
    _write_report,
    _write_text,
    render_markdown,
    render_reports,
)


def _summary() -> dict[str, Any]:
    return {
        "valid_universe_count": 8,
        "website_count": 4,
        "wikipedia_count": 4,
        "wikivoyage_count": 4,
        "covered_by_any_text_count": 7,
        "geometry_failure_count": 1,
        "source_keys_not_in_raw": {"website": 0, "wikipedia": 0, "wikivoyage": 1},
        "overlap_categories": [
            {"category": category, "count": 1, "percentage": 12.5}
            for category in (
                "neither",
                "website_only",
                "wikipedia_only",
                "wikivoyage_only",
                "website_wikipedia_only",
                "website_wikivoyage_only",
                "wikipedia_wikivoyage_only",
                "all_three",
            )
        ],
        "pairwise_intersections": {
            "website_wikipedia": 2,
            "website_wikivoyage": 2,
            "wikipedia_wikivoyage": 2,
            "all_three": 1,
        },
        "osm_type_counts": {"way": 6, "relation": 2},
        "geometry_type_counts": {"Polygon": 5, "MultiPolygon": 3},
        "area_statistics": {
            "total_m2": 800.0,
            "min_m2": 1.0,
            "max_m2": 8.0,
            "mean_m2": 4.5,
            "median_m2": 4.5,
            "p25_m2": 2.75,
            "p75_m2": 6.25,
            "p95_m2": 7.65,
        },
    }


def test_markdown_report_contains_denominator_and_all_overlap_categories(tmp_path: Path) -> None:
    report = render_markdown(_summary(), tmp_path / "report.md")
    text = report.read_text(encoding="utf-8")

    assert "Valid raw polygon universe" in text
    assert "website + Wikipedia only" in text
    assert "Wikivoyage" in text
    assert "Geometry failures" in text


def test_markdown_report_has_a_stable_public_format(tmp_path: Path) -> None:
    report = render_markdown(_summary(), tmp_path / "report.md")

    assert (
        report.read_text(encoding="utf-8")
        == """# OSM polygon text coverage report

- **Valid raw polygon universe:** 8
- **Covered by any successful text:** 7 (87.50%)
- **Geometry failures (outside denominator):** 1

## Coverage sources

- Website: 4
- Wikipedia: 4
- Wikivoyage: 4

## Mutually exclusive overlap categories

| Category | Polygons | Percentage |
| --- | ---: | ---: |
| neither | 1 | 12.50% |
| website only | 1 | 12.50% |
| Wikipedia only | 1 | 12.50% |
| Wikivoyage only | 1 | 12.50% |
| website + Wikipedia only | 1 | 12.50% |
| website + Wikivoyage only | 1 | 12.50% |
| Wikipedia + Wikivoyage only | 1 | 12.50% |
| all three | 1 | 12.50% |

## Geometry summary

- total_m2: 800.00 m²
- min_m2: 1.00 m²
- max_m2: 8.00 m²
- mean_m2: 4.50 m²
- median_m2: 4.50 m²
- p25_m2: 2.75 m²
- p75_m2: 6.25 m²
- p95_m2: 7.65 m²

## Reproducibility boundary

Source trees are read-only. This report contains no raw PBF, full geometry, or fetched text.
"""
    )


def test_category_rows_uses_explicit_percentages_and_zero_denominator_fallback() -> None:
    summary = _summary()
    rows = _category_rows(summary)
    assert rows[0] == ("neither", 1, 12.5)

    zero = {"valid_universe_count": 0, "overlap_categories": [{"category": "neither", "count": 0}]}
    assert _category_rows(zero) == [("neither", 0, 0.0)]

    missing_total = {"overlap_categories": [{"category": "neither", "count": 1}]}
    assert _category_rows(missing_total) == [("neither", 1, 0.0)]
    missing_percentage = {
        "valid_universe_count": 8,
        "overlap_categories": [{"category": "neither", "count": 2}],
    }
    assert _category_rows(missing_percentage) == [("neither", 2, 25.0)]
    explicit_percentage = {
        "valid_universe_count": 8,
        "overlap_categories": [{"category": "neither", "count": 1, "percentage": 37.5}],
    }
    assert _category_rows(explicit_percentage) == [("neither", 1, 37.5)]


def test_markdown_report_skips_missing_area_statistics_and_derives_missing_percentages(
    tmp_path: Path,
) -> None:
    summary = _summary()
    summary["area_statistics"]["p25_m2"] = None  # type: ignore[index]
    summary["overlap_categories"][0].pop("percentage")  # type: ignore[index]

    report = render_markdown(summary, tmp_path / "report.md")

    assert "p25_m2" not in report.read_text(encoding="utf-8")


def test_area_lines_only_formats_present_area_statistics() -> None:
    assert render_module._area_lines(
        {"area_statistics": {"min_m2": 1, "missing_m2": None, "max_m2": 2.5}}
    ) == ["- min_m2: 1.00 m²", "- max_m2: 2.50 m²"]
    assert render_module._area_lines({}) == []


def test_report_writer_forwards_resume_replacement_policy(tmp_path: Path) -> None:
    output = tmp_path / "report.md"
    output.write_text("old", encoding="utf-8")

    with pytest.raises(FileExistsError, match="overwrite report"):
        _write_report(output, "new", resume=False)
    assert _write_report(output, "new", resume=True) == output
    assert output.read_text(encoding="utf-8") == "new"


def test_markdown_renderer_is_fresh_by_default(tmp_path: Path) -> None:
    output = tmp_path / "report.md"
    render_markdown(_summary(), output)

    with pytest.raises(FileExistsError, match="overwrite report"):
        render_markdown(_summary(), output)


def test_markdown_report_uses_defaults_for_optional_summary_fields(tmp_path: Path) -> None:
    summary = _summary()
    summary.pop("geometry_failure_count")
    summary.pop("area_statistics")
    summary["valid_universe_count"] = 0
    summary["covered_by_any_text_count"] = 0

    report = render_markdown(summary, tmp_path / "nested" / "report.md")
    text = report.read_text(encoding="utf-8")

    assert "Covered by any successful text:** 0 (0.00%)" in text
    assert "Geometry failures (outside denominator):** 0" in text
    assert "## Geometry summary" in text

    summary["overlap_categories"].append({"category": "other", "count": 0, "percentage": 0.0})
    unknown_report = render_markdown(summary, tmp_path / "unknown.md")
    assert "| other | 0 | 0.00% |" in unknown_report.read_text(encoding="utf-8")


def test_report_writers_refuse_existing_final_and_temporary_files(tmp_path: Path) -> None:
    output = tmp_path / "report.md"
    output.write_text("existing", encoding="utf-8")
    with pytest.raises(FileExistsError, match="overwrite report"):
        _write_text(output, "replacement")

    temporary = tmp_path / ".other.md.tmp"
    temporary.write_text("existing", encoding="utf-8")
    with pytest.raises(FileExistsError, match="overwrite report"):
        _write_text(tmp_path / "other.md", "replacement")


def test_report_writer_creates_nested_utf8_output_with_exact_encoding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[object] = []
    original_write_text = Path.write_text

    def write_text(
        path: Path,
        text: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        captured.append(encoding)
        return original_write_text(path, text, encoding=encoding, errors=errors, newline=newline)

    monkeypatch.setattr(Path, "write_text", write_text)
    output = _write_text(tmp_path / "one" / "two" / "report.md", "café")

    assert output.read_text(encoding="utf-8") == "café"
    assert captured == ["utf-8"]


def test_chart_renderers_pass_stable_plot_contracts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class Axis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, object]] = []

        def bar(self, labels: object, values: object, **kwargs: object) -> None:
            self.calls.append(("bar", (labels, values, kwargs)))

        def set_ylabel(self, value: str) -> None:
            self.calls.append(("ylabel", value))

        def set_title(self, value: str) -> None:
            self.calls.append(("title", value))

        def tick_params(self, **kwargs: object) -> None:
            self.calls.append(("tick_params", kwargs))

        def ticklabel_format(self, **kwargs: object) -> None:
            self.calls.append(("ticklabel_format", kwargs))

    class Figure:
        def __init__(self) -> None:
            self.tight_layout_calls = 0
            self.savefig_calls: list[tuple[Path, int, dict[str, str]]] = []

        def tight_layout(self) -> None:
            self.tight_layout_calls += 1

        def savefig(self, path: Path, *, dpi: int, metadata: dict[str, str]) -> None:
            self.savefig_calls.append((path, dpi, metadata))

    axes: list[Axis] = []
    figures: list[Figure] = []

    def subplots(*, figsize: tuple[int, int]) -> tuple[Figure, Axis]:
        assert figsize in {(10, 5), (8, 5)}
        figure = Figure()
        axis = Axis()
        figures.append(figure)
        axes.append(axis)
        return figure, axis

    closed: list[Figure] = []
    monkeypatch.setattr(render_module.plt, "subplots", subplots)
    monkeypatch.setattr(render_module.plt, "close", closed.append)

    summary = _summary()
    coverage_path = tmp_path / "coverage.png"
    area_path = tmp_path / "area.png"
    _render_coverage_chart(summary, coverage_path)
    _render_area_chart(summary, area_path)

    assert axes[0].calls == [
        (
            "bar",
            (
                [
                    "neither",
                    "website only",
                    "Wikipedia only",
                    "Wikivoyage only",
                    "website + Wikipedia only",
                    "website + Wikivoyage only",
                    "Wikipedia + Wikivoyage only",
                    "all three",
                ],
                [1, 1, 1, 1, 1, 1, 1, 1],
                {"color": "#2364aa"},
            ),
        ),
        ("ylabel", "Polygon count"),
        ("title", "Successful text coverage overlap"),
        ("tick_params", {"axis": "x", "labelrotation": 35}),
    ]
    assert axes[1].calls == [
        (
            "bar",
            (
                ["min", "p25", "median", "p75", "p95", "max"],
                [1.0, 2.75, 4.5, 6.25, 7.65, 8.0],
                {"color": "#3da35d"},
            ),
        ),
        ("ylabel", "Area (m²)"),
        ("title", "Polygon area statistics"),
        (
            "ticklabel_format",
            {"axis": "y", "style": "sci", "scilimits": (0, 0)},
        ),
    ]
    assert figures[0].tight_layout_calls == 1
    assert figures[1].tight_layout_calls == 1
    assert figures[0].savefig_calls == [(coverage_path, 120, {"Software": "osm polygon coverage"})]
    assert figures[1].savefig_calls == [(area_path, 120, {"Software": "osm polygon coverage"})]
    assert closed == figures


def test_coverage_chart_uses_the_original_label_for_unknown_categories(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class Axis:
        def __init__(self) -> None:
            self.labels: object | None = None

        def bar(self, labels: object, values: object, **kwargs: object) -> None:
            self.labels = labels

        def set_ylabel(self, value: str) -> None:
            pass

        def set_title(self, value: str) -> None:
            pass

        def tick_params(self, **kwargs: object) -> None:
            pass

    class Figure:
        def tight_layout(self) -> None:
            pass

        def savefig(self, path: Path, *, dpi: int, metadata: dict[str, str]) -> None:
            pass

    axis = Axis()
    monkeypatch.setattr(render_module.plt, "subplots", lambda **kwargs: (Figure(), axis))
    monkeypatch.setattr(render_module.plt, "close", lambda figure: None)
    summary = {"valid_universe_count": 1, "overlap_categories": [{"category": "other", "count": 1}]}

    _render_coverage_chart(summary, tmp_path / "coverage.png")

    assert axis.labels == ["other"]


def test_area_chart_accepts_a_summary_without_optional_area_statistics(
    tmp_path: Path,
) -> None:
    _render_area_chart({}, tmp_path / "area.png")
    assert (tmp_path / "area.png").is_file()


def test_render_reports_writes_deterministic_public_artifacts(tmp_path: Path) -> None:
    summary = _summary()
    summary["unicode_note"] = "café"
    first = render_reports(summary, tmp_path / "first")
    render_reports(summary, tmp_path / "second")

    assert first == (
        tmp_path / "first" / "summary.json",
        tmp_path / "first" / "report.md",
        tmp_path / "first" / "coverage_categories.png",
        tmp_path / "first" / "area_distributions.png",
    )
    assert json.loads((tmp_path / "first" / "summary.json").read_text(encoding="utf-8")) == summary
    summary_text = (tmp_path / "first" / "summary.json").read_text(encoding="utf-8")
    assert summary_text.startswith('{\n  "area_statistics"')
    assert '"unicode_note": "café"' in summary_text

    assert {path.name for path in first} == {
        "summary.json",
        "report.md",
        "coverage_categories.png",
        "area_distributions.png",
    }
    for name in ("summary.json", "report.md", "coverage_categories.png", "area_distributions.png"):
        first_path = tmp_path / "first" / name
        second_path = tmp_path / "second" / name
        assert (
            hashlib.sha256(first_path.read_bytes()).digest()
            == hashlib.sha256(second_path.read_bytes()).digest()
        )

    resumed = render_reports(summary, tmp_path / "first", resume=True)
    assert resumed == first
    assert (tmp_path / "first" / "summary.json").is_file()
    assert (tmp_path / "first" / "report.md").is_file()


def test_render_reports_passes_the_exact_json_serialization_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dumps_calls: list[tuple[object, dict[str, object]]] = []
    write_calls: list[tuple[Path, str]] = []

    def fake_dumps(value: object, **kwargs: object) -> str:
        dumps_calls.append((value, kwargs))
        return "{}"

    def fake_write(path: Path, value: str) -> Path:
        write_calls.append((path, value))
        return path

    monkeypatch.setattr(render_module.json, "dumps", fake_dumps)
    monkeypatch.setattr(render_module, "_write_text", fake_write)
    monkeypatch.setattr(render_module, "render_markdown", lambda summary, path: path)
    monkeypatch.setattr(render_module, "_render_coverage_chart", lambda summary, path: None)
    monkeypatch.setattr(render_module, "_render_area_chart", lambda summary, path: None)
    summary = {"unicode_note": "café"}

    paths = render_reports(summary, tmp_path / "reports")

    assert dumps_calls == [
        (
            summary,
            {
                "ensure_ascii": False,
                "indent": 2,
                "sort_keys": True,
                "allow_nan": False,
            },
        )
    ]
    assert write_calls == [(tmp_path / "reports" / "summary.json", "{}\n")]
    assert paths == (
        tmp_path / "reports" / "summary.json",
        tmp_path / "reports" / "report.md",
        tmp_path / "reports" / "coverage_categories.png",
        tmp_path / "reports" / "area_distributions.png",
    )


def test_render_reports_creates_a_missing_nested_output_root(tmp_path: Path) -> None:
    output = tmp_path / "missing" / "nested" / "reports"

    paths = render_reports(_summary(), output)

    assert all(path.is_file() for path in paths)

    existing = tmp_path / "existing"
    existing.mkdir()
    existing_paths = render_reports(_summary(), existing)
    assert all(path.is_file() for path in existing_paths)


def test_render_reports_rejects_non_finite_json_values(tmp_path: Path) -> None:
    summary = _summary()
    summary["invalid"] = float("nan")

    with pytest.raises(ValueError, match="Out of range float values are not JSON compliant"):
        render_reports(summary, tmp_path / "reports")
