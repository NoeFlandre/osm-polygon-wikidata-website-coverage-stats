"""Deterministic Markdown, JSON, and static chart rendering."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
from matplotlib import pyplot as plt

_CATEGORY_LABELS = {
    "neither": "neither",
    "website_only": "website only",
    "wikipedia_only": "Wikipedia only",
    "wikivoyage_only": "Wikivoyage only",
    "website_wikipedia_only": "website + Wikipedia only",
    "website_wikivoyage_only": "website + Wikivoyage only",
    "wikipedia_wikivoyage_only": "Wikipedia + Wikivoyage only",
    "all_three": "all three",
}


def _write_text(path: Path, text: str, *, replace_existing: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    if not replace_existing and (path.exists() or temporary.exists()):
        raise FileExistsError(f"refusing to overwrite report: {path}")
    if replace_existing:
        temporary.unlink(missing_ok=True)
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)
    return path


def _category_rows(summary: Mapping[str, Any]) -> list[tuple[str, int, float]]:
    total = int(summary.get("valid_universe_count", 0))
    rows: list[tuple[str, int, float]] = []
    for item in summary["overlap_categories"]:
        count = int(item["count"])
        percentage = float(item.get("percentage", count / total * 100 if total else 0.0))
        rows.append((str(item["category"]), count, percentage))
    return rows


def _area_lines(summary: Mapping[str, Any]) -> list[str]:
    return [
        f"- {name}: {float(value):,.2f} m²"
        for name, value in summary.get("area_statistics", {}).items()
        if value is not None
    ]


def _write_report(path: Path, text: str, *, resume: bool) -> Path:
    if resume:
        return _write_text(path, text, replace_existing=True)
    return _write_text(path, text)


def render_markdown(summary: Mapping[str, Any], output_path: Path, *, resume: bool = False) -> Path:
    """Write a compact human-readable coverage report."""

    total = int(summary["valid_universe_count"])
    covered = int(summary["covered_by_any_text_count"])
    covered_percentage = covered / total * 100 if total else 0
    failure_count = int(summary.get("geometry_failure_count", 0))
    lines = [
        "# OSM polygon text coverage report",
        "",
        f"- **Valid raw polygon universe:** {total:,}",
        f"- **Covered by any successful text:** {covered:,} ({covered_percentage:.2f}%)",
        f"- **Geometry failures (outside denominator):** {failure_count:,}",
        "",
        "## Coverage sources",
        "",
        f"- Website: {int(summary['website_count']):,}",
        f"- Wikipedia: {int(summary['wikipedia_count']):,}",
        f"- Wikivoyage: {int(summary['wikivoyage_count']):,}",
        "",
        "## Mutually exclusive overlap categories",
        "",
        "| Category | Polygons | Percentage |",
        "| --- | ---: | ---: |",
    ]
    lines.extend(
        f"| {_CATEGORY_LABELS.get(category, category)} | {count:,} | {percentage:.2f}% |"
        for category, count, percentage in _category_rows(summary)
    )
    lines.extend(["", "## Geometry summary", ""])
    lines.extend(_area_lines(summary))
    lines.extend(
        [
            "",
            "## Reproducibility boundary",
            "",
            "Source trees are read-only. This report contains no raw PBF, "
            "full geometry, or fetched text.",
            "",
        ]
    )
    return _write_report(output_path, "\n".join(lines), resume=resume)


def _render_coverage_chart(summary: Mapping[str, Any], path: Path) -> None:
    rows = _category_rows(summary)
    figure, axis = plt.subplots(figsize=(10, 5))
    axis.bar(
        [_CATEGORY_LABELS.get(row[0], row[0]) for row in rows],
        [row[1] for row in rows],
        color="#2364aa",
    )
    axis.set_ylabel("Polygon count")
    axis.set_title("Successful text coverage overlap")
    axis.tick_params(axis="x", labelrotation=35)
    figure.tight_layout()
    figure.savefig(path, dpi=120, metadata={"Software": "osm polygon coverage"})
    plt.close(figure)


def _render_area_chart(summary: Mapping[str, Any], path: Path) -> None:
    statistics = summary.get("area_statistics", {})
    names = ["min_m2", "p25_m2", "median_m2", "p75_m2", "p95_m2", "max_m2"]
    values = [float(statistics[name]) for name in names if statistics.get(name) is not None]
    labels = [name.removesuffix("_m2") for name in names if statistics.get(name) is not None]
    figure, axis = plt.subplots(figsize=(8, 5))
    axis.bar(labels, values, color="#3da35d")
    axis.set_ylabel("Area (m²)")
    axis.set_title("Polygon area statistics")
    axis.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
    figure.tight_layout()
    figure.savefig(path, dpi=120, metadata={"Software": "osm polygon coverage"})
    plt.close(figure)


def render_reports(
    summary: Mapping[str, Any], output_root: Path, *, resume: bool = False
) -> tuple[Path, ...]:
    """Write all public report artifacts and return their paths."""

    output_root.mkdir(parents=True, exist_ok=True)
    summary_text = (
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    if resume:
        summary_path = _write_text(
            output_root / "summary.json", summary_text, replace_existing=True
        )
        report_path = render_markdown(summary, output_root / "report.md", resume=True)
    else:
        summary_path = _write_text(output_root / "summary.json", summary_text)
        report_path = render_markdown(summary, output_root / "report.md")
    coverage_chart = output_root / "coverage_categories.png"
    area_chart = output_root / "area_distributions.png"
    _render_coverage_chart(summary, coverage_chart)
    _render_area_chart(summary, area_chart)
    return summary_path, report_path, coverage_chart, area_chart
