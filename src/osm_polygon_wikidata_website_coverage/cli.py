"""Thin command-line composition for preflight, runs, and publication staging."""

# Typer declares options in function signatures by design.
# ruff: noqa: B008

from __future__ import annotations

from pathlib import Path

import typer

from osm_polygon_wikidata_website_coverage.config.paths import (
    DEFAULT_DATA_ROOT,
    DEFAULT_RAW_PBF_ROOT,
    DEFAULT_WEBSITE_ROOT,
    DEFAULT_WIKIDATA_ROOT,
    DataPaths,
)
from osm_polygon_wikidata_website_coverage.pipeline.run import run_analysis

app = typer.Typer(
    name="coverage",
    help="Analyze raw OSM polygon coverage against successful text datasets.",
    no_args_is_help=True,
)


def _paths(
    data_root: Path,
    raw_pbf_root: Path,
    wikidata_root: Path,
    website_root: Path,
) -> DataPaths:
    return DataPaths.from_values(
        data_root=data_root,
        raw_pbf_root=raw_pbf_root,
        wikidata_root=wikidata_root,
        website_root=website_root,
    )


@app.command()
def preflight(
    data_root: Path = typer.Option(DEFAULT_DATA_ROOT, help="Seagate output root."),
    raw_pbf_root: Path = typer.Option(DEFAULT_RAW_PBF_ROOT, help="Read-only raw PBF root."),
    wikidata_root: Path = typer.Option(DEFAULT_WIKIDATA_ROOT, help="Read-only Wikidata root."),
    website_root: Path = typer.Option(DEFAULT_WEBSITE_ROOT, help="Read-only website root."),
) -> None:
    """Validate roots and list the sorted raw PBF inventory."""

    paths = _paths(data_root, raw_pbf_root, wikidata_root, website_root)
    files = tuple(sorted(paths.raw_pbf_root.glob("*.osm.pbf"), key=lambda path: path.name))
    if not files:
        raise typer.BadParameter(f"no regular *.osm.pbf files found under {paths.raw_pbf_root}")
    typer.echo(f"data root: {paths.data_root}")
    typer.echo(f"raw PBF files: {len(files)}")
    for path in files:
        typer.echo(f"{path.name}\t{path.stat().st_size} bytes")


@app.command("run")
def run_command(
    run_id: str = typer.Option("20260827-coverage-v1", help="Unique run identifier."),
    data_root: Path = typer.Option(DEFAULT_DATA_ROOT, help="Seagate output root."),
    raw_pbf_root: Path = typer.Option(DEFAULT_RAW_PBF_ROOT, help="Read-only raw PBF root."),
    wikidata_root: Path = typer.Option(DEFAULT_WIKIDATA_ROOT, help="Read-only Wikidata root."),
    website_root: Path = typer.Option(DEFAULT_WEBSITE_ROOT, help="Read-only website root."),
) -> None:
    """Extract, join, aggregate, render, and verify one complete run."""

    paths = _paths(data_root, raw_pbf_root, wikidata_root, website_root)
    result = run_analysis(paths, run_id)
    typer.echo(f"completed run: {result.run_root}")
    typer.echo(f"valid polygon universe: {result.aggregation.global_row_count}")


@app.command("stage-hf")
def stage_hf_command(
    run_root: Path = typer.Argument(..., help="Completed run directory."),
    destination: Path = typer.Option(..., help="Empty staging directory."),
) -> None:
    """Stage only compact coverage artifacts for the public HF dataset."""

    from osm_polygon_wikidata_website_coverage.publishing.hf import stage_hf

    staged = stage_hf(run_root, destination)
    typer.echo(f"staged Hugging Face dataset files: {staged}")


if __name__ == "__main__":
    app()
