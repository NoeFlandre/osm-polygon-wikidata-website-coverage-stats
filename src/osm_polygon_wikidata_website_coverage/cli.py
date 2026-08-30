"""Thin command-line composition for the two-set overlap calculation."""

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
from osm_polygon_wikidata_website_coverage.pipeline.extract import MAX_WORKERS, regular_pbf_files
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
    files = regular_pbf_files(paths.raw_pbf_root)
    if not files:
        raise typer.BadParameter(f"no regular *.osm.pbf files found under {paths.raw_pbf_root}")
    typer.echo(f"data root: {paths.data_root}")
    typer.echo(f"raw PBF files: {len(files)}")
    for path in files:
        typer.echo(f"{path.name}\t{path.stat().st_size} bytes")


@app.command("run")
def run_command(
    run_id: str = typer.Option(
        "20260829-website-wikidata-overlap-v1", help="Unique run identifier."
    ),
    workers: int = typer.Option(
        1,
        min=1,
        max=MAX_WORKERS,
        help=(
            "PBF extraction workers; one is fastest for the Seagate disk by default "
            f"(maximum {MAX_WORKERS})."
        ),
    ),
    batch_rows: int = typer.Option(
        100_000,
        min=1,
        help="Bounded raw-identity rows per Parquet row group.",
    ),
    data_root: Path = typer.Option(DEFAULT_DATA_ROOT, help="Seagate output root."),
    raw_pbf_root: Path = typer.Option(DEFAULT_RAW_PBF_ROOT, help="Read-only raw PBF root."),
    wikidata_root: Path = typer.Option(DEFAULT_WIKIDATA_ROOT, help="Read-only Wikidata root."),
    website_root: Path = typer.Option(DEFAULT_WEBSITE_ROOT, help="Read-only website root."),
    resume: bool = typer.Option(
        True,
        "--resume/--fresh",
        help="Reuse completed per-PBF checkpoints when rerunning an interrupted run.",
    ),
) -> None:
    """Compute raw-universe overlap between successful website and Wikidata text."""

    paths = _paths(data_root, raw_pbf_root, wikidata_root, website_root)
    result = run_analysis(
        paths,
        run_id,
        batch_rows=batch_rows,
        resume=resume,
        workers=workers,
    )
    typer.echo(f"completed run: {result.run_root}")
    typer.echo(f"raw polygon universe: {result.overlap.row_count}")
    typer.echo(f"overlap summary: {result.overlap.summary_path}")


if __name__ == "__main__":
    app()
