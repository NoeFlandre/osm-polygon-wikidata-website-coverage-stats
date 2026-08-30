import runpy
import sys
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from typer.testing import CliRunner

import osm_polygon_wikidata_website_coverage.cli as cli_module
from osm_polygon_wikidata_website_coverage.cli import app
from osm_polygon_wikidata_website_coverage.config.paths import DEFAULT_DATA_ROOT, DataPaths


def test_cli_preflight_does_not_write_to_input_roots(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    pbf = raw / "fixture-latest.osm.pbf"
    pbf.write_bytes(b"fixture")
    wikidata = tmp_path / "wikidata"
    wikidata.mkdir()
    website = tmp_path / "website"
    website.mkdir()
    before = pbf.read_bytes()

    result = CliRunner().invoke(
        app,
        [
            "preflight",
            "--raw-pbf-root",
            str(raw),
            "--wikidata-root",
            str(wikidata),
            "--website-root",
            str(website),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert "fixture-latest.osm.pbf" in result.stdout
    assert pbf.read_bytes() == before
    assert not list(raw.glob("*.coverage*"))


def test_cli_help_lists_only_the_two_public_commands() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "preflight" in result.stdout
    assert "run" in result.stdout
    assert "stage-hf" not in result.stdout


def test_cli_preflight_rejects_an_empty_raw_inventory(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    wikidata = tmp_path / "wikidata"
    wikidata.mkdir()
    website = tmp_path / "website"
    website.mkdir()

    result = CliRunner().invoke(
        app,
        [
            "preflight",
            "--raw-pbf-root",
            str(raw),
            "--wikidata-root",
            str(wikidata),
            "--website-root",
            str(website),
        ],
    )

    assert result.exit_code != 0
    assert "no regular" in result.output or "no regular" in str(result.exception)


def test_cli_run_composes_paths_and_reports_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    fake = SimpleNamespace(
        run_root=tmp_path / "run",
        overlap=SimpleNamespace(row_count=7, summary_path=tmp_path / "summary.parquet"),
    )
    calls: list[tuple[object, str, dict[str, object]]] = []

    def fake_run(paths: object, run_id: str, **kwargs: object) -> object:
        calls.append((paths, run_id, kwargs))
        return fake

    monkeypatch.setattr(cli_module, "run_analysis", fake_run)
    cli_module.run_command(
        run_id="fixture",
        data_root=DEFAULT_DATA_ROOT,
        raw_pbf_root=source,
        wikidata_root=source,
        website_root=source,
        workers=3,
        batch_rows=17,
        resume=True,
    )

    output = capsys.readouterr().out
    assert calls[0][1] == "fixture"
    assert calls[0][2] == {"batch_rows": 17, "resume": True, "workers": 3}
    composed_paths = cast(DataPaths, calls[0][0])
    assert composed_paths.data_root == DEFAULT_DATA_ROOT
    assert composed_paths.raw_pbf_root == source.resolve()
    assert composed_paths.wikidata_root == source.resolve()
    assert composed_paths.website_root == source.resolve()
    assert "completed run" in output
    assert "7" in output


def test_cli_paths_forwards_each_explicit_root(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    wikidata = tmp_path / "wikidata"
    website = tmp_path / "website"
    raw.mkdir()
    wikidata.mkdir()
    website.mkdir()
    paths = cli_module._paths(
        DEFAULT_DATA_ROOT / "custom-output",
        raw,
        wikidata,
        website,
    )

    assert paths.data_root == DEFAULT_DATA_ROOT / "custom-output"
    assert paths.raw_pbf_root == raw.resolve()
    assert paths.wikidata_root == wikidata.resolve()
    assert paths.website_root == website.resolve()


def test_cli_module_entrypoint_can_render_help(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["coverage", "--help"])
    monkeypatch.delitem(sys.modules, "osm_polygon_wikidata_website_coverage.cli", raising=False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(SystemExit) as raised:
            runpy.run_module(
                "osm_polygon_wikidata_website_coverage.cli",
                run_name="__main__",
            )

    assert raised.value.code == 0
    assert not any(warning.category is RuntimeWarning for warning in caught)
