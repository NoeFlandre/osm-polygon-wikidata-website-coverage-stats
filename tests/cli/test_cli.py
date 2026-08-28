import runpy
import sys
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


def test_cli_help_lists_the_three_public_commands() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "preflight" in result.stdout
    assert "stage-hf" in result.stdout


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
        run_root=tmp_path / "run", aggregation=SimpleNamespace(global_row_count=7)
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
        with_geometry=False,
        resume=True,
    )

    output = capsys.readouterr().out
    assert calls[0][1] == "fixture"
    assert calls[0][2] == {
        "batch_rows": 50_000,
        "resume": True,
        "scanner": cli_module.scan_pbf_keys,
        "workers": 3,
    }
    composed_paths = cast(DataPaths, calls[0][0])
    assert composed_paths.data_root == DEFAULT_DATA_ROOT
    assert composed_paths.raw_pbf_root == source.resolve()
    assert composed_paths.wikidata_root == source.resolve()
    assert composed_paths.website_root == source.resolve()
    assert "completed run" in output
    assert "7" in output


def test_cli_run_can_request_full_geometry_extraction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    fake = SimpleNamespace(
        run_root=tmp_path / "run", aggregation=SimpleNamespace(global_row_count=7)
    )
    calls: list[dict[str, object]] = []

    def fake_run(paths: object, run_id: str, **kwargs: object) -> object:
        del paths, run_id
        calls.append(kwargs)
        return fake

    monkeypatch.setattr(cli_module, "run_analysis", fake_run)
    cli_module.run_command(
        run_id="fixture",
        data_root=DEFAULT_DATA_ROOT,
        raw_pbf_root=source,
        wikidata_root=source,
        website_root=source,
        workers=3,
        with_geometry=True,
        resume=False,
    )

    assert calls == [{"resume": False, "workers": 3}]
    assert "completed run" in capsys.readouterr().out


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


def test_cli_stage_hf_composes_publication_and_reports_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import osm_polygon_wikidata_website_coverage.publishing.hf as hf_module

    destination = tmp_path / "hf"
    calls: list[tuple[Path, Path]] = []

    def fake_stage(run_root: Path, target: Path) -> Path:
        calls.append((run_root, target))
        return target

    monkeypatch.setattr(hf_module, "stage_hf", fake_stage)
    cli_module.stage_hf_command(tmp_path / "run", destination)

    assert calls == [(tmp_path / "run", destination)]
    assert "staged Hugging Face" in capsys.readouterr().out


def test_cli_module_entrypoint_can_render_help(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["coverage", "--help"])

    with pytest.raises(SystemExit) as raised:
        runpy.run_module(
            "osm_polygon_wikidata_website_coverage.cli",
            run_name="__main__",
        )

    assert raised.value.code == 0
