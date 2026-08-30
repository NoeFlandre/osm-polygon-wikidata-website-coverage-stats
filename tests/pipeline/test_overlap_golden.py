import json
from pathlib import Path
from typing import Any, NoReturn, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.pipeline.overlap as overlap_module
from osm_polygon_wikidata_website_coverage.io.parquet import IDENTITY_SCHEMA, MEMBERSHIP_SCHEMA
from osm_polygon_wikidata_website_coverage.pipeline.join import MembershipResult
from osm_polygon_wikidata_website_coverage.pipeline.overlap import (
    OVERLAP_SHARD_COUNT,
    compute_overlap,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLDEN_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "overlap-golden.json"


def _load_fixture() -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(GOLDEN_FIXTURE.read_text(encoding="utf-8")))


def _write_parquet(path: Path, schema: pa.Schema, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, compression="zstd")
    return path


def _sorted_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (str(row["osm_type"]), int(row["osm_id"])))


def _read_rows(paths: tuple[Path, ...]) -> list[dict[str, Any]]:
    rows = [cast(dict[str, Any], row) for path in paths for row in pq.read_table(path).to_pylist()]
    return _sorted_rows(rows)


def _artifact_bytes(output_root: Path) -> dict[str, bytes]:
    coverage = output_root / "coverage"
    artifacts = list((coverage / "overlap").glob("*.parquet"))
    artifacts.extend((coverage / "overlap-summary.parquet", coverage / "manifest.json"))
    return {
        str(path.relative_to(output_root)): path.read_bytes()
        for path in sorted(artifacts, key=lambda item: str(item))
    }


def test_golden_overlap_contract_and_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _load_fixture()
    raw = _write_parquet(
        tmp_path / "raw" / "universe.parquet",
        IDENTITY_SCHEMA,
        cast(list[dict[str, Any]], fixture["raw"]),
    )
    website = _write_parquet(
        tmp_path / "members" / "website.parquet",
        MEMBERSHIP_SCHEMA,
        cast(list[dict[str, Any]], fixture["website"]),
    )
    wikidata = _write_parquet(
        tmp_path / "members" / "wikidata.parquet",
        MEMBERSHIP_SCHEMA,
        cast(list[dict[str, Any]], fixture["wikidata"]),
    )

    first = compute_overlap(raw.parent, MembershipResult((website, wikidata)), tmp_path / "run")

    assert len(first.paths) == OVERLAP_SHARD_COUNT
    assert _read_rows(first.paths) == _sorted_rows(cast(list[dict[str, Any]], fixture["rows"]))
    assert pq.read_table(first.summary_path).to_pylist() == fixture["summary"]
    assert first.summary == fixture["counts"]
    assert first.row_count == 4
    before = _artifact_bytes(tmp_path / "run")

    def fail_if_recomputed(*args: object, **kwargs: object) -> NoReturn:
        del args, kwargs
        raise AssertionError("a valid golden stage was recomputed")

    monkeypatch.setattr(overlap_module, "_run_overlap_query", fail_if_recomputed)
    second = compute_overlap(
        raw.parent,
        MembershipResult((website, wikidata)),
        tmp_path / "run",
        resume=True,
    )

    assert second == first
    assert _artifact_bytes(tmp_path / "run") == before
