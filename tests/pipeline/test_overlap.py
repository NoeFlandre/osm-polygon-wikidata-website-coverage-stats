import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.pipeline.overlap as overlap_module
from osm_polygon_wikidata_website_coverage.io.parquet import (
    IDENTITY_SCHEMA,
    MEMBERSHIP_SCHEMA,
    OVERLAP_SCHEMA,
    SUMMARY_SCHEMA,
)
from osm_polygon_wikidata_website_coverage.pipeline.join import MembershipResult
from osm_polygon_wikidata_website_coverage.pipeline.overlap import (
    MEMORY_LIMIT,
    OVERLAP_SHARD_COUNT,
    OverlapError,
    compute_overlap,
)


def _identity_file(root: Path, name: str, rows: list[dict[str, object]]) -> Path:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=IDENTITY_SCHEMA), path)
    return path


def _membership_file(root: Path, name: str, rows: list[dict[str, object]]) -> Path:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=MEMBERSHIP_SCHEMA), path)
    return path


def test_compute_overlap_deduplicates_raw_ids_and_writes_four_categories(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw-identities"
    _identity_file(
        raw,
        "a.parquet",
        [
            {"osm_type": "way", "osm_id": 1},
            {"osm_type": "way", "osm_id": 2},
            {"osm_type": "relation", "osm_id": 3},
        ],
    )
    _identity_file(raw, "b.parquet", [{"osm_type": "way", "osm_id": 1}])
    members = tmp_path / "members"
    website = _membership_file(members, "website.parquet", [{"osm_type": "way", "osm_id": 1}])
    wikidata = _membership_file(
        members,
        "wikidata.parquet",
        [{"osm_type": "way", "osm_id": 2}, {"osm_type": "relation", "osm_id": 3}],
    )

    result = compute_overlap(raw, MembershipResult((website, wikidata)), tmp_path / "run")

    assert len(result.paths) == OVERLAP_SHARD_COUNT == 64
    rows = [row for path in result.paths for row in pq.read_table(path).to_pylist()]
    assert sorted(rows, key=lambda row: (row["osm_type"], row["osm_id"])) == [
        {
            "osm_type": "relation",
            "osm_id": 3,
            "website": False,
            "wikidata": True,
            "overlap_category": "wikidata_only",
        },
        {
            "osm_type": "way",
            "osm_id": 1,
            "website": True,
            "wikidata": False,
            "overlap_category": "website_only",
        },
        {
            "osm_type": "way",
            "osm_id": 2,
            "website": False,
            "wikidata": True,
            "overlap_category": "wikidata_only",
        },
    ]
    summary = pq.read_table(result.summary_path).to_pylist()
    assert summary == [
        {"overlap_category": "neither", "count": 0, "percentage": 0.0},
        {
            "overlap_category": "website_only",
            "count": 1,
            "percentage": pytest.approx(33.33333333333333),
        },
        {
            "overlap_category": "wikidata_only",
            "count": 2,
            "percentage": pytest.approx(66.66666666666666),
        },
        {"overlap_category": "both", "count": 0, "percentage": 0.0},
    ]
    assert result.summary == {"neither": 0, "website_only": 1, "wikidata_only": 2, "both": 0}
    assert not (tmp_path / "run" / "scratch").exists()


def test_write_shard_deduplicates_raw_rows_before_membership_join(tmp_path: Path) -> None:
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"osm_type": "way", "osm_id": 1},
                {"osm_type": "way", "osm_id": 1},
            ],
            schema=IDENTITY_SCHEMA,
        ),
        bucket / "part-0.parquet",
    )

    connection = overlap_module.duckdb.connect(database=":memory:")
    try:
        connection.execute("CREATE TEMP TABLE website_keys (osm_type VARCHAR, osm_id BIGINT)")
        connection.execute("CREATE TEMP TABLE wikidata_keys (osm_type VARCHAR, osm_id BIGINT)")
        connection.execute("INSERT INTO website_keys VALUES ('way', 1)")
        output = tmp_path / "shard.parquet"
        overlap_module._write_shard(connection, bucket, output)
    finally:
        connection.close()

    assert pq.read_table(output).to_pylist() == [
        {
            "osm_type": "way",
            "osm_id": 1,
            "website": True,
            "wikidata": False,
            "overlap_category": "website_only",
        }
    ]


def test_compute_overlap_uses_bounded_duckdb_configuration_and_reuses_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw-identities"
    _identity_file(raw, "raw.parquet", [{"osm_type": "way", "osm_id": 1}])
    members = tmp_path / "members"
    website = _membership_file(members, "website.parquet", [])
    wikidata = _membership_file(members, "wikidata.parquet", [])
    output = tmp_path / "run"

    first = compute_overlap(raw, MembershipResult((website, wikidata)), output)
    manifest = json.loads((output / "coverage" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["memory_limit"] == MEMORY_LIMIT == "3GB"

    monkeypatch.setattr(
        overlap_module, "_run_overlap_query", lambda *args, **kwargs: pytest.fail("recomputed")
    )
    second = compute_overlap(raw, MembershipResult((website, wikidata)), output, resume=True)
    assert second.paths == first.paths
    assert second.summary == first.summary


def test_compute_overlap_rebuilds_invalid_resume_outputs_and_validates_inputs(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw-identities"
    _identity_file(raw, "raw.parquet", [{"osm_type": "way", "osm_id": 1}])
    members = tmp_path / "members"
    website = _membership_file(members, "website.parquet", [])
    wikidata = _membership_file(members, "wikidata.parquet", [])
    output = tmp_path / "run"
    compute_overlap(raw, MembershipResult((website, wikidata)), output)
    (output / "coverage" / "manifest.json").write_text("{}", encoding="utf-8")
    (output / "coverage" / "overlap" / "shard-00.parquet").unlink()

    result = compute_overlap(raw, MembershipResult((website, wikidata)), output, resume=True)

    assert result.row_count == 1
    with pytest.raises(ValueError, match="two membership"):
        compute_overlap(raw, MembershipResult((website,)), tmp_path / "bad")  # type: ignore[arg-type]
    with pytest.raises(OverlapError, match="raw identity"):
        compute_overlap(
            tmp_path / "missing", MembershipResult((website, wikidata)), tmp_path / "bad2"
        )


def test_overlap_validators_reject_missing_corrupt_and_wrong_schema_inputs(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    with pytest.raises(OverlapError, match="contains no Parquet"):
        overlap_module._identity_files(raw)

    with pytest.raises(OverlapError, match="missing"):
        overlap_module._validate_membership_path(tmp_path / "missing.parquet")

    corrupt = tmp_path / "corrupt.parquet"
    corrupt.write_bytes(b"not parquet")
    with pytest.raises(OverlapError, match="cannot be read"):
        overlap_module._validate_membership_path(corrupt)
    wrong = tmp_path / "wrong.parquet"
    pq.write_table(pa.table({"wrong": [1]}), wrong)
    with pytest.raises(OverlapError, match="schema mismatch"):
        overlap_module._validate_membership_path(wrong)

    assert overlap_module._output_is_valid(tmp_path / "missing-overlap.parquet") is False
    assert overlap_module._summary_is_valid(tmp_path / "missing-summary.parquet") is False
    valid_overlap = tmp_path / "valid-overlap.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=OVERLAP_SCHEMA), valid_overlap)
    assert overlap_module._output_is_valid(valid_overlap) is True
    valid_summary = tmp_path / "valid-summary.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=SUMMARY_SCHEMA), valid_summary)
    assert overlap_module._summary_is_valid(valid_summary) is True


def test_overlap_stage_rejects_incomplete_shard_inventory_and_cleans_failed_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    raw_file = _identity_file(raw, "raw.parquet", [{"osm_type": "way", "osm_id": 1}])
    members = tmp_path / "members"
    website = _membership_file(members, "website.parquet", [])
    wikidata = _membership_file(members, "wikidata.parquet", [])
    output = tmp_path / "run"
    compute_overlap(raw, MembershipResult((website, wikidata)), output)
    (output / "coverage" / "overlap" / "shard-00.parquet").unlink()
    raw_inventory = overlap_module._inventory((raw_file,), raw)
    membership_inventory = overlap_module._inventory((website, wikidata), members)
    assert overlap_module._stage_is_reusable(output, raw_inventory, membership_inventory) is False

    def fail(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("partition failed")

    monkeypatch.setattr(overlap_module, "_write_raw_partitions", fail)
    with pytest.raises(RuntimeError, match="partition failed"):
        overlap_module._write_partitioned_overlap(object(), raw, tmp_path / "failed")
    assert not (tmp_path / "failed" / "scratch" / "overlap-parts.tmp").exists()
    assert not (tmp_path / "failed" / "coverage" / ".overlap.tmp").exists()


def test_overlap_rejects_unknown_summary_category_and_fresh_overwrite(
    tmp_path: Path,
) -> None:
    with pytest.raises(OverlapError, match="unexpected"):
        overlap_module._summary_counts([("unexpected", 1)])

    raw = tmp_path / "raw"
    _identity_file(raw, "raw.parquet", [{"osm_type": "way", "osm_id": 1}])
    members = tmp_path / "members"
    website = _membership_file(members, "website.parquet", [])
    wikidata = _membership_file(members, "wikidata.parquet", [])
    output = tmp_path / "run"
    compute_overlap(raw, MembershipResult((website, wikidata)), output)
    with pytest.raises(FileExistsError, match="overwrite"):
        compute_overlap(raw, MembershipResult((website, wikidata)), output)
