import json
from pathlib import Path
from typing import NoReturn, cast

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

    def fail_if_recomputed(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("recomputed")

    monkeypatch.setattr(overlap_module, "_run_overlap_query", fail_if_recomputed)
    second = compute_overlap(raw, MembershipResult((website, wikidata)), output, resume=True)
    assert second == first


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
    with pytest.raises(ValueError, match="^exactly two membership paths are required$"):
        compute_overlap(
            raw,
            MembershipResult(cast(tuple[Path, Path], (website,))),
            tmp_path / "bad",
        )
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


def test_parquet_schema_validator_accepts_each_expected_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    overlap = tmp_path / "overlap.parquet"
    summary = tmp_path / "summary.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=OVERLAP_SCHEMA), overlap)
    pq.write_table(pa.Table.from_pylist([], schema=SUMMARY_SCHEMA), summary)

    assert overlap_module._parquet_matches_schema(overlap, OVERLAP_SCHEMA) is True
    assert overlap_module._parquet_matches_schema(summary, SUMMARY_SCHEMA) is True
    monkeypatch.setattr(
        overlap_module.pq,
        "ParquetFile",
        lambda path: cast(object, type("MetadataOnly", (), {"metadata": None})()),
    )
    assert overlap_module._parquet_matches_schema(overlap, OVERLAP_SCHEMA) is False


def test_overlap_path_helpers_keep_the_persisted_layout(tmp_path: Path) -> None:
    assert overlap_module._overlap_root(tmp_path) == tmp_path / "coverage" / "overlap"
    assert overlap_module._summary_path(tmp_path) == (
        tmp_path / "coverage" / "overlap-summary.parquet"
    )
    assert overlap_module._manifest_path(tmp_path) == tmp_path / "coverage" / "manifest.json"
    assert overlap_module._scratch_root(tmp_path) == tmp_path / "scratch"


def test_stage_manifest_requires_a_manifest() -> None:
    assert overlap_module._stage_manifest_matches(None, [], []) is False


def test_identity_files_are_sorted_by_filename_not_path_comparison() -> None:
    class ReversedPath:
        def __init__(self, name: str) -> None:
            self.name = name

        def __lt__(self, other: "ReversedPath") -> bool:
            return self.name > other.name

    class Root:
        def is_dir(self) -> bool:
            return True

        def glob(self, pattern: str) -> tuple[ReversedPath, ...]:
            assert pattern == "*.parquet"
            return ReversedPath("b.parquet"), ReversedPath("a.parquet")

    files = overlap_module._identity_files(cast(Path, Root()))

    assert [path.name for path in files] == ["a.parquet", "b.parquet"]


def test_empty_overlap_output_uses_the_required_schema_and_compression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "empty.parquet"
    original_write_table = pq.write_table
    captured: dict[str, object] = {}

    def write_table(table: pa.Table, path: Path, **kwargs: object) -> None:
        captured["schema"] = table.schema
        captured["compression"] = kwargs.get("compression")
        original_write_table(table, path, **kwargs)

    monkeypatch.setattr(overlap_module.pq, "write_table", write_table)
    overlap_module._write_empty(output)

    assert captured == {"schema": OVERLAP_SCHEMA, "compression": "zstd"}
    assert pq.read_table(output).num_rows == 0


def test_raw_partition_query_uses_escaped_destination_and_exact_options(
    tmp_path: Path,
) -> None:
    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[str]]] = []

        def execute(self, query: str, parameters: list[str]) -> None:
            self.calls.append((query, parameters))

    connection = Connection()
    raw_root = tmp_path / "raw's"
    partitions = tmp_path / "parts's"

    overlap_module._write_raw_partitions(
        cast(overlap_module.duckdb.DuckDBPyConnection, connection), raw_root, partitions
    )

    destination = str(partitions).replace("'", "''")
    assert connection.calls == [
        (
            f"COPY ({overlap_module._PARTITION_QUERY}) TO '{destination}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (shard_id))",
            [str(raw_root / "*.parquet")],
        )
    ]


def test_partitioned_overlap_uses_recreatable_directories_and_cleanup_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rmtree_calls: list[tuple[Path, bool]] = []
    mkdir_calls: list[tuple[Path, bool, bool]] = []

    def fake_rmtree(path: str | Path, *, ignore_errors: bool = False) -> None:
        rmtree_calls.append((Path(path), ignore_errors))

    def fake_mkdir(
        path: Path,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        del mode
        mkdir_calls.append((path, parents, exist_ok))

    monkeypatch.setattr(overlap_module.shutil, "rmtree", fake_rmtree)
    monkeypatch.setattr(Path, "mkdir", fake_mkdir)
    monkeypatch.setattr(overlap_module, "_write_raw_partitions", lambda *args: None)
    monkeypatch.setattr(overlap_module, "_write_shard", lambda *args: None)
    expected = (tmp_path / "result.parquet",)
    monkeypatch.setattr(overlap_module, "_promote_overlap", lambda *args: expected)

    output_root = tmp_path / "run"
    result = overlap_module._write_partitioned_overlap(
        cast(overlap_module.duckdb.DuckDBPyConnection, object()),
        tmp_path / "raw",
        output_root,
    )

    partitions = output_root / "scratch" / "overlap-parts.tmp"
    temporary = output_root / "coverage" / ".overlap.tmp"
    assert result == expected
    assert mkdir_calls == [(partitions, True, True), (temporary, True, True)]
    assert rmtree_calls == [
        (partitions, True),
        (temporary, True),
        (partitions, True),
    ]


def test_partitioned_overlap_cleans_temporary_output_after_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rmtree_calls: list[tuple[Path, bool]] = []

    def fake_rmtree(path: str | Path, *, ignore_errors: bool = False) -> None:
        rmtree_calls.append((Path(path), ignore_errors))

    def fake_mkdir(
        path: Path,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        del path, mode, parents, exist_ok

    def fail(*args: object) -> NoReturn:
        del args
        raise RuntimeError("partition failed")

    monkeypatch.setattr(overlap_module.shutil, "rmtree", fake_rmtree)
    monkeypatch.setattr(Path, "mkdir", fake_mkdir)
    monkeypatch.setattr(overlap_module, "_write_raw_partitions", fail)

    output_root = tmp_path / "run"
    with pytest.raises(RuntimeError, match="partition failed"):
        overlap_module._write_partitioned_overlap(
            cast(overlap_module.duckdb.DuckDBPyConnection, object()),
            tmp_path / "raw",
            output_root,
        )

    partitions = output_root / "scratch" / "overlap-parts.tmp"
    temporary = output_root / "coverage" / ".overlap.tmp"
    assert rmtree_calls == [
        (partitions, True),
        (temporary, True),
        (temporary, True),
        (partitions, True),
    ]


def test_summary_records_zero_total_as_zero_percentages() -> None:
    counts = {"neither": 0, "website_only": 0, "wikidata_only": 0, "both": 0}

    assert overlap_module._summary_records(counts) == [
        {"overlap_category": "neither", "count": 0, "percentage": 0.0},
        {"overlap_category": "website_only", "count": 0, "percentage": 0.0},
        {"overlap_category": "wikidata_only", "count": 0, "percentage": 0.0},
        {"overlap_category": "both", "count": 0, "percentage": 0.0},
    ]


def test_summary_output_uses_the_required_schema_and_compression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows: list[dict[str, object]] = []
    captured: dict[str, object] = {}
    original_write_table = pq.write_table

    def write_table(table: pa.Table, path: Path, **kwargs: object) -> None:
        captured["schema"] = table.schema
        captured["compression"] = kwargs.get("compression")
        original_write_table(table, path, **kwargs)

    monkeypatch.setattr(overlap_module.pq, "write_table", write_table)
    output_root = tmp_path / "nested" / "run"

    result = overlap_module._write_summary(output_root, rows)

    assert result == output_root / "coverage" / "overlap-summary.parquet"
    assert captured == {"schema": SUMMARY_SCHEMA, "compression": "zstd"}
    assert pq.read_table(result).schema == SUMMARY_SCHEMA


def test_manifest_records_all_overlap_metadata(tmp_path: Path) -> None:
    raw_inventory = [{"path": "raw.parquet", "size_bytes": 11, "mtime_ns": 12}]
    membership_inventory = [
        {"path": "website.parquet", "size_bytes": 21, "mtime_ns": 22},
        {"path": "wikidata.parquet", "size_bytes": 31, "mtime_ns": 32},
    ]
    summary = {"neither": 1, "website_only": 2, "wikidata_only": 3, "both": 4}

    path = overlap_module._write_manifest(
        tmp_path / "run", raw_inventory, membership_inventory, 10, summary
    )

    assert path == tmp_path / "run" / "coverage" / "manifest.json"
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "schema_version": "1",
        "memory_limit": MEMORY_LIMIT,
        "duckdb_threads": overlap_module.DUCKDB_THREADS,
        "raw_inventory": raw_inventory,
        "membership_inventory": membership_inventory,
        "row_count": 10,
        "summary": summary,
    }


def test_membership_tables_use_stable_names_and_reject_extra_inputs(tmp_path: Path) -> None:
    website = tmp_path / "website.parquet"
    wikidata = tmp_path / "wikidata.parquet"
    extra = tmp_path / "extra.parquet"

    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[str]]] = []

        def execute(self, query: str, parameters: list[str]) -> None:
            self.calls.append((query, parameters))

    connection = Connection()
    typed_connection = cast(overlap_module.duckdb.DuckDBPyConnection, connection)
    overlap_module._load_membership_tables(typed_connection, (website, wikidata))

    assert connection.calls == [
        (
            "CREATE TEMP TABLE website_keys AS SELECT DISTINCT osm_type, osm_id "
            "FROM read_parquet(?)",
            [str(website)],
        ),
        (
            "CREATE TEMP TABLE wikidata_keys AS SELECT DISTINCT osm_type, osm_id "
            "FROM read_parquet(?)",
            [str(wikidata)],
        ),
    ]
    with pytest.raises(ValueError):
        overlap_module._load_membership_tables(
            typed_connection, cast(tuple[Path, Path], (website, wikidata, extra))
        )


def test_compute_overlap_forwards_inventories_and_runtime_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    raw_file = _identity_file(raw, "raw.parquet", [{"osm_type": "way", "osm_id": 1}])
    members = tmp_path / "members"
    website_root = members / "website"
    wikidata_root = members / "wikidata"
    website = _membership_file(website_root, "website.parquet", [])
    wikidata = _membership_file(wikidata_root, "wikidata.parquet", [])
    output_root = tmp_path / "nested" / "run"
    raw_inventory: list[dict[str, object]] = [{"source": "raw"}]
    membership_inventory: list[dict[str, object]] = [{"source": "members"}]
    summary = {"neither": 1, "website_only": 2, "wikidata_only": 3, "both": 4}
    summary_rows = [{"overlap_category": "neither", "count": 1, "percentage": 10.0}]
    shard = tmp_path / "shard.parquet"
    summary_path = tmp_path / "summary.parquet"
    seen_database: list[str] = []
    captured: dict[str, object] = {}

    class Connection:
        def close(self) -> None:
            captured["closed"] = True

    def connect(*, database: str) -> Connection:
        seen_database.append(database)
        return Connection()

    def inventory(paths: tuple[Path, ...], root: Path) -> list[dict[str, object]]:
        inventory_calls = captured.setdefault("inventory_calls", [])
        cast(list[object], inventory_calls).append((paths, root))
        return raw_inventory if root == raw else membership_inventory

    def write_manifest(
        root: Path,
        raw_values: list[dict[str, object]],
        membership_values: list[dict[str, object]],
        row_count: int,
        summary_values: dict[str, int],
    ) -> Path:
        captured["manifest"] = (root, raw_values, membership_values, row_count, summary_values)
        return tmp_path / "manifest.json"

    monkeypatch.setattr(overlap_module.duckdb, "connect", connect)
    monkeypatch.setattr(overlap_module, "configure_connection", lambda *args: None)
    monkeypatch.setattr(overlap_module, "_load_membership_tables", lambda *args: None)
    monkeypatch.setattr(overlap_module, "_inventory", inventory)
    monkeypatch.setattr(overlap_module, "_run_overlap_query", lambda *args: (shard,))
    monkeypatch.setattr(overlap_module, "_summary_rows", lambda *args: (summary_rows, summary))
    monkeypatch.setattr(overlap_module, "_write_summary", lambda *args: summary_path)
    monkeypatch.setattr(overlap_module, "_write_manifest", write_manifest)

    result = compute_overlap(raw, MembershipResult((website, wikidata)), output_root)

    assert seen_database == [":memory:"]
    assert output_root.is_dir()
    assert captured["inventory_calls"] == [
        ((raw_file,), raw),
        ((website, wikidata), website_root),
    ]
    assert captured["manifest"] == (
        output_root,
        raw_inventory,
        membership_inventory,
        10,
        summary,
    )
    assert result.paths == (shard,)
    assert result.summary_path == summary_path
    assert result.row_count == 10
    assert result.summary == summary


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
    raw_inventory = overlap_module._inventory((raw_file,), raw)
    membership_inventory = overlap_module._inventory((website, wikidata), members)
    (output / "coverage" / "overlap-summary.parquet").unlink()
    assert overlap_module._stage_is_reusable(output, raw_inventory, membership_inventory) is False
    compute_overlap(raw, MembershipResult((website, wikidata)), output, resume=True)
    (output / "coverage" / "overlap" / "shard-00.parquet").unlink()
    assert overlap_module._stage_is_reusable(output, raw_inventory, membership_inventory) is False

    def fail(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("partition failed")

    monkeypatch.setattr(overlap_module, "_write_raw_partitions", fail)
    with pytest.raises(RuntimeError, match="partition failed"):
        overlap_module._write_partitioned_overlap(
            cast(overlap_module.duckdb.DuckDBPyConnection, object()), raw, tmp_path / "failed"
        )
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
