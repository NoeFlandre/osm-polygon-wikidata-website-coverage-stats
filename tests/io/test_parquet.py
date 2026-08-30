import inspect
from pathlib import Path

import pyarrow.parquet as pq
import pytest

import osm_polygon_wikidata_website_coverage.io.parquet as parquet_module
from osm_polygon_wikidata_website_coverage.domain.identity import OsmIdentity
from osm_polygon_wikidata_website_coverage.io.parquet import (
    IDENTITY_SCHEMA,
    OVERLAP_SCHEMA,
    IdentityParquetWriter,
)


def test_identity_writer_uses_one_atomic_file_and_bounded_row_groups(tmp_path: Path) -> None:
    with IdentityParquetWriter(tmp_path, filename="raw.parquet", batch_rows=2) as writer:
        for identity_id in range(5):
            writer.write(OsmIdentity("way", identity_id + 1))

    output = tmp_path / "raw.parquet"
    parquet = pq.ParquetFile(output)
    assert parquet.metadata.num_rows == 5
    assert parquet.metadata.num_row_groups == 3
    assert pq.read_schema(output) == IDENTITY_SCHEMA
    assert not list(tmp_path.glob("*.tmp"))


def test_identity_writer_emits_schema_for_empty_input_and_rejects_bad_batches(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="^batch_rows must be positive$"):
        IdentityParquetWriter(tmp_path, filename="bad.parquet", batch_rows=0)

    writer = IdentityParquetWriter(tmp_path, filename="default.parquet")
    assert writer._batch_rows == 100_000
    writer.abort()
    with IdentityParquetWriter(tmp_path, filename="empty.parquet"):
        pass
    assert pq.read_table(tmp_path / "empty.parquet").num_rows == 0


@pytest.mark.parametrize("filename", ["", "nested/raw.parquet", "raw.txt"])
def test_identity_writer_rejects_non_parquet_filenames(tmp_path: Path, filename: str) -> None:
    with pytest.raises(ValueError, match="^filename must be a Parquet filename$"):
        IdentityParquetWriter(tmp_path, filename=filename)


def test_identity_writer_default_batch_size_is_stable(tmp_path: Path) -> None:
    assert inspect.signature(IdentityParquetWriter).parameters["batch_rows"].default == 100_000
    writer = IdentityParquetWriter(tmp_path, filename="raw.parquet")
    assert writer._batch_rows == 100_000
    writer.abort()


def test_identity_writer_creates_nested_output_directory(tmp_path: Path) -> None:
    directory = tmp_path / "nested" / "output"

    writer = IdentityParquetWriter(directory, filename="raw.parquet")
    writer.abort()

    assert directory.is_dir()


def test_identity_writer_aborts_on_context_error_without_promoting_partial_file(
    tmp_path: Path,
) -> None:
    with (
        pytest.raises(RuntimeError, match="stop"),
        IdentityParquetWriter(tmp_path, filename="failed.parquet", batch_rows=1) as writer,
    ):
        writer.write(OsmIdentity("relation", 3))
        raise RuntimeError("stop")

    assert not (tmp_path / "failed.parquet").exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_identity_writer_is_idempotent_after_close_and_refuses_overwrites(tmp_path: Path) -> None:
    writer = IdentityParquetWriter(tmp_path, filename="raw.parquet")
    writer.close()
    assert writer._writer is None
    writer.close()
    with pytest.raises(RuntimeError, match="^cannot write to a closed Parquet writer$"):
        writer.write(OsmIdentity("way", 1))

    with (
        pytest.raises(FileExistsError, match="overwrite"),
        IdentityParquetWriter(tmp_path, filename="raw.parquet"),
    ):
        pass

    (tmp_path / ".other.parquet.tmp").write_bytes(b"existing")
    with (
        pytest.raises(FileExistsError, match="overwrite"),
        IdentityParquetWriter(tmp_path, filename="other.parquet"),
    ):
        pass


def test_identity_writer_abort_is_idempotent_before_and_after_opening(tmp_path: Path) -> None:
    writer = IdentityParquetWriter(tmp_path, filename="aborted.parquet")
    assert writer._closed is False
    writer.abort()
    assert writer._closed is True
    writer.abort()
    assert not (tmp_path / "aborted.parquet").exists()
    with pytest.raises(RuntimeError, match="^cannot write to a closed Parquet writer$"):
        writer.write(OsmIdentity("way", 1))


def test_identity_writer_opens_compressed_writer_with_identity_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    class Writer:
        def __init__(self, path: Path, schema: object, **kwargs: object) -> None:
            captured.update(path=path, schema=schema, **kwargs)

        def close(self) -> None:
            pass

    monkeypatch.setattr(parquet_module.pq, "ParquetWriter", Writer)
    writer = IdentityParquetWriter(tmp_path, filename="raw.parquet")
    writer._open_writer()

    assert captured == {
        "path": tmp_path / ".raw.parquet.tmp",
        "schema": IDENTITY_SCHEMA,
        "compression": "zstd",
    }
    writer.abort()


def test_identity_writer_flushes_with_identity_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    class TableFactory:
        @staticmethod
        def from_pydict(data: dict[str, list[object]], *, schema: object) -> object:
            captured["data"] = {key: list(values) for key, values in data.items()}
            captured["schema"] = schema
            return object()

    class Writer:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        def write_table(self, table: object) -> None:
            captured["table"] = table

        def close(self) -> None:
            pass

    monkeypatch.setattr(parquet_module.pa, "Table", TableFactory)
    monkeypatch.setattr(parquet_module.pq, "ParquetWriter", Writer)
    writer = IdentityParquetWriter(tmp_path, filename="raw.parquet", batch_rows=1)
    writer.write(OsmIdentity("way", 1))

    assert captured == {
        "data": {"osm_type": ["way"], "osm_id": [1]},
        "schema": IDENTITY_SCHEMA,
        "table": captured["table"],
    }
    writer.abort()


def test_identity_writer_cleans_up_after_writer_close_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class BrokenWriter:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self.close_calls = 0

        def write_table(self, table: object) -> None:
            del table

        def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("close failed")

    monkeypatch.setattr(parquet_module.pq, "ParquetWriter", BrokenWriter)
    writer = IdentityParquetWriter(tmp_path, filename="broken.parquet", batch_rows=1)
    writer.write(OsmIdentity("way", 1))
    with pytest.raises(RuntimeError, match="close failed"):
        writer.close()
    writer.abort()


def test_overlap_schema_contains_only_two_flags_and_category() -> None:
    assert OVERLAP_SCHEMA.names == [
        "osm_type",
        "osm_id",
        "website",
        "wikidata",
        "overlap_category",
    ]
