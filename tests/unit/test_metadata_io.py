"""Atomic small-object writes and GET-only metadata reads, without real AWS."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from unittest import mock

import boto3
import pytest
from moto import mock_aws
from pyiceberg.io import load_file_io

from ducklake_iceberg.metadata_io import MetadataFileIO

PREFIX = "s3://metadata-bucket/mirror"


@pytest.fixture
def storage() -> Iterator[tuple[MetadataFileIO, list[str]]]:
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        client.create_bucket(Bucket="metadata-bucket")
        operations: list[str] = []

        def record(model: Any, **kwargs: Any) -> None:
            operations.append(str(model.name))

        client.meta.events.register("before-call.s3.*", record)
        yield MetadataFileIO(load_file_io(), client, PREFIX), operations


def test_small_write_is_single_conditional_put_and_read_has_no_head(
    storage: tuple[MetadataFileIO, list[str]],
) -> None:
    io, operations = storage
    path = PREFIX + "/v1.metadata.json"
    with io.new_output(path).create() as stream:
        stream.write(b'{"version":1}')
    assert operations == ["PutObject"]
    with io.new_input(path).open(seekable=False) as stream:
        assert stream.read() == b'{"version":1}'
    assert operations == ["PutObject", "GetObject"]
    with pytest.raises(FileExistsError), io.new_output(path).create() as stream:
        stream.write(b"replacement")
    with io.new_input(path).open() as stream:
        assert stream.read() == b'{"version":1}'


def test_handoff_may_be_replaced_and_failed_writer_does_not_commit(
    storage: tuple[MetadataFileIO, list[str]],
) -> None:
    io, operations = storage
    path = PREFIX + "/_ducklake_iceberg_last_export.json"
    for content in (b"one", b"two"):
        with io.new_output(path).create(overwrite=True) as stream:
            stream.write(content)

    def failing_writer() -> None:
        with io.new_output(path).create(overwrite=True) as stream:
            stream.write(b"incomplete")
            raise ValueError("writer failed")

    with pytest.raises(ValueError, match="writer failed"):
        failing_writer()
    assert operations == ["PutObject", "PutObject"]
    with io.new_input(path).open() as reader:
        assert reader.read() == b"two"


def test_missing_and_input_size(storage: tuple[MetadataFileIO, list[str]]) -> None:
    io, _ = storage
    path = PREFIX + "/missing.json"
    assert io.new_input(path).exists() is False
    with pytest.raises(FileNotFoundError):
        io.new_input(path).open()
    with io.new_output(path).create() as stream:
        stream.write(b"abc")
    output = io.new_output(path)
    assert output.exists() is True
    assert len(output) == 3
    with output.to_input_file().open() as stream:
        assert stream.read() == b"abc"


def test_parquet_input_delegates_and_source_write_is_forbidden(
    storage: tuple[MetadataFileIO, list[str]],
) -> None:
    io, _ = storage
    path = "s3://source-bucket/source/data.parquet"
    with mock.patch.object(io.delegate, "new_input") as delegate:
        assert io.new_input(path) is delegate.return_value
        delegate.assert_called_once_with(path)
    with pytest.raises(PermissionError):
        io.new_output(path)
    with pytest.raises(PermissionError):
        io.delete(path)


def test_large_output_uses_bounded_multipart_and_preserves_immutability(
    storage: tuple[MetadataFileIO, list[str]],
) -> None:
    io, operations = storage
    path = PREFIX + "/large.avro"
    contents = b"x" * (8 * 1024 * 1024 + 10)
    with io.new_output(path).create() as stream:
        stream.write(contents)
    assert operations == [
        "CreateMultipartUpload",
        "UploadPart",
        "UploadPart",
        "CompleteMultipartUpload",
    ]
    with io.new_input(path).open() as stream:
        assert stream.read() == contents
    with pytest.raises(FileExistsError), io.new_output(path).create() as stream:
        stream.write(contents)
    assert operations[-1] == "AbortMultipartUpload"


def test_sdk_file_calls_are_not_counted_twice() -> None:
    from ducklake_iceberg import timing
    from ducklake_iceberg.obs.metrics import (
        RunMetrics,
        add_file_call,
        collect,
        note_api_call,
    )

    metrics = RunMetrics(run_id="test", command="sync")
    with collect(metrics), timing.phase("s3_put"):
        note_api_call()
        add_file_call(type("SDKIO", (), {"tracks_sdk_calls": True})(), "s3_put")
    assert metrics.totals()["api_calls"] == 1
