"""Tests for the export/register/sync commands.

The pure logic (handoff pointer, register coherence,
architecture) is tested here with a local FileIO (file://) and moto for Glue.
The full export→register pipeline over a real DuckLake catalog + S3 lives in
the integration matrix (PyArrow's S3FileSystem does not respect moto).
"""

from __future__ import annotations

import ast
import os
import pathlib
import uuid
from collections.abc import Iterator

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from moto import mock_aws
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

from ducklake_iceberg.config import DuckLakeIcebergSettings, load_settings
from ducklake_iceberg.errors import DivergenceError
from ducklake_iceberg.register import (
    POINTER_NAME,
    ExportPointer,
    PointerError,
    pointer_path,
    read_pointer,
    resolve_metadata_location,
    run_register,
    write_pointer,
)
from ducklake_iceberg.writer.metadata import (
    DuckLakeProvenance,
    SnapshotCounters,
    build_table_metadata,
    write_metadata_json,
)

REGION = "us-east-1"


# -- handoff pointer -------------------------------------------------------


def _pointer(
    location: str, *, snapshot_id: int = 42, table: str = "orders"
) -> ExportPointer:
    return ExportPointer(
        version=1,
        metadata_location=f"{location}/metadata/v1.metadata.json",
        metadata_version=1,
        location=location,
        glue_database="analytics",
        glue_table=table,
        ducklake_catalog="lake",
        ducklake_schema="sales",
        ducklake_table=table,
        table_id=7,
        ducklake_snapshot_id=snapshot_id,
        schema_version=3,
        exported_at="2025-10-26T11:02:48Z",
        ducklake_iceberg_version="0.1.0",
    )


def test_pointer_name_is_not_version_hint() -> None:
    assert POINTER_NAME == "_ducklake_iceberg_last_export.json"
    assert "version-hint" not in POINTER_NAME


def test_pointer_path_outside_metadata() -> None:
    path = pointer_path("file:///x/t")
    assert path.endswith("/_ducklake_iceberg_last_export.json")
    assert "/metadata/" not in path


def test_pointer_roundtrip(tmp_path: pathlib.Path) -> None:
    location = "file://" + str(tmp_path)
    io = PyArrowFileIO()
    written = write_pointer(io, _pointer(location))
    assert written.endswith(POINTER_NAME)
    got = read_pointer(io, location)
    assert got.ducklake_snapshot_id == 42
    assert got.metadata_version == 1


def test_pointer_without_delete_layout_is_legacy(tmp_path: pathlib.Path) -> None:
    import json

    location = "file://" + str(tmp_path)
    io = PyArrowFileIO()
    write_pointer(io, _pointer(location))
    path = tmp_path / POINTER_NAME
    payload = json.loads(path.read_text())
    payload.pop("delete_file_layout")
    path.write_text(json.dumps(payload))

    assert read_pointer(io, location).delete_file_layout == "per-data-file-v1"


def test_pointer_overwrite(tmp_path: pathlib.Path) -> None:
    location = "file://" + str(tmp_path)
    io = PyArrowFileIO()
    write_pointer(io, _pointer(location, snapshot_id=1))
    write_pointer(io, _pointer(location, snapshot_id=2))  # sobrescreve
    assert read_pointer(io, location).ducklake_snapshot_id == 2


def test_read_pointer_absent(tmp_path: pathlib.Path) -> None:
    io = PyArrowFileIO()
    with pytest.raises(PointerError, match="no export found"):
        read_pointer(io, "file://" + str(tmp_path))


def test_read_pointer_unknown_version(tmp_path: pathlib.Path) -> None:
    import json

    path = tmp_path / POINTER_NAME
    path.write_text(json.dumps({"version": 99}))
    io = PyArrowFileIO()
    with pytest.raises(PointerError, match="version"):
        read_pointer(io, "file://" + str(tmp_path))


# -- register.py imports neither duckdb nor ducklake_iceberg.catalog -----------------


def test_register_module_architecture() -> None:
    import ducklake_iceberg.register as r

    tree = ast.parse(pathlib.Path(r.__file__).read_text())
    forbidden: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            forbidden += [
                n.name
                for n in node.names
                if n.name == "duckdb" or n.name.startswith("ducklake_iceberg.catalog")
            ]
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and (
                node.module == "duckdb"
                or node.module.startswith("ducklake_iceberg.catalog")
            )
        ):
            forbidden.append(node.module)
    assert forbidden == [], f"register.py importa {forbidden}"


# -- register: resolution and coherence with local FileIO + moto Glue -------


@pytest.fixture
def settings(tmp_path: pathlib.Path) -> Iterator[DuckLakeIcebergSettings]:
    data = tmp_path / "data"
    iceberg = tmp_path / "iceberg"
    data.mkdir()
    iceberg.mkdir()
    env = {
        "DUCKLAKE_ICEBERG_CATALOG_DSN": "ducklake:sqlite:/tmp/x.db",
        "DUCKLAKE_ICEBERG_DATA_PATH": str(data),
        "DUCKLAKE_ICEBERG_ICEBERG_METADATA_PREFIX": str(iceberg),
        "DUCKLAKE_ICEBERG_GLUE_DATABASE": "analytics",
        "DUCKLAKE_ICEBERG_AWS_REGION": REGION,
    }
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        yield load_settings()
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _write_table_metadata(
    settings_obj: DuckLakeIcebergSettings, table: str, *, snapshot_id: int
) -> str:
    """Write a local metadata.json and the pointer; return the location."""
    from ducklake_iceberg.glue import normalize_table_name

    prefix = settings_obj.iceberg_metadata_prefix.rstrip("/")
    location = f"{prefix}/{settings_obj.glue_database}/{normalize_table_name(table)}"
    pathlib.Path(location.removeprefix("file://") + "/metadata").mkdir(
        parents=True, exist_ok=True
    )
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "uf", StringType(), required=False),
        schema_id=0,
    )
    spec = PartitionSpec(spec_id=0)
    counters = SnapshotCounters(0, 0, 0, 0, 0, 0)
    prov = DuckLakeProvenance(
        catalog="lake",
        schema_name="sales",
        table_name=table,
        table_id=7,
        snapshot_id=snapshot_id,
        schema_version=0,
        exported_at="2025-10-26T11:02:48Z",
    )
    loc_uri = location if location.startswith("file://") else "file://" + location
    metadata = build_table_metadata(
        schema,
        spec,
        loc_uri,
        f"{loc_uri}/metadata/snap.avro",
        counters,
        prov,
        table_uuid=uuid.uuid4(),
    )
    io = PyArrowFileIO()
    mjson = f"{loc_uri}/metadata/v1.metadata.json"
    write_metadata_json(io, metadata, mjson)
    # pointer
    pointer = ExportPointer(
        version=1,
        metadata_location=mjson,
        metadata_version=1,
        location=loc_uri,
        glue_database=settings_obj.glue_database,
        glue_table=normalize_table_name(table),
        ducklake_catalog="lake",
        ducklake_schema="sales",
        ducklake_table=table,
        table_id=7,
        ducklake_snapshot_id=snapshot_id,
        schema_version=0,
        exported_at="2025-10-26T11:02:48Z",
        ducklake_iceberg_version="0.1.0",
    )
    write_pointer(io, pointer)
    return loc_uri


def test_resolve_prefers_explicit(settings: DuckLakeIcebergSettings) -> None:
    loc, pointer = resolve_metadata_location(
        settings, "sales", "orders", explicit="s3://x/m.json"
    )
    assert loc == "s3://x/m.json"
    assert pointer is None


def test_resolve_uses_pointer(settings: DuckLakeIcebergSettings) -> None:
    loc_uri = _write_table_metadata(settings, "orders", snapshot_id=42)
    resolved, pointer = resolve_metadata_location(settings, "sales", "orders")
    assert resolved == f"{loc_uri}/metadata/v1.metadata.json"
    assert pointer is not None
    assert pointer.ducklake_snapshot_id == 42


@mock_aws
def test_register_coherence_ok(settings: DuckLakeIcebergSettings) -> None:
    # _check_coherence accepts when the metadata snapshot matches the pointer.
    from ducklake_iceberg.register import _check_coherence

    pointer = _pointer("file:///x/t", snapshot_id=42)
    _check_coherence(
        {"ducklake.snapshot_id": "42", "ducklake.table": "orders"},
        pointer=pointer,
        schema_name="sales",
        table_name="orders",
        metadata_location="s3://x/m.json",
    )  # does not raise


def test_register_coherence_divergence() -> None:
    # pointer says snapshot 99, metadata says 42 -> DivergenceError (exit 3)
    from ducklake_iceberg.register import _check_coherence

    pointer = _pointer("file:///x/t", snapshot_id=99)
    with pytest.raises(DivergenceError, match="snapshot"):
        _check_coherence(
            {"ducklake.snapshot-id": "42"},
            pointer=pointer,
            schema_name="sales",
            table_name="orders",
            metadata_location="s3://x/m.json",
        )


def test_register_explicit_wrong_table() -> None:
    # --metadata-location of another table -> DivergenceError
    from ducklake_iceberg.register import _check_coherence

    with pytest.raises(DivergenceError, match="table"):
        _check_coherence(
            {"ducklake.table": "other"},
            pointer=None,
            schema_name="sales",
            table_name="orders",
            metadata_location="s3://x/m.json",
        )


def test_register_no_export(settings: DuckLakeIcebergSettings) -> None:
    with pytest.raises(PointerError, match="no export found"):
        run_register(settings, "sales", "orders")
