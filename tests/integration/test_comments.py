"""Documentation projection through real DuckLake snapshots and simulated Glue."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from unittest import mock

import boto3
import pytest
from moto import mock_aws
from pyiceberg.table import StaticTable

from tests.integration.conftest import CatalogFactory, run_sync, sync_config

pytestmark = [pytest.mark.integration, pytest.mark.aws]


@pytest.fixture
def glue_env() -> Iterator[None]:
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


def _table() -> dict[str, Any]:
    return cast(
        "dict[str, Any]",
        boto3.client("glue", region_name="us-east-1").get_table(
            DatabaseName="analytics", Name="t"
        )["Table"],
    )


def _metadata(doc: dict[str, Any]) -> dict[str, Any]:
    return cast(
        "dict[str, Any]",
        json.loads(Path(doc["results"][0]["metadata_location"]).read_text()),
    )


def _hashes(data: str) -> dict[str, str]:
    return {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in Path(data).rglob("*.parquet")
    }


def test_first_publish_comment_only_idempotent(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t(id INTEGER, note VARCHAR)",
        "INSERT INTO lake.main.t VALUES (1, 'one'), (2, 'two')",
        "COMMENT ON TABLE lake.main.t IS 'Descrição 🦆\nwith newline'",
        "COMMENT ON COLUMN lake.main.t.note IS 'écriture 🦆'",
    )
    config = sync_config(catalog, tmp_path)
    source = _hashes(catalog.data_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    from ducklake_iceberg import timing

    assert timing.counter("aws.glue.CreateTable") == 1
    assert timing.counter("aws.glue.UpdateTable") == 0
    first = _metadata(doc)
    assert (
        first["properties"]["comment"]
        == _table()["Description"]
        == "Descrição 🦆\nwith newline"
    )
    assert first["schemas"][-1]["fields"][1]["doc"] == "écriture 🦆"
    assert _table()["StorageDescriptor"]["Columns"][1]["Comment"] == "écriture 🦆"
    old_schemas = first["schemas"]
    for table_text, column_text in [
        ("second", "second column"),
        (None, None),
        ("", ""),
    ]:

        def literal(value: str | None) -> str:
            return "NULL" if value is None else "'" + value + "'"

        catalog.write(
            f"COMMENT ON TABLE lake.main.t IS {literal(table_text)}",
            f"COMMENT ON COLUMN lake.main.t.note IS {literal(column_text)}",
        )
        code, doc = run_sync(config)
        if table_text == "":
            assert code == 4, doc
            continue
        assert code == 0, doc["problems"]
        current = _metadata(doc)
        assert current["current-snapshot-id"] == first["current-snapshot-id"]
        assert current["snapshots"] == first["snapshots"]
        assert current["last-sequence-number"] == first["last-sequence-number"]
        assert current["schemas"][: len(old_schemas)] == old_schemas
        assert (current["properties"].get("comment") or None) == (table_text or None)
        assert (_table().get("Description") or None) == (table_text or None)
        assert (_table()["StorageDescriptor"]["Columns"][1].get("Comment") or None) == (
            column_text or None
        )
        assert _hashes(catalog.data_path) == source
        rows = (
            StaticTable.from_metadata(doc["results"][0]["metadata_location"])
            .scan()
            .to_arrow()
            .to_pylist()
        )
        assert rows == [{"id": 1, "note": "one"}, {"id": 2, "note": "two"}]
        published = doc["results"][0]["metadata_location"]
        code, repeated = run_sync(config)
        assert code == 4, repeated
        assert repeated["results"][0]["metadata_location"] == published


@pytest.mark.parametrize("documented", [True, False])
def test_comment_drift_dry_run_and_repair(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    documented: bool,
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t(id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
        "COMMENT ON TABLE lake.main.t IS "
        + ("'source table'" if documented else "NULL"),
        "COMMENT ON COLUMN lake.main.t.id IS "
        + ("'source column'" if documented else "NULL"),
    )
    config = sync_config(catalog, tmp_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    metadata = doc["results"][0]["metadata_location"]
    before = Path(metadata).read_bytes()
    client = boto3.client("glue", region_name="us-east-1")
    table = _table()
    columns = table["StorageDescriptor"]["Columns"]
    columns[0]["Comment"] = "manual column"
    client.update_table(
        DatabaseName="analytics",
        TableInput={
            "Name": "t",
            "Owner": "preserved",
            "Description": "manual table",
            "TableType": table["TableType"],
            "Parameters": {**table["Parameters"], "custom": "value"},
            "StorageDescriptor": {**table["StorageDescriptor"], "Columns": columns},
        },
    )
    code, dry = run_sync(config, "--dry-run")
    assert code == 0, dry
    assert _table()["Description"] == "manual table"
    code, repaired = run_sync(config)
    assert code == 0, repaired
    assert (_table().get("Description") or None) == (
        "source table" if documented else None
    )
    assert (_table()["StorageDescriptor"]["Columns"][0].get("Comment") or None) == (
        "source column" if documented else None
    )
    assert _table()["Owner"] == "preserved"
    assert _table()["Parameters"]["custom"] == "value"
    assert repaired["results"][0]["metadata_location"] == metadata
    assert Path(metadata).read_bytes() == before
    code, repeated = run_sync(config)
    assert code == 4, repeated


@pytest.mark.parametrize(
    ("target", "text"),
    [
        ("TABLE lake.main.t", "a" * 2049),
        ("COLUMN lake.main.t.id", "é" * 128),
        ("COLUMN lake.main.t.id", "line\nbreak"),
    ],
)
def test_invalid_comment_blocks_before_flush(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    target: str,
    text: str,
) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.main.t(id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
        f"COMMENT ON {target} IS '{text}'",
    )
    config = sync_config(catalog, tmp_path)
    with mock.patch(
        "ducklake_iceberg.pipeline.flush_inlined",
        side_effect=AssertionError("flush forbidden"),
    ):
        code, doc = run_sync(config)
    assert code == 2, doc
    assert not list((tmp_path / "iceberg").rglob("*.json"))
    assert (
        boto3.client("glue", region_name="us-east-1").get_databases()["DatabaseList"]
        == []
    )


def test_snapshot_rename_recreate_and_quoted_column(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        'CREATE TABLE lake.main.t(id INTEGER, "a.b" VARCHAR)',
        "INSERT INTO lake.main.t VALUES (1, 'value')",
        "COMMENT ON COLUMN lake.main.t.\"a.b\" IS 'original'",
    )
    config = sync_config(catalog, tmp_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    old = _metadata(doc)
    fid = _table()["StorageDescriptor"]["Columns"][1]["Parameters"]["iceberg.field.id"]
    catalog.write('ALTER TABLE lake.main.t RENAME COLUMN "a.b" TO renamed')
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    live = _table()["StorageDescriptor"]["Columns"][1]
    assert live["Name"] == "renamed"
    assert live["Comment"] == "original"
    assert live["Parameters"]["iceberg.field.id"] == fid
    assert _metadata(doc)["schemas"][: len(old["schemas"])] == old["schemas"]
    catalog.write(
        "ALTER TABLE lake.main.t DROP COLUMN renamed",
        'ALTER TABLE lake.main.t ADD COLUMN "a.b" VARCHAR',
    )
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    current = [
        c
        for c in _table()["StorageDescriptor"]["Columns"]
        if c["Parameters"]["iceberg.field.current"] == "true"
    ]
    replacement = next(c for c in current if c["Name"] == "a.b")
    assert replacement["Parameters"]["iceberg.field.id"] != fid
    assert not replacement.get("Comment")
    assert StaticTable.from_metadata(
        doc["results"][0]["metadata_location"]
    ).scan().to_arrow().to_pylist() == [{"id": 1, "a.b": None}]


def test_snapshot_concurrent_comment_change(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ducklake_iceberg.export as export

    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t(id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
        "COMMENT ON TABLE lake.main.t IS 'selected snapshot'",
    )
    config = sync_config(catalog, tmp_path)
    check = export._check_source_locations
    changed = False

    def concurrent(*args: Any, **kwargs: Any) -> Any:
        nonlocal changed
        result = check(*args, **kwargs)
        if not changed:
            changed = True
            catalog.write("COMMENT ON TABLE lake.main.t IS 'next snapshot'")
        return result

    monkeypatch.setattr(export, "_check_source_locations", concurrent)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    assert (
        _table()["Description"]
        == _metadata(doc)["properties"]["comment"]
        == "selected snapshot"
    )
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    assert _table()["Description"] == "next snapshot"


def test_legacy_export_gains_comments(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t(id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
    )
    config = sync_config(catalog, tmp_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    previous = _metadata(doc)
    pointer = next((tmp_path / "iceberg").rglob("_ducklake_iceberg_last_export.json"))
    handoff = json.loads(pointer.read_text())
    handoff.pop("comments_fingerprint")
    pointer.write_text(json.dumps(handoff))
    catalog.write("COMMENT ON TABLE lake.main.t IS 'legacy upgrade'")
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    assert _table()["Description"] == "legacy upgrade"
    assert _metadata(doc)["current-snapshot-id"] == previous["current-snapshot-id"]


@pytest.mark.parametrize("boundary", ["write_pointer", "register"])
def test_comment_recovery_before_newer_source(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    boundary: str,
) -> None:
    import ducklake_iceberg.glue as glue

    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t(id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
        "COMMENT ON TABLE lake.main.t IS 'original'",
    )
    config = sync_config(catalog, tmp_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    catalog.write("COMMENT ON TABLE lake.main.t IS 'pending'")
    target = (
        "ducklake_iceberg.export.write_pointer"
        if boundary == "write_pointer"
        else "ducklake_iceberg.pipeline.run_register"
    )
    with mock.patch(target, side_effect=OSError("injected")):
        code, failed = run_sync(config)
    assert code == 1, failed
    assert _table()["Description"] == "original"
    pending = next((tmp_path / "iceberg").rglob("v2.metadata.json"))
    immutable = pending.read_bytes()
    catalog.write("COMMENT ON TABLE lake.main.t IS 'newer'")
    client = glue._glue_client(
        glue.GlueTarget("analytics", "t", "us-east-1", None, None)
    )
    update = client.update_table
    descriptions = []

    def observe(**kwargs: Any) -> Any:
        descriptions.append(kwargs["TableInput"]["Description"])
        return update(**kwargs)

    with mock.patch.object(client, "update_table", side_effect=observe):
        code, recovered = run_sync(config)
    assert code == 0, recovered["problems"]
    assert descriptions == ["pending", "newer"]
    assert _table()["Description"] == "newer"
    assert pending.read_bytes() == immutable


def test_duplicate_live_comment_is_error(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.main.t(id INTEGER)",
        "COMMENT ON COLUMN lake.main.t.id IS 'one'",
    )
    catalog.meta_exec(
        "INSERT INTO ducklake_column_tag SELECT * FROM ducklake_column_tag "
        "WHERE key='comment'"
    )
    code, doc = run_sync(sync_config(catalog, tmp_path))
    assert code == 1, doc
    assert not list((tmp_path / "iceberg").rglob("*.json"))


@pytest.mark.parametrize("width", [1, 64])
def test_wide_comments_keep_budget(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    width: int,
) -> None:
    from ducklake_iceberg import timing

    columns = ",".join(f"c{i} VARCHAR" for i in range(width))
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        f"CREATE TABLE lake.main.t({columns})",
        *[f"COMMENT ON COLUMN lake.main.t.c{i} IS 'doc {i}'" for i in range(width)],
    )
    config = sync_config(catalog, tmp_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    assert len(_table()["StorageDescriptor"]["Columns"]) == width
    code, repeated = run_sync(config)
    assert code == 4, repeated["problems"]
    assert timing.counter("duckdb.connect") == 1
    assert timing.counter("catalog.queries") <= 25
    assert timing.counter("aws.glue.GetTable") == 1
    assert timing.counter("aws.glue.UpdateTable") == 0


@pytest.mark.parametrize(
    "fault",
    ["timeout_after_commit", "throttle", "conflict", "external_writer", "denied"],
)
def test_comment_drift_retry_and_conflict(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
    fault: str,
) -> None:
    import copy

    from botocore.exceptions import ClientError, ReadTimeoutError

    from ducklake_iceberg.glue import GlueTarget, _glue_client

    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t(id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
        "COMMENT ON TABLE lake.main.t IS 'canonical'",
    )
    config = sync_config(catalog, tmp_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    metadata = doc["results"][0]["metadata_location"]
    client = _glue_client(GlueTarget("analytics", "t", "us-east-1", None, None))
    update = client.update_table
    table = _table()
    update(
        DatabaseName="analytics",
        TableInput={
            "Name": "t",
            "TableType": table["TableType"],
            "Description": "manual",
            "Parameters": table["Parameters"],
            "StorageDescriptor": table["StorageDescriptor"],
        },
    )
    attempts = 0

    def flaky(**kwargs: Any) -> Any:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            if fault == "timeout_after_commit":
                update(**kwargs)
                raise ReadTimeoutError(endpoint_url="http://simulated-glue")
            if fault in ("conflict", "external_writer"):
                rival = copy.deepcopy(kwargs)
                rival["TableInput"]["Description"] = "rival"
                rival["TableInput"]["Owner"] = "concurrent-owner"
                if fault == "external_writer":
                    rival["TableInput"]["Parameters"]["metadata_location"] = (
                        "s3://foreign/metadata.json"
                    )
                update(**rival)
            error = {
                "throttle": "ThrottlingException",
                "denied": "AccessDeniedException",
            }.get(fault, "ConcurrentModificationException")
            raise ClientError({"Error": {"Code": error}}, "UpdateTable")
        return update(**kwargs)

    with (
        mock.patch.object(client, "update_table", side_effect=flaky),
        mock.patch("ducklake_iceberg.retry.time.sleep"),
    ):
        code, result = run_sync(config)
    expected = 3 if fault == "external_writer" else 1 if fault == "denied" else 0
    assert code == expected, result["problems"]
    if expected == 0:
        assert _table()["Description"] == "canonical"
        assert result["results"][0]["metadata_location"] == metadata
        if fault == "conflict":
            assert _table()["Owner"] == "concurrent-owner"
        assert attempts == (1 if fault == "timeout_after_commit" else 2)
    elif fault == "denied":
        assert attempts == 1
        assert _table()["Description"] == "manual"
    else:
        assert attempts == 1
        assert (
            _table()["Parameters"]["metadata_location"] == "s3://foreign/metadata.json"
        )


def test_dry_run_with_inlined_comments_does_not_flush(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
) -> None:
    catalog = make_catalog(
        "CREATE TABLE lake.main.t(id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
        "COMMENT ON TABLE lake.main.t IS 'inlined'",
    )
    config = sync_config(catalog, tmp_path)
    with mock.patch(
        "ducklake_iceberg.pipeline.flush_inlined",
        side_effect=AssertionError("flush forbidden"),
    ):
        code, doc = run_sync(config, "--dry-run")
    assert code == 0, doc["problems"]
    assert not list((tmp_path / "iceberg").rglob("*.json"))
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    assert _table()["Description"] == "inlined"


def test_comment_history_snapshot_reads(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
) -> None:
    from ducklake_iceberg.catalog.connection import open_catalog
    from ducklake_iceberg.catalog.reader import current_snapshot, read_comments

    catalog = make_catalog(
        "CREATE TABLE lake.main.t(id INTEGER)",
        "COMMENT ON TABLE lake.main.t IS 'old table'",
        "COMMENT ON COLUMN lake.main.t.id IS 'old column'",
    )
    with (
        open_catalog(catalog.settings(), read_only=True, s3=False) as con,
        con.read_transaction(),
    ):
        old_snapshot = current_snapshot(con).snapshot_id
    catalog.write(
        "COMMENT ON TABLE lake.main.t IS 'new table'",
        "COMMENT ON COLUMN lake.main.t.id IS NULL",
    )
    with (
        open_catalog(catalog.settings(), read_only=True, s3=False) as con,
        con.read_transaction(),
    ):
        now = current_snapshot(con).snapshot_id
        # The table's ID is resolved by the same public metadata read as export.
        from ducklake_iceberg.catalog.reader import parse_table_ref, resolve_table

        identity = resolve_table(con, parse_table_ref("main.t"), current_snapshot(con))
        old = read_comments(con.raw, con.meta, identity.table_id, old_snapshot)
        new = read_comments(con.raw, con.meta, identity.table_id, now)
    assert old.table == "old table"
    assert old.columns[0][3] == "old column"
    assert new.table == "new table"
    assert new.columns[0][3] is None


def test_source_comment_update_preserves_column_parameters(
    make_catalog: CatalogFactory,
    tmp_path: Path,
    glue_env: None,
) -> None:
    catalog = make_catalog(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.t(id INTEGER)",
        "INSERT INTO lake.main.t VALUES (1)",
        "COMMENT ON COLUMN lake.main.t.id IS 'old'",
    )
    config = sync_config(catalog, tmp_path)
    code, doc = run_sync(config)
    assert code == 0, doc["problems"]
    table = _table()
    table["StorageDescriptor"]["Columns"][0]["Parameters"]["business.owner"] = "team"
    client = boto3.client("glue", region_name="us-east-1")
    client.update_table(
        DatabaseName="analytics",
        TableInput={
            "Name": "t",
            "TableType": table["TableType"],
            "Owner": "owner",
            "Parameters": {**table["Parameters"], "custom": "retained"},
            "StorageDescriptor": table["StorageDescriptor"],
        },
    )
    catalog.write("COMMENT ON COLUMN lake.main.t.id IS 'new'")
    code, result = run_sync(config)
    assert code == 0, result["problems"]
    current = _table()
    assert current["StorageDescriptor"]["Columns"][0]["Comment"] == "new"
    assert (
        current["StorageDescriptor"]["Columns"][0]["Parameters"]["business.owner"]
        == "team"
    )
    assert current["Owner"] == "owner"
    assert current["Parameters"]["custom"] == "retained"
