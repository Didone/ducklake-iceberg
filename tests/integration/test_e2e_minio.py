"""End to end, zero-copy: DuckLake (SQLite) -> ``sync`` -> Iceberg in MinIO.

The Iceberg metadata goes to a real S3-compatible store (MinIO, through the
``s3_endpoint`` setting) and the table is registered in a simulated Glue
(moto). After every ``sync`` the tests check that:

* the DuckLake data files were not modified or deleted (SHA-256 of the whole
  ``data_path``) and that only DuckLake's own flush of inlined rows added any;
* every Iceberg data file is a live DuckLake file (same path and size);
* the metadata bucket holds only Iceberg metadata and position-delete files:
  no data file was copied into it;
* PyIceberg reads exactly the rows DuckLake holds, with the same field ids.

One self-contained test runs the complete lifecycle in explicit order. Each
phase retains its original assertions; no phase depends on another pytest case.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import time
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import boto3
import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
import pytest
import yaml
from moto import mock_aws
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, BooleanExpression
from pyiceberg.expressions import EqualTo as _EqualTo
from pyiceberg.table import Table
from typer.testing import CliRunner

from ducklake_iceberg.cli import app
from tests.conftest import reset_process_state


def EqualTo(column: str, value: Any) -> BooleanExpression:  # noqa: N802
    """``column == value`` (pyiceberg's constructor is untyped)."""
    return cast("BooleanExpression", cast(Any, _EqualTo)(column, value))


pytestmark = [pytest.mark.integration, pytest.mark.minio]

runner = CliRunner()

DEFAULT_IMAGE = "pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
KEY = SECRET = "minioadmin"
BUCKET = "metadata-bucket"
REGION = "us-east-1"
DATABASE = "analytics"


@dataclass
class Lake:
    root: Path
    endpoint: str
    config: Path
    env: dict[str, str]

    @property
    def data(self) -> Path:
        return self.root / "data"

    # -- DuckLake ----------------------------------------------------------
    def write(self, *statements: str) -> None:
        con = duckdb.connect(":memory:")
        try:
            for ext in ("ducklake", "sqlite"):
                con.execute(f"INSTALL {ext}")
                con.execute(f"LOAD {ext}")
            con.execute(
                f"ATTACH 'ducklake:sqlite:{self.root}/catalog.sqlite' AS lake "
                f"(DATA_PATH '{self.data}/', METADATA_CATALOG 'lake_meta')"
            )
            for statement in statements:
                con.execute(statement)
        finally:
            con.close()

    def query(self, sql: str) -> Any:
        con = duckdb.connect(":memory:")
        try:
            for ext in ("ducklake", "sqlite"):
                con.execute(f"LOAD {ext}")
            con.execute(
                f"ATTACH 'ducklake:sqlite:{self.root}/catalog.sqlite' AS lake "
                f"(DATA_PATH '{self.data}/', METADATA_CATALOG 'lake_meta', "
                "READ_ONLY)"
            )
            return con.sql(sql).to_arrow_table()
        finally:
            con.close()

    def live_files(self, schema: str, table: str) -> set[tuple[str, int]]:
        rows = self.query(
            "SELECT data_file, data_file_size_bytes FROM "
            f"ducklake_list_files('lake', '{table}', schema => '{schema}')"
        ).to_pylist()
        return {
            (Path(r["data_file"]).resolve().as_posix(), int(r["data_file_size_bytes"]))
            for r in rows
        }

    # -- data_path snapshot --------------------------------------------------
    def tree(self) -> dict[str, str]:
        return {
            str(p.relative_to(self.data)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(self.data.rglob("*"))
            if p.is_file()
        }

    # -- the metadata bucket (read through PyArrow, not through boto3) -------
    def bucket_objects(self) -> dict[str, int]:
        fs = pafs.S3FileSystem(
            access_key=KEY,
            secret_key=SECRET,
            endpoint_override=self.endpoint.removeprefix("http://"),
            scheme="http",
            region=REGION,
        )
        infos = fs.get_file_info(pafs.FileSelector(BUCKET, recursive=True))
        return {
            i.path.removeprefix(f"{BUCKET}/"): i.size
            for i in infos
            if i.type == pafs.FileType.File
        }

    def bucket_hashes(self, keys: list[str]) -> set[str]:
        fs = pafs.S3FileSystem(
            access_key=KEY,
            secret_key=SECRET,
            endpoint_override=self.endpoint.removeprefix("http://"),
            scheme="http",
            region=REGION,
        )
        out = set()
        for key in keys:
            with fs.open_input_stream(f"{BUCKET}/{key}") as stream:
                out.add(hashlib.sha256(stream.read()).hexdigest())
        return out

    # -- the tool ----------------------------------------------------------
    def sync(self, table: str, expect: int = 0) -> dict[str, Any]:
        result = runner.invoke(
            app,
            ["sync", "--table", table, "--config", str(self.config), "--json"],
            env=self.env,
        )
        assert result.exit_code == expect, result.stdout + str(result.stderr)
        envelope: dict[str, Any] = json.loads(result.stdout)
        return envelope

    def iceberg(self, name: str) -> Table:
        catalog = load_catalog(
            "glue",
            **{
                "type": "glue",
                "glue.region": REGION,
                "s3.endpoint": self.endpoint,
                "s3.access-key-id": KEY,
                "s3.secret-access-key": SECRET,
                "s3.region": REGION,
            },
        )
        return catalog.load_table(f"{DATABASE}.{name}")


# -- fixtures --------------------------------------------------------------
@pytest.fixture(scope="module")
def minio_container() -> Iterator[Any]:
    try:
        from testcontainers.core.container import DockerContainer

        container = (
            DockerContainer(
                os.environ.get("DUCKLAKE_ICEBERG_MINIO_IMAGE", DEFAULT_IMAGE)
            )
            .with_env("MINIO_ROOT_USER", KEY)
            .with_env("MINIO_ROOT_PASSWORD", SECRET)
            .with_command("server /data")
            .with_exposed_ports(9000)
        )
        container.start()
    except Exception as exc:  # no Docker daemon / image pull failure
        pytest.skip(f"Docker/MinIO unavailable: {exc}")
    try:
        endpoint = (
            f"http://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(9000)}"
        )
        for _ in range(60):
            try:
                with urllib.request.urlopen(f"{endpoint}/minio/health/live") as r:  # noqa: S310
                    if r.status == 200:
                        break
            except OSError:
                time.sleep(1)
        else:
            pytest.skip("MinIO did not become healthy")
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=REGION,
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET,
        )
        s3.create_bucket(Bucket=BUCKET)
        container.endpoint = endpoint  # type: ignore[attr-defined]
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def minio_endpoint(minio_container: Any) -> str:
    return str(minio_container.endpoint)


@pytest.fixture
def lake(minio_endpoint: str, tmp_path: Path) -> Iterator[Lake]:
    root = tmp_path / "lake"
    (root / "data").mkdir(parents=True)
    config = root / "ducklake-iceberg.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "catalog_dsn": f"ducklake:sqlite:{root}/catalog.sqlite",
                "data_path": f"{root}/data/",
                "iceberg_metadata_prefix": f"s3://{BUCKET}/iceberg/",
                "glue_database": DATABASE,
                "aws_region": REGION,
                "s3_endpoint": minio_endpoint,
            }
        ),
        encoding="utf-8",
    )
    env = {
        "AWS_ACCESS_KEY_ID": KEY,
        "AWS_SECRET_ACCESS_KEY": SECRET,
        "AWS_DEFAULT_REGION": REGION,
        "DUCKLAKE_ICEBERG_STATE_DIR": str(root / "state"),
    }
    with pytest.MonkeyPatch.context() as patch:
        for name in ("AWS_PROFILE", "AWS_ENDPOINT_URL"):
            patch.delenv(name, raising=False)
        with mock_aws():
            # mock_aws (and other test modules, at import time) install fake
            # AWS credentials; MinIO is not mocked (PyArrow talks to it
            # directly), so it needs the real ones and no stray session token
            for name in ("AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
                patch.delenv(name, raising=False)
            for name, value in env.items():
                patch.setenv(name, value)
            boto3.client("glue", region_name=REGION).create_database(
                DatabaseInput={"Name": DATABASE}
            )
            yield Lake(root, minio_endpoint, config, env)


# -- audits ----------------------------------------------------------------
def _rows(table: pa.Table, order: str) -> list[dict[str, Any]]:
    def norm(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: norm(v) for k, v in value.items()}
        if isinstance(value, list | tuple):
            items = [norm(v) for v in value]
            if items and all(isinstance(i, list) and len(i) == 2 for i in items):
                return dict(items)  # a map read back as pairs
            return items
        if isinstance(value, float):
            return round(value, 5)
        if isinstance(value, dt.datetime):
            return value.replace(tzinfo=None)
        return value

    return [
        {k: norm(v) for k, v in row.items()} for row in table.sort_by(order).to_pylist()
    ]


def audit(
    lake: Lake,
    schema: str,
    name: str,
    before: dict[str, str],
    *,
    flush_may_add_files: bool = False,
    order: str = "id",
) -> Table:
    """Zero-copy and equality checks of one table after a sync."""
    after = lake.tree()
    modified = [k for k in before if k in after and before[k] != after[k]]
    removed = [k for k in before if k not in after]
    assert not modified, f"DuckLake files modified: {modified}"
    assert not removed, f"DuckLake files removed: {removed}"
    if not flush_may_add_files:
        assert set(after) == set(before), "sync added files to the DuckLake data_path"

    table = lake.iceberg(name)
    # same values, same field ids
    dl_rows = _rows(lake.query(f"SELECT * FROM lake.{schema}.{name}"), order)
    ice_rows = _rows(table.scan().to_arrow(), order)
    assert ice_rows == dl_rows
    column_ids = {
        r["column_name"]: r["column_id"]
        for r in lake.query(
            "SELECT column_id, column_name FROM lake_meta.ducklake_column "
            "WHERE end_snapshot IS NULL AND parent_column IS NULL AND table_id = "
            "(SELECT table_id FROM lake_meta.ducklake_table "
            f"WHERE table_name = '{name}' AND end_snapshot IS NULL)"
        ).to_pylist()
    }
    assert {f.name: f.field_id for f in table.schema().fields} == column_ids

    # every Iceberg data file IS a live DuckLake file
    files = table.inspect.data_files().to_pylist()
    iceberg_files = {
        (
            Path(f["file_path"].removeprefix("file://")).resolve().as_posix(),
            f["file_size_in_bytes"],
        )
        for f in files
        if f["content"] == 0
    }
    assert iceberg_files == lake.live_files(schema, name)
    # tool-written delete files live in the metadata bucket
    for f in files:
        if f["content"] != 0:
            assert f["file_path"].startswith(f"s3://{BUCKET}/")
            assert "/deletes/" in f["file_path"]

    # the bucket holds only metadata and position deletes: nothing is a copy
    objects = lake.bucket_objects()
    unexpected = [
        k
        for k in objects
        if not (
            k.endswith(
                (".metadata.json", ".avro", "_ducklake_iceberg_last_export.json")
            )
            or ("/deletes/" in k and k.endswith(".parquet"))
        )
    ]
    assert not unexpected, unexpected
    parquet = [k for k in objects if k.endswith(".parquet")]
    assert not (lake.bucket_hashes(parquet) & set(after.values())), "data file copied"
    return table


def _count(table: Table, expr: BooleanExpression) -> int:
    return int(table.scan(row_filter=expr).to_arrow().num_rows)


def _dl_count(lake: Lake, schema: str, name: str, where: str) -> int:
    return int(
        lake.query(
            f"SELECT count(*) AS n FROM lake.{schema}.{name} WHERE {where}"
        ).to_pylist()[0]["n"]
    )


def step(
    lake: Lake,
    schema: str,
    name: str,
    *,
    expect: int = 0,
    flush: bool = False,
    order: str = "id",
) -> Table:
    before = lake.tree()
    lake.sync(f"{schema}.{name}", expect=expect)
    return audit(lake, schema, name, before, flush_may_add_files=flush, order=order)


# -- the lifecycle -----------------------------------------------------------
def _check_create_tables_and_insert(lake: Lake) -> None:
    lake.write(
        "CREATE SCHEMA lake.sales",
        """CREATE TABLE lake.sales.orders(id BIGINT, customer VARCHAR,
           amount DECIMAL(12,2), qty INTEGER, created TIMESTAMP, flag BOOLEAN,
           d DATE, tags VARCHAR[], addr STRUCT(city VARCHAR, zip VARCHAR),
           attrs MAP(VARCHAR, INTEGER), ratio DOUBLE, small SMALLINT, f FLOAT)""",
        """INSERT INTO lake.sales.orders SELECT i, 'cust_' || (i % 7),
           (i * 1.25)::DECIMAL(12,2), (i % 5)::INTEGER,
           TIMESTAMP '2024-01-01' + INTERVAL (i) HOUR, i % 2 = 0,
           DATE '2024-01-01' + (i % 30)::INTEGER, ['a', 'b' || i],
           {'city': 'c' || i, 'zip': 'z' || i}, MAP {'k': i::INTEGER}, i / 3.0,
           (i % 100)::SMALLINT, i / 7.0 FROM range(1, 2001) t(i)""",
        "CREATE TABLE lake.sales.events(id BIGINT, region VARCHAR, ts TIMESTAMP, "
        "payload VARCHAR)",
        "ALTER TABLE lake.sales.events SET PARTITIONED BY (region)",
        """INSERT INTO lake.sales.events SELECT i,
           ['north', 'south', 'east'][1 + i % 3],
           TIMESTAMP '2024-03-01' + INTERVAL (i) MINUTE, 'p' || i
           FROM range(1, 601) t(i)""",
        "CREATE TABLE lake.sales.tiny(id INTEGER, v VARCHAR)",
        "INSERT INTO lake.sales.tiny VALUES (1, 'a'), (2, 'b')",
    )
    step(lake, "sales", "orders")
    events = step(lake, "sales", "events")
    assert [f.name for f in events.spec().fields] == ["region"]
    for region in ("north", "south", "east"):
        assert _count(events, EqualTo("region", region)) == _dl_count(
            lake, "sales", "events", f"region = '{region}'"
        )
    # rows still inlined in the DuckLake catalog: the flush writes the Parquet
    step(lake, "sales", "tiny", flush=True)


def _check_insert_delete_update_merge(lake: Lake) -> None:
    lake.write(
        """INSERT INTO lake.sales.orders SELECT i, 'new_' || i, 1.00, 1,
           TIMESTAMP '2025-01-01', true, DATE '2025-01-01', ['x'],
           {'city': 'n', 'zip': '0'}, MAP {'a': 1}, 0.5, 1::SMALLINT, 0.25
           FROM range(5000, 5301) t(i)"""
    )
    step(lake, "sales", "orders")

    before_delete = lake.iceberg("orders")
    snapshot = before_delete.current_snapshot()
    assert snapshot is not None
    rows_before = before_delete.scan().to_arrow().num_rows
    lake.write("DELETE FROM lake.sales.orders WHERE id % 3 = 0 AND id < 2000")
    deleted = step(lake, "sales", "orders")
    assert deleted.scan().to_arrow().num_rows < rows_before
    # time travel: the data files of the earlier snapshot are still there
    assert (
        deleted.scan(snapshot_id=snapshot.snapshot_id).to_arrow().num_rows
        == rows_before
    )

    lake.write(
        "UPDATE lake.sales.orders SET customer = 'UPDATED', qty = qty + 100 "
        "WHERE id % 2 = 0 AND id < 1500"
    )
    step(lake, "sales", "orders")

    lake.write(
        """MERGE INTO lake.sales.orders AS t USING (
             SELECT i AS id, 'merged' AS customer, 7.77::DECIMAL(12,2) AS amount
             FROM range(1, 50) t(i)
             UNION ALL
             SELECT i, 'merged_new', 1.11::DECIMAL(12,2) FROM range(9000, 9040) t(i)
           ) AS s ON t.id = s.id
           WHEN MATCHED THEN UPDATE SET customer = s.customer, amount = s.amount
           WHEN NOT MATCHED THEN INSERT (id, customer, amount)
             VALUES (s.id, s.customer, s.amount)"""
    )
    step(lake, "sales", "orders")

    lake.write(
        "DELETE FROM lake.sales.events WHERE region = 'north' AND id % 2 = 0",
        "UPDATE lake.sales.events SET payload = 'upd' "
        "WHERE region = 'south' AND id < 300",
    )
    step(lake, "sales", "events", flush=True)


def _check_schema_evolution(lake: Lake) -> None:
    lake.write("ALTER TABLE lake.sales.orders ADD COLUMN note VARCHAR")
    step(lake, "sales", "orders")

    # DEFAULT on a table that already has files cannot be expressed in Iceberg v2
    lake.write("ALTER TABLE lake.sales.orders ADD COLUMN priority INTEGER DEFAULT 5")
    published = lake.iceberg("orders").metadata_location
    objects = lake.bucket_objects()
    envelope = lake.sync("sales.orders", expect=2)
    assert any(p["code"] == "blocked" for p in envelope["problems"])
    assert lake.iceberg("orders").metadata_location == published
    assert lake.bucket_objects() == objects  # the blocked sync wrote nothing
    lake.write("ALTER TABLE lake.sales.orders DROP COLUMN priority")
    step(lake, "sales", "orders", expect=4)

    lake.write("ALTER TABLE lake.sales.orders ADD COLUMN addr.country VARCHAR")
    table = step(lake, "sales", "orders")
    country = table.schema().find_field("addr.country")
    assert not country.required  # older files do not contain it

    lake.write("ALTER TABLE lake.sales.orders RENAME COLUMN customer TO client")
    step(lake, "sales", "orders")
    lake.write("ALTER TABLE lake.sales.orders DROP COLUMN ratio")
    step(lake, "sales", "orders")
    lake.write(
        "ALTER TABLE lake.sales.orders ALTER qty SET DATA TYPE BIGINT",
        "ALTER TABLE lake.sales.orders ALTER f SET DATA TYPE DOUBLE",
    )
    step(lake, "sales", "orders")
    lake.write(
        """INSERT INTO lake.sales.orders SELECT i, 'evo', 2.00, 2,
           TIMESTAMP '2025-02-02', false, DATE '2025-02-02', ['e'],
           {'city': 'e', 'zip': 'e', 'country': 'BR'}, MAP {'e': 2},
           3::SMALLINT, 1.5, 'n' || i FROM range(20000, 20201) t(i)"""
    )
    table = step(lake, "sales", "orders")
    assert {f.name for f in table.schema().fields} >= {"client", "note"}
    assert "customer" not in {f.name for f in table.schema().fields}

    # a DEFAULT column added before any data file exists is fine
    lake.write(
        "CREATE TABLE lake.sales.defaults(id BIGINT)",
        "ALTER TABLE lake.sales.defaults ADD COLUMN p INTEGER DEFAULT 5",
        "INSERT INTO lake.sales.defaults SELECT i, 7 FROM range(100) t(i)",
    )
    step(lake, "sales", "defaults")


def _check_partition_evolution(lake: Lake) -> None:
    def regions() -> None:
        events = lake.iceberg("events")
        for region in ("north", "south", "east", "west"):
            assert _count(events, EqualTo("region", region)) == _dl_count(
                lake, "sales", "events", f"region = '{region}'"
            )

    lake.write(
        "ALTER TABLE lake.sales.events SET PARTITIONED BY (region, bucket(4, id))",
        """INSERT INTO lake.sales.events SELECT i,
           ['north', 'south', 'east', 'west'][1 + i % 4],
           TIMESTAMP '2025-04-01' + INTERVAL (i) MINUTE, 'b' || i
           FROM range(4000, 4301) t(i)""",
    )
    events = step(lake, "sales", "events")
    specs = {
        s.spec_id: [
            f"{events.schema().find_column_name(f.source_id)}:{f.transform}"
            for f in s.fields
        ]
        for s in events.metadata.partition_specs
    }
    assert ["region:identity", "id:bucket[4]"] in specs.values()
    regions()
    assert _count(events, And(EqualTo("region", "north"), EqualTo("id", 4004))) == (
        _dl_count(lake, "sales", "events", "region = 'north' AND id = 4004")
    )
    # the region field keeps ONE field id across specs
    ids = {
        f.name: f.field_id
        for s in events.metadata.partition_specs
        for f in s.fields
        if f.name == "region"
    }
    assert (
        len(
            {
                f.field_id
                for s in events.metadata.partition_specs
                for f in s.fields
                if f.name == "region"
            }
        )
        == 1
    ), ids

    lake.write(
        "ALTER TABLE lake.sales.events RESET PARTITIONED BY",
        "INSERT INTO lake.sales.events SELECT i, 'north', TIMESTAMP '2025-05-01', "
        "'r' || i FROM range(6000, 6101) t(i)",
    )
    step(lake, "sales", "events")
    regions()

    lake.write(
        "ALTER TABLE lake.sales.events SET PARTITIONED BY (region)",
        "INSERT INTO lake.sales.events SELECT i, 'south', TIMESTAMP '2025-06-01', "
        "'s' || i FROM range(7000, 7101) t(i)",
    )
    step(lake, "sales", "events")
    regions()

    # calendar transforms do not map to Iceberg: published unpartitioned, still correct
    lake.write(
        "ALTER TABLE lake.sales.events SET PARTITIONED BY (year(ts), month(ts))",
        "INSERT INTO lake.sales.events SELECT i, 'east', TIMESTAMP '2026-01-15', "
        "'y' || i FROM range(8000, 8101) t(i)",
    )
    step(lake, "sales", "events")
    regions()

    # deletes and updates across files of several partition specs
    lake.write(
        "DELETE FROM lake.sales.events WHERE id % 7 = 0",
        "UPDATE lake.sales.events SET payload = 'mixed' WHERE id % 11 = 0",
    )
    step(lake, "sales", "events", flush=True)
    regions()

    lake.write(
        "ALTER TABLE lake.sales.orders SET PARTITIONED BY (client)",
        """INSERT INTO lake.sales.orders SELECT i, 'cust_' || (i % 3), 5.5, 1,
           TIMESTAMP '2025-07-01', true, DATE '2025-07-01', ['p'],
           {'city': 'p', 'zip': 'p', 'country': 'AR'}, MAP {'p': 1}, 1::SMALLINT,
           2.5, 'pp' FROM range(30000, 30201) t(i)""",
    )
    orders = step(lake, "sales", "orders")
    assert [f.name for f in orders.spec().fields] == ["client"]
    assert _count(orders, EqualTo("client", "cust_1")) == _dl_count(
        lake, "sales", "orders", "client = 'cust_1'"
    )


def _check_second_run_changes_nothing(lake: Lake) -> None:
    objects = lake.bucket_objects()
    for name in ("orders", "events", "tiny", "defaults"):
        lake.sync(f"sales.{name}", expect=4)
    assert lake.bucket_objects() == objects


def _dry_run(lake: Lake, table: str, expect: int) -> list[dict[str, Any]]:
    before_tree, before_bucket = lake.tree(), lake.bucket_objects()
    result = runner.invoke(
        app,
        ["sync", "--table", table, "--dry-run", "--config", str(lake.config), "--json"],
        env=lake.env,
    )
    assert result.exit_code == expect, result.stdout
    assert lake.tree() == before_tree  # nothing flushed into DuckLake
    assert lake.bucket_objects() == before_bucket  # no metadata written
    steps: list[dict[str, Any]] = json.loads(result.stdout)["results"][0]["steps"]
    return steps


def _check_dry_run_is_a_read_only_monitor(lake: Lake) -> None:
    # up to date: exit 4, Glue already points at the last export
    steps = _dry_run(lake, "sales.orders", expect=4)
    assert steps[-1]["glue"] == "in_sync"

    # new rows in DuckLake: exit 0 (something to publish), Glue would advance
    lake.write(
        "INSERT INTO lake.sales.orders (id, client) "
        "SELECT i, 'monitor' FROM range(40000, 40300) t(i)"
    )
    steps = _dry_run(lake, "sales.orders", expect=0)
    assert [s["step"] for s in steps] == ["doctor", "export", "register"]
    assert steps[-1]["glue"] == "in_sync"  # the new export is not written

    # pending inlined rows: exit 0, reported instead of planned
    lake.write("INSERT INTO lake.sales.tiny VALUES (50, 'z')")
    steps = _dry_run(lake, "sales.tiny", expect=0)
    assert steps[1]["pending_inlined_rows"] == 1

    # an external write to the mirror: exit 3, with nothing written
    table = lake.iceberg("defaults")
    table.append(
        pa.table({"id": pa.array([999], pa.int64()), "p": pa.array([1], pa.int32())})
    )
    _dry_run(lake, "sales.defaults", expect=3)
    # the external writer's data file is not ours; drop it so the bucket audit
    # of the other tables keeps checking that the tool writes no data files
    fs = pafs.S3FileSystem(
        access_key=KEY,
        secret_key=SECRET,
        endpoint_override=lake.endpoint.removeprefix("http://"),
        scheme="http",
        region=REGION,
    )
    fs.delete_dir_contents(f"{BUCKET}/iceberg/{DATABASE}/defaults/data")

    # the real sync of the pending changes still works afterwards
    step(lake, "sales", "orders")
    step(lake, "sales", "tiny", flush=True)


# -- DuckLake data on S3, synced by a least-privilege principal ---------------
DATA_BUCKET = "data-bucket"
SYNC_KEY, SYNC_SECRET = "sync-user", "sync-user-secret"
POLICY = Path(__file__).parents[2] / "docs" / "deploy" / "iam" / "sync-policy.json"


def _render_policy(**values: str) -> dict[str, Any]:
    text = POLICY.read_text(encoding="utf-8")
    for name, value in values.items():
        text = text.replace(f"<{name}>", value)
    assert "<" not in text, "unrendered placeholder"
    policy: dict[str, Any] = json.loads(text)
    return policy


def _mc(container: Any, *args: str) -> str:
    code, output = container.exec(["mc", *args])
    text = output.decode() if isinstance(output, bytes) else str(output)
    assert code == 0, text
    return text


_NO_MINIO = frozenset({"s3:PutObjectAcl"})


def _create_sync_user(container: Any, policy: dict[str, Any]) -> None:
    """A MinIO user restricted to the S3 statements of the shipped policy."""
    s3_only = {
        "Version": policy["Version"],
        "Statement": [
            # MinIO has no object ACLs; every other action is enforced as on AWS
            {**st, "Action": [a for a in st["Action"] if a not in _NO_MINIO]}
            for st in policy["Statement"]
            if all(a.startswith("s3:") for a in st["Action"])
        ],
    }
    _mc(container, "alias", "set", "local", "http://127.0.0.1:9000", KEY, SECRET)
    document = json.dumps(s3_only).replace("'", "'\\''")
    target = "/data/.sync-policy.json"  # a path inside the MinIO container
    code, output = container.exec(["sh", "-c", f"printf '%s' '{document}' > {target}"])
    assert code == 0, output
    _mc(container, "admin", "policy", "create", "local", "sync", target)
    _mc(container, "admin", "user", "add", "local", SYNC_KEY, SYNC_SECRET)
    _mc(container, "admin", "policy", "attach", "local", "sync", "--user", SYNC_KEY)


def _check_s3_lake_synced_with_the_shipped_least_privilege_policy(
    lake: Lake, minio_container: Any, tmp_path: Path
) -> None:
    """Data and metadata on S3, using ``docs/deploy/iam/sync-policy.json``.

    MinIO enforces S3 statements, including the deny on deleting DuckLake data.
    Every Glue operation must be granted by the policy's Glue statement.
    Existing-object hashes also verify immutability: allowing PutObject for
    flush files does not by itself prevent overwrites.
    """
    host = lake.endpoint.removeprefix("http://")
    boto3.client(
        "s3",
        endpoint_url=lake.endpoint,
        region_name=REGION,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    ).create_bucket(Bucket=DATA_BUCKET)
    policy = _render_policy(
        DATA_BUCKET=DATA_BUCKET,
        DATA_PREFIX="ducklake",
        METADATA_BUCKET=BUCKET,
        METADATA_PREFIX="iceberg-s3data",
        REGION=REGION,
        ACCOUNT="123456789012",
        GLUE_DATABASE=DATABASE,
    )
    _create_sync_user(minio_container, policy)
    glue_allowed = {
        a.removeprefix("glue:")
        for st in policy["Statement"]
        if st["Effect"] == "Allow"
        for a in st["Action"]
        if a.startswith("glue:")
    }

    def ducklake() -> duckdb.DuckDBPyConnection:
        """The application that owns the lake writes with full credentials."""
        con = duckdb.connect(":memory:")
        for ext in ("ducklake", "sqlite", "httpfs"):
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")
        con.execute(
            f"CREATE SECRET (TYPE s3, KEY_ID '{KEY}', SECRET '{SECRET}', "
            f"ENDPOINT '{host}', URL_STYLE 'path', USE_SSL false, "
            f"REGION '{REGION}')"
        )
        con.execute(
            f"ATTACH 'ducklake:sqlite:{tmp_path}/s3catalog.sqlite' AS lake "
            f"(DATA_PATH 's3://{DATA_BUCKET}/ducklake/', METADATA_CATALOG 'lake_meta')"
        )
        return con

    def data_objects() -> dict[str, tuple[int, str]]:
        fs = pafs.S3FileSystem(
            access_key=KEY,
            secret_key=SECRET,
            endpoint_override=host,
            scheme="http",
            region=REGION,
        )
        out = {}
        for info in fs.get_file_info(pafs.FileSelector(DATA_BUCKET, recursive=True)):
            if info.type == pafs.FileType.File:
                with fs.open_input_stream(info.path) as stream:
                    out[info.path] = (
                        info.size,
                        hashlib.sha256(stream.read()).hexdigest(),
                    )
        return out

    config = tmp_path / "s3.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "catalog_dsn": f"ducklake:sqlite:{tmp_path}/s3catalog.sqlite",
                "data_path": f"s3://{DATA_BUCKET}/ducklake/",
                "iceberg_metadata_prefix": f"s3://{BUCKET}/iceberg-s3data/",
                "glue_database": DATABASE,
                "aws_region": REGION,
                "s3_endpoint": lake.endpoint,
            }
        ),
        encoding="utf-8",
    )
    sync_env = {
        **lake.env,
        "AWS_ACCESS_KEY_ID": SYNC_KEY,
        "AWS_SECRET_ACCESS_KEY": SYNC_SECRET,
        "DUCKLAKE_ICEBERG_TIMINGS": "1",
    }
    glue_called: set[str] = set()

    def sync(expect: int = 0) -> None:
        result = runner.invoke(
            app,
            ["sync", "--table", "s3data.t", "--config", str(config), "--json"],
            env=sync_env,
        )
        assert result.exit_code == expect, result.stdout + str(result.stderr)
        counters = json.loads(result.stdout)["timings"]["counters"]
        glue_called.update(
            k.removeprefix("aws.glue.") for k in counters if k.startswith("aws.glue.")
        )

    def check() -> None:
        con = ducklake()
        try:
            live = {
                (r[0].removeprefix("s3://"), r[1])
                for r in con.execute(
                    "SELECT data_file, data_file_size_bytes FROM "
                    "ducklake_list_files('lake', 't', schema => 's3data')"
                ).fetchall()
            }
            expected = con.sql("SELECT * FROM lake.s3data.t ORDER BY id").fetchall()
        finally:
            con.close()
        table = lake.iceberg("t")
        files = table.inspect.data_files().to_pylist()
        assert {
            (f["file_path"].removeprefix("s3://"), f["file_size_in_bytes"])
            for f in files
            if f["content"] == 0
        } == live
        got = table.scan().to_arrow().sort_by("id").to_pylist()
        assert [tuple(r.values()) for r in got] == expected

    con = ducklake()
    try:
        con.execute("CREATE SCHEMA lake.s3data")
        con.execute("CREATE TABLE lake.s3data.t(id BIGINT, v VARCHAR)")
        con.execute(
            "INSERT INTO lake.s3data.t SELECT i, 'v' || i FROM range(1, 1001) t(i)"
        )
    finally:
        con.close()
    before = data_objects()
    sync()
    assert data_objects() == before  # not one data object was touched
    check()

    con = ducklake()
    try:
        con.execute("DELETE FROM lake.s3data.t WHERE id % 4 = 0")
        con.execute("UPDATE lake.s3data.t SET v = 'u' WHERE id % 5 = 0")
        con.execute(
            "INSERT INTO lake.s3data.t SELECT i, 'n' FROM range(2000, 2501) t(i)"
        )
        # a few rows stay inlined in the catalog: the sync must flush them
        con.execute("INSERT INTO lake.s3data.t VALUES (9001, 'inlined')")
    finally:
        con.close()
    before = data_objects()
    sync()
    after = data_objects()
    assert {k: v for k, v in after.items() if k in before} == before
    assert len(after) > len(before)  # the flush wrote the inlined row
    check()
    sync(expect=4)

    assert glue_called, "no Glue call recorded"
    assert glue_called <= glue_allowed, glue_called - glue_allowed


def test_complete_zero_copy_lifecycle(
    lake: Lake, minio_container: Any, tmp_path: Path
) -> None:
    """All lifecycle and IAM phases are explicit, independently runnable as a case."""
    reset_process_state()
    _check_create_tables_and_insert(lake)
    reset_process_state()
    _check_insert_delete_update_merge(lake)
    reset_process_state()
    _check_schema_evolution(lake)
    reset_process_state()
    _check_partition_evolution(lake)
    reset_process_state()
    _check_second_run_changes_nothing(lake)
    reset_process_state()
    _check_dry_run_is_a_read_only_monitor(lake)
    reset_process_state()
    _check_s3_lake_synced_with_the_shipped_least_privilege_policy(
        lake, minio_container, tmp_path
    )
