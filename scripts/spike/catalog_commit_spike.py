"""Phase-0 spike: publish a DuckLake table to an Iceberg catalog in *commit mode*.

Question answered: can an Iceberg catalog accept, without copying any data,

1. ``register_table`` of a ``v1.metadata.json`` written by ducklake-iceberg
   (field IDs equal to DuckLake ``column_id``s), and then
2. ``commit_table`` of snapshots whose manifest lists, manifests and position
   delete files were written by ducklake-iceberg, plus schema and partition
   evolution and the ``ducklake.*`` sync properties, guarded by requirements?

The spike reuses today's export (pointer mode) as the *planner*: each export
writes the next ``metadata.json`` of its own chain, and the difference between
two consecutive files is translated into ``TableUpdate``s and
``TableRequirement``s that are committed to the catalog. The real
implementation will plan against the catalog's metadata instead.

After every step the table is read back *through the catalog* and compared
with DuckLake (rows, field IDs, data files referenced in place). Finally an
external write is committed with PyIceberg and the next commit must be
rejected (``CommitFailedException``), which is how commit mode detects
divergence.

Usage (all storage on a MinIO/S3 endpoint)::

    python scripts/spike/catalog_commit_spike.py --catalog sql
    python scripts/spike/catalog_commit_spike.py --catalog rest --uri http://localhost:8181

Not shipped and not part of the test suite.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
import tempfile
import time
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

import boto3
import duckdb
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.update import (
    AddPartitionSpecUpdate,
    AddSchemaUpdate,
    AddSnapshotUpdate,
    AssertCurrentSchemaId,
    AssertRefSnapshotId,
    AssertTableUUID,
    RemoveSnapshotsUpdate,
    SetCurrentSchemaUpdate,
    SetDefaultSpecUpdate,
    SetPropertiesUpdate,
    SetSnapshotRefUpdate,
    TableRequirement,
    TableUpdate,
)

from ducklake_iceberg.config import DuckLakeIcebergSettings
from ducklake_iceberg.export import ExportResult, run_export
from ducklake_iceberg.writer.metadata import read_metadata_json

KEY = SECRET = "minioadmin"
REGION = "us-east-1"
NAMESPACE = "spike"

RESULTS: list[tuple[str, bool, str]] = []
#: "register" (register_table of v1) or "create" (catalogs without the
#: register endpoint: create_table with a placeholder schema, then a first
#: commit that adds the DuckLake schema with its own field ids)
BOOTSTRAP = "register"


def record(step: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((step, ok, detail))
    print(
        f"  {'PASS' if ok else 'FAIL'}  {step}" + (f"  -- {detail}" if detail else "")
    )


# -- DuckLake on MinIO --------------------------------------------------------
class Lake:
    def __init__(self, root: Path, endpoint: str, bucket: str) -> None:
        self.root, self.endpoint, self.bucket = root, endpoint, bucket
        self.host = endpoint.removeprefix("http://")

    def con(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        for ext in ("ducklake", "sqlite", "httpfs"):
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")
        con.execute(
            f"CREATE SECRET (TYPE s3, KEY_ID '{KEY}', SECRET '{SECRET}', "
            f"ENDPOINT '{self.host}', URL_STYLE 'path', USE_SSL false, "
            f"REGION '{REGION}')"
        )
        con.execute(
            f"ATTACH 'ducklake:sqlite:{self.root}/lake.sqlite' AS lake "
            f"(DATA_PATH 's3://{self.bucket}/ducklake/', METADATA_CATALOG 'lake_meta')"
        )
        return con

    def write(self, *sql: str) -> None:
        con = self.con()
        try:
            for statement in sql:
                con.execute(statement)
        finally:
            con.close()

    def rows(self, table: str) -> list[tuple[Any, ...]]:
        con = self.con()
        try:
            return sorted(con.sql(f"SELECT * FROM lake.{table}").fetchall())
        finally:
            con.close()

    def live_files(self, schema: str, table: str) -> set[str]:
        con = self.con()
        try:
            return {
                r[0]
                for r in con.execute(
                    "SELECT data_file FROM "
                    f"ducklake_list_files('lake', '{table}', schema => '{schema}')"
                ).fetchall()
            }
        finally:
            con.close()

    def column_ids(self, table: str) -> dict[str, int]:
        con = self.con()
        try:
            return {
                name: cid
                for cid, name in con.execute(
                    "SELECT column_id, column_name FROM lake_meta.ducklake_column "
                    "WHERE end_snapshot IS NULL AND parent_column IS NULL AND "
                    "table_id = (SELECT table_id FROM lake_meta.ducklake_table "
                    f"WHERE table_name = '{table}' AND end_snapshot IS NULL)"
                ).fetchall()
            }
        finally:
            con.close()


# -- translating two consecutive metadata files into a commit -----------------
def updates_between(
    old: TableMetadata, new: TableMetadata, catalog_base: TableMetadata
) -> tuple[tuple[TableRequirement, ...], tuple[TableUpdate, ...]]:
    """Updates from ``old`` to ``new`` (the tool's chain).

    The requirements use the catalog's identity (uuid, current schema id,
    which the catalog may have numbered differently) and *our* last published
    snapshot: a writer in between moves ``main`` and the commit is rejected.
    """
    updates: list[TableUpdate] = []
    old_schemas = {s.schema_id for s in old.schemas}
    for schema in new.schemas:
        if schema.schema_id not in old_schemas:
            updates.append(AddSchemaUpdate(schema_=schema))
    if new.current_schema_id != old.current_schema_id:
        # -1 = "the schema added last": the server may renumber schema ids
        updates.append(SetCurrentSchemaUpdate(schema_id=-1))
    old_specs = {s.spec_id for s in old.partition_specs}
    for spec in new.partition_specs:
        if spec.spec_id not in old_specs:
            updates.append(AddPartitionSpecUpdate(spec=spec))
    if new.default_spec_id != old.default_spec_id:
        added = [s for s in new.partition_specs if s.spec_id not in old_specs]
        if added:
            updates.append(SetDefaultSpecUpdate(spec_id=-1))
        else:  # back to an earlier spec
            updates.append(SetDefaultSpecUpdate(spec_id=new.default_spec_id))
    old_snapshots = {s.snapshot_id for s in old.snapshots}
    for snapshot in new.snapshots:
        if snapshot.snapshot_id not in old_snapshots:
            updates.append(AddSnapshotUpdate(snapshot=snapshot))
    removed = old_snapshots - {s.snapshot_id for s in new.snapshots}
    if removed:
        updates.append(RemoveSnapshotsUpdate(snapshot_ids=sorted(removed)))
    if new.current_snapshot_id != old.current_snapshot_id:
        assert new.current_snapshot_id is not None
        updates.append(
            SetSnapshotRefUpdate(
                ref_name="main", type="branch", snapshot_id=new.current_snapshot_id
            )
        )
    changed = {k: v for k, v in new.properties.items() if old.properties.get(k) != v}
    if changed:
        updates.append(SetPropertiesUpdate(updates=changed))
    requirements: tuple[TableRequirement, ...] = (
        AssertTableUUID(uuid=catalog_base.table_uuid),
        AssertRefSnapshotId(ref="main", snapshot_id=old.current_snapshot_id),
        AssertCurrentSchemaId(current_schema_id=catalog_base.current_schema_id),
    )
    return requirements, tuple(updates)


# -- the spike ----------------------------------------------------------------
def build_settings(
    root: Path, endpoint: str, bucket: str, metadata_prefix: str | None
) -> DuckLakeIcebergSettings:
    return DuckLakeIcebergSettings(
        catalog_dsn=f"ducklake:sqlite:{root}/lake.sqlite",
        data_path=f"s3://{bucket}/ducklake/",
        iceberg_metadata_prefix=metadata_prefix or f"s3://{bucket}/iceberg/",
        glue_database=NAMESPACE,
        aws_region=REGION,
        s3_endpoint=endpoint,
    )


def bootstrap_by_create(
    catalog: Catalog, ident: tuple[str, str], v1: ExportResult
) -> bool:
    """First publication without ``register_table``.

    ``create_table`` reassigns field ids, so it only creates an empty table
    with a throwaway one-column schema; the first ``commit_table`` then adds
    the real schema (ids preserved on the update path), the spec, the
    snapshot and the properties written by ducklake-iceberg.
    """
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType

    io = catalog.load_table  # noqa: F841 - keep the catalog warm
    placeholder = Schema(NestedField(1, "_placeholder", StringType(), required=False))
    location = v1.plan.location
    try:
        created = catalog.create_table(ident, placeholder, location=location)
    except Exception as exc:
        record(
            "create: create_table (placeholder)",
            False,
            f"{type(exc).__name__}: {exc}"[:300],
        )
        return False
    record("create: create_table (placeholder)", True, created.metadata_location)
    new = read_metadata_json(created.io, v1.plan.metadata_location)
    schema = new.schema()
    updates: list[TableUpdate] = [
        AddSchemaUpdate(schema_=schema),
        SetCurrentSchemaUpdate(schema_id=-1),
    ]
    spec = new.spec()
    if spec.fields:
        updates += [AddPartitionSpecUpdate(spec=spec), SetDefaultSpecUpdate(spec_id=-1)]
    snapshot = new.current_snapshot()
    assert snapshot is not None
    updates += [
        AddSnapshotUpdate(snapshot=snapshot),
        SetSnapshotRefUpdate(
            ref_name="main", type="branch", snapshot_id=snapshot.snapshot_id
        ),
        SetPropertiesUpdate(updates=dict(new.properties)),
    ]
    requirements = (
        AssertTableUUID(uuid=created.metadata.table_uuid),
        AssertRefSnapshotId(ref="main", snapshot_id=None),
    )
    try:
        catalog.commit_table(created, requirements, tuple(updates))
        record("create: first commit with the DuckLake schema accepted", True)
    except Exception as exc:
        record(
            "create: first commit with the DuckLake schema accepted",
            False,
            f"{type(exc).__name__}: {exc}"[:300],
        )
        return False
    return True


def check_mirror(
    catalog: Catalog, lake: Lake, ident: tuple[str, str], label: str
) -> Table:
    table = catalog.load_table(ident)
    got = sorted(tuple(r.values()) for r in table.scan().to_arrow().to_pylist())
    expected = lake.rows(f"main.{ident[1]}")
    record(
        f"{label}: rows read through the catalog == DuckLake",
        got == expected,
        f"{len(got)} vs {len(expected)}",
    )
    ids = {f.name: f.field_id for f in table.schema().fields}
    record(
        f"{label}: field ids == DuckLake column ids",
        ids == lake.column_ids(ident[1]),
        str(ids),
    )
    files = {task.file.file_path for task in table.scan().plan_files()}
    live = lake.live_files("main", ident[1])
    record(
        f"{label}: data files referenced in place (zero copy)",
        files == live,
        f"{len(files)} files",
    )
    return table


def run_spike(catalog: Catalog, lake: Lake, settings: DuckLakeIcebergSettings) -> None:
    ident = (NAMESPACE, "orders")
    try:
        catalog.create_namespace(NAMESPACE)
    except Exception as exc:  # already exists
        print("  (namespace)", type(exc).__name__)
    try:  # leftover of a previous spike run (its files may be gone already)
        catalog.drop_table(ident)
        print("  (dropped a leftover table)")
    except Exception as exc:
        print("  (no leftover table)", type(exc).__name__)

    def export(label: str) -> ExportResult:
        result = run_export(settings, "main", "orders")
        print(
            f"  export {label}: exit {int(result.exit_code)} "
            f"v{result.plan.metadata_version} {result.operation}"
        )
        return result

    def commit_next(label: str, previous: ExportResult, current: ExportResult) -> None:
        io = catalog.load_table(ident).io
        old = read_metadata_json(io, previous.plan.metadata_location)
        new = read_metadata_json(io, current.plan.metadata_location)
        requirements, updates = updates_between(
            old, new, catalog.load_table(ident).metadata
        )
        table = catalog.load_table(ident)
        try:
            catalog.commit_table(table, requirements, updates)
            record(
                f"{label}: commit_table accepted",
                True,
                ", ".join(type(u).__name__ for u in updates),
            )
        except Exception as exc:
            record(
                f"{label}: commit_table accepted",
                False,
                f"{type(exc).__name__}: {exc}"[:300],
            )
            raise
        check_mirror(catalog, lake, ident, label)

    # 1. create + insert -> register_table of the tool-written v1
    lake.write(
        "CALL lake.set_option('data_inlining_row_limit', 0)",
        "CREATE TABLE lake.main.orders(id BIGINT, customer VARCHAR, amount DECIMAL(12,2), "
        "tags VARCHAR[], addr STRUCT(city VARCHAR, zip VARCHAR))",
        "INSERT INTO lake.main.orders SELECT i, 'c' || (i % 7), i * 1.25, ['a'], "
        "{'city': 'x', 'zip': 'y'} FROM range(1, 1001) t(i)",
    )
    v1 = export("create")
    if BOOTSTRAP == "create":
        if not bootstrap_by_create(catalog, ident, v1):
            return
    else:
        try:
            catalog.register_table(ident, v1.plan.metadata_location)
            record("create: register_table of the tool-written metadata.json", True)
        except Exception as exc:
            record(
                "create: register_table of the tool-written metadata.json",
                False,
                f"{type(exc).__name__}: {exc}"[:300],
            )
            return
    check_mirror(catalog, lake, ident, "create")

    steps: list[tuple[str, Callable[[], None]]] = [
        (
            "insert",
            lambda: lake.write(
                "INSERT INTO lake.main.orders SELECT i, 'n', 1.0, ['b'], "
                "{'city': 'n', 'zip': 'n'} FROM range(2000, 2300) t(i)"
            ),
        ),
        (
            "delete+update (position deletes)",
            lambda: lake.write(
                "DELETE FROM lake.main.orders WHERE id % 3 = 0",
                "UPDATE lake.main.orders SET customer = 'u' WHERE id % 5 = 0",
            ),
        ),
        (
            "add column (schema evolution)",
            lambda: lake.write(
                "ALTER TABLE lake.main.orders ADD COLUMN note VARCHAR",
                "INSERT INTO lake.main.orders SELECT i, 'e', 2.0, ['c'], "
                "{'city': 'e', 'zip': 'e'}, 'note' FROM range(3000, 3100) t(i)",
            ),
        ),
        (
            "merge",
            lambda: lake.write(
                "MERGE INTO lake.main.orders AS t USING (SELECT i AS id FROM range(1, 30) t(i)) s "
                "ON t.id = s.id WHEN MATCHED THEN UPDATE SET customer = 'm' "
                "WHEN NOT MATCHED THEN INSERT (id, customer) VALUES (s.id, 'm')"
            ),
        ),
    ]
    previous = v1
    for label, mutate in steps:
        mutate()
        current = export(label)
        commit_next(label, previous, current)
        previous = current
    partition_evolution(catalog, lake, settings)
    spec_id_renumbering(catalog, lake, settings)
    external_write_is_rejected(catalog, lake, ident, previous, export)


def partition_evolution(
    catalog: Catalog, lake: Lake, settings: DuckLakeIcebergSettings
) -> None:
    """A separate table: identity partition, then a new spec, then unpartitioned."""
    ident = (NAMESPACE, "events")
    try:
        catalog.drop_table(ident)
    except Exception:
        pass
    lake.write(
        "CREATE TABLE lake.main.events(id BIGINT, region VARCHAR, v VARCHAR)",
        "ALTER TABLE lake.main.events SET PARTITIONED BY (region)",
        "INSERT INTO lake.main.events SELECT i, ['n', 's'][1 + i % 2], 'v' "
        "FROM range(1, 301) t(i)",
    )
    first = run_export(settings, "main", "events")
    catalog.register_table(ident, first.plan.metadata_location)
    check_mirror(catalog, lake, ident, "partitioned create")
    describe_ids(catalog, ident)
    previous = first
    for label, sql in (
        (
            "new partition spec",
            (
                "ALTER TABLE lake.main.events SET PARTITIONED BY (region, bucket(4, id))",
                "INSERT INTO lake.main.events SELECT i, 'w', 'b' FROM range(1000, 1100) t(i)",
            ),
        ),
        (
            "back to unpartitioned",
            (
                "ALTER TABLE lake.main.events RESET PARTITIONED BY",
                "INSERT INTO lake.main.events SELECT i, 'e', 'u' FROM range(2000, 2100) t(i)",
            ),
        ),
    ):
        lake.write(*sql)
        current = run_export(settings, "main", "events")
        io = catalog.load_table(ident).io
        old = read_metadata_json(io, previous.plan.metadata_location)
        new = read_metadata_json(io, current.plan.metadata_location)
        requirements, updates = updates_between(
            old, new, catalog.load_table(ident).metadata
        )
        tool_specs = sorted(sp.spec_id for sp in new.partition_specs)
        try:
            catalog.commit_table(catalog.load_table(ident), requirements, updates)
            record(
                f"{label}: commit_table accepted", True, f"tool spec ids {tool_specs}"
            )
            check_mirror(catalog, lake, ident, label)
        except Exception as exc:
            record(
                f"{label}: commit accepted and table readable",
                False,
                f"tool spec ids {tool_specs}; {type(exc).__name__}: {exc}"[:300],
            )
        describe_ids(catalog, ident)
        previous = current


def spec_id_renumbering(
    catalog: Catalog, lake: Lake, settings: DuckLakeIcebergSettings
) -> None:
    """Finding 3: a table first published unpartitioned, then partitioned.

    The tool names the new spec after DuckLake's ``partition_id`` (catalog-wide,
    so usually > 1); a catalog that numbers specs itself assigns 1, and the
    manifests then reference a spec the catalog does not have.
    """
    ident = (NAMESPACE, "late")
    with contextlib.suppress(Exception):
        catalog.drop_table(ident)
    lake.write(
        "CREATE TABLE lake.main.late(id BIGINT, region VARCHAR)",
        "INSERT INTO lake.main.late SELECT i, 'a' FROM range(1, 101) t(i)",
    )
    first = run_export(settings, "main", "late")
    if BOOTSTRAP == "create":
        bootstrap_by_create(catalog, ident, first)
    else:
        catalog.register_table(ident, first.plan.metadata_location)
    lake.write(
        "ALTER TABLE lake.main.late SET PARTITIONED BY (region)",
        "INSERT INTO lake.main.late SELECT i, 'b' FROM range(200, 300) t(i)",
    )
    second = run_export(settings, "main", "late")
    io = catalog.load_table(ident).io
    old = read_metadata_json(io, first.plan.metadata_location)
    new = read_metadata_json(io, second.plan.metadata_location)
    requirements, updates = updates_between(
        old, new, catalog.load_table(ident).metadata
    )
    catalog.commit_table(catalog.load_table(ident), requirements, updates)
    table = catalog.load_table(ident)
    kept = new.default_spec_id == table.metadata.default_spec_id
    record(
        "spec ids: the catalog keeps the spec id chosen by the tool",
        kept,
        f"tool {new.default_spec_id}, catalog {table.metadata.default_spec_id}",
    )
    try:
        check_mirror(catalog, lake, ident, "late partitioning")
    except Exception as exc:
        record(
            "late partitioning: table readable", False, f"{type(exc).__name__}: {exc}"
        )


def describe_ids(catalog: Catalog, ident: tuple[str, str]) -> None:
    """Spec and schema ids as stored by the catalog vs the manifests' spec ids."""
    table = catalog.load_table(ident)
    specs = {
        s.spec_id: [f.name for f in s.fields] for s in table.metadata.partition_specs
    }
    schemas = [s.schema_id for s in table.metadata.schemas]
    manifests = sorted(
        {m.partition_spec_id for m in table.current_snapshot().manifests(table.io)}
    )
    print(
        f"  catalog specs {specs}, schemas {schemas}; manifests use spec ids {manifests}"
    )


def external_write_is_rejected(
    catalog: Catalog,
    lake: Lake,
    ident: tuple[str, str],
    previous: ExportResult,
    export: Callable[[str], ExportResult],
) -> ExportResult | None:
    # external write through the catalog, then our next commit must be rejected
    table = catalog.load_table(ident)
    try:
        import pyarrow as pa

        table.append(
            pa.Table.from_pylist(
                [{"id": 99999, "customer": "external"}],
                schema=table.schema().as_arrow(),
            )
        )
        print("  external append committed by PyIceberg")
    except Exception as exc:
        record(
            "external write: PyIceberg append",
            False,
            f"{type(exc).__name__}: {exc}"[:300],
        )
        return None
    lake.write("INSERT INTO lake.main.orders (id, customer) VALUES (50000, 'after')")
    current = export("after external write")
    io = catalog.load_table(ident).io
    old = read_metadata_json(io, previous.plan.metadata_location)
    new = read_metadata_json(io, current.plan.metadata_location)
    requirements, updates = updates_between(
        old, new, catalog.load_table(ident).metadata
    )
    try:
        catalog.commit_table(catalog.load_table(ident), requirements, updates)
        record("external write: our commit is rejected", False, "commit accepted")
    except CommitFailedException as exc:
        record(
            "external write: our commit is rejected (CommitFailedException)",
            True,
            str(exc)[:160],
        )
    except Exception as exc:
        record(
            "external write: our commit is rejected",
            False,
            f"{type(exc).__name__}: {exc}"[:300],
        )

    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catalog", required=True, choices=["sql", "rest", "glue"])
    parser.add_argument("--endpoint", default="http://127.0.0.1:9000", help="MinIO/S3")
    parser.add_argument("--bucket", default="spike")
    parser.add_argument("--uri", help="REST catalog URI")
    parser.add_argument("--warehouse", help="REST warehouse / catalog name")
    parser.add_argument("--prop", action="append", default=[], help="extra key=value")
    parser.add_argument(
        "--bootstrap", choices=["register", "create"], default="register"
    )
    parser.add_argument(
        "--metadata-prefix",
        help="iceberg_metadata_prefix (default s3://<bucket>/iceberg/)",
    )
    args = parser.parse_args()

    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        region_name=REGION,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    try:
        s3.create_bucket(Bucket=args.bucket)
    except Exception:
        pass
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=args.bucket):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=args.bucket, Key=obj["Key"])

    global BOOTSTRAP
    BOOTSTRAP = args.bootstrap
    root = Path(tempfile.mkdtemp(prefix="spike-"))
    io_props = {
        "s3.endpoint": args.endpoint,
        "s3.access-key-id": KEY,
        "s3.secret-access-key": SECRET,
        "s3.region": REGION,
        "s3.path-style-access": "true",
    }
    props: dict[str, str] = dict(io_props)
    if args.catalog == "sql":
        props |= {
            "type": "sql",
            "uri": f"sqlite:///{root}/catalog.db",
            "warehouse": f"s3://{args.bucket}/warehouse",
        }
    elif args.catalog == "rest":
        props |= {"type": "rest", "uri": args.uri or "http://localhost:8181"}
        if args.warehouse:
            props["warehouse"] = args.warehouse
    else:
        props |= {"type": "glue", "glue.region": REGION}
    for item in args.prop:
        key, _, value = item.partition("=")
        props[key] = value
    print(
        f"catalog: {args.catalog} {json.dumps({k: v for k, v in props.items() if 'secret' not in k and 'credential' not in k})}"
    )
    catalog = load_catalog("spike", **props)
    lake = Lake(root, args.endpoint, args.bucket)
    settings = build_settings(root, args.endpoint, args.bucket, args.metadata_prefix)
    started = time.time()
    try:
        run_spike(catalog, lake, settings)
    except Exception:
        traceback.print_exc()
        record("spike finished without an unexpected error", False)
    passed = sum(ok for _, ok, _ in RESULTS)
    print(f"\n{passed}/{len(RESULTS)} checks passed in {time.time() - started:.1f}s")
    return 0 if RESULTS and all(ok for _, ok, _ in RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
