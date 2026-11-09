"""``ducklake-iceberg validate`` end to end.

Real DuckLake -> ``ducklake-iceberg sync`` -> moto Glue, with Iceberg metadata under a
local prefix. The silent-failure scenarios deliberately corrupt the
manifest/``metadata.json`` after a valid export, which is the only way to
exercise them.
"""

from __future__ import annotations

import json
import os
import struct
from collections.abc import Callable, Iterator
from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import fastavro
from jsonschema import Draft202012Validator
from moto import mock_aws
from pyiceberg.catalog.glue import GlueCatalog
from typer.testing import CliRunner

from ducklake_iceberg.cli import app

from .conftest import CatalogHarness

pytestmark = [pytest.mark.integration, pytest.mark.aws]

runner = CliRunner()
REGION = "us-east-1"
SCHEMA_PATH = (
    Path(__file__).parents[2] / "src/ducklake_iceberg/schemas/envelope-v1.json"
)


@pytest.fixture
def glue_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("DUCKLAKE_ICEBERG_STATE_DIR", str(tmp_path / "state"))
    with mock_aws(), mock.patch("ducklake_iceberg.glue._validate_metadata_location"):
        yield


@pytest.fixture
def config(catalog: CatalogHarness, tmp_path: Path) -> Path:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    path = tmp_path / "ducklake-iceberg.yaml"
    path.write_text(
        f"catalog_dsn: '{catalog.dsn}'\n"
        f"data_path: '{catalog.data_path}'\n"
        f"iceberg_metadata_prefix: '{tmp_path / 'iceberg'}'\n"
        f"glue_database: analytics\n"
        f"aws_region: {REGION}\n"
    )
    return path


def _invoke(config: Path, *args: str) -> tuple[int, dict[str, Any]]:
    result = runner.invoke(app, [*args, "--config", str(config), "--json"])
    return result.exit_code, json.loads(result.stdout)


def _sync(config: Path) -> None:
    code, doc = _invoke(config, "sync", "--table", "main.t")
    assert code == 0, doc["problems"]


def _validate(config: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    return _invoke(config, "validate", "--table", "main.t", "--only", "data", *extra)


def _checks(doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {check["id"]: check for check in doc["results"][0]["checks"]}


def _local(uri: str) -> Path:
    return Path(uri.removeprefix("file://"))


def _metadata_path() -> Path:
    table = GlueCatalog("t", **{"glue.region": REGION}).load_table(("analytics", "t"))
    return _local(table.metadata_location)


def _manifests() -> list[Path]:
    metadata = json.loads(_metadata_path().read_text(encoding="utf-8"))
    current = metadata["current-snapshot-id"]
    snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == current)
    with _local(snapshot["manifest-list"]).open("rb") as handle:
        entries = cast(list[dict[str, Any]], list(fastavro.reader(handle)))
    return [
        _local(str(entry["manifest_path"]))
        for entry in entries
        if entry.get("content", 0) == 0
    ]


def _rewrite_manifest(
    path: Path, edit: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
) -> None:
    """Rewrite a manifest preserving the Avro schema, codec and metadata."""
    with path.open("rb") as handle:
        reader = fastavro.reader(handle)
        schema = reader.writer_schema
        codec = reader.codec
        metadata = {
            key: value
            for key, value in reader.metadata.items()
            if not key.startswith("avro.")
        }
        records = [cast(dict[str, Any], record) for record in reader]
    edited = edit(records)
    with path.open("wb") as handle:
        fastavro.writer(handle, schema, edited, codec=codec, metadata=metadata)


def _entry_count(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for _ in fastavro.reader(handle))


def _field_id(name: str) -> int:
    metadata = json.loads(_metadata_path().read_text(encoding="utf-8"))
    schema = next(
        s
        for s in metadata["schemas"]
        if s["schema-id"] == metadata["current-schema-id"]
    )
    return int(next(f["id"] for f in schema["fields"] if f["name"] == name))


def _edit_current_schema(edit: Callable[[dict[str, Any]], None]) -> None:
    path = _metadata_path()
    metadata = json.loads(path.read_text(encoding="utf-8"))
    schema = next(
        s
        for s in metadata["schemas"]
        if s["schema-id"] == metadata["current-schema-id"]
    )
    edit(schema)
    metadata["last-column-id"] = max(
        int(metadata["last-column-id"]), *(int(f["id"]) for f in schema["fields"])
    )
    path.write_text(json.dumps(metadata), encoding="utf-8")


# -- happy path, determinism and contract ---------------------------------------


def test_identical_table_passes_all_five_checks(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """All five checks pass (none is skipped)."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, d DATE, v DOUBLE, s VARCHAR)")
    catalog.write("ALTER TABLE lake.main.t SET PARTITIONED BY (d)")
    catalog.write(
        "INSERT INTO lake.main.t SELECT i, DATE '2024-01-01' + (i % 4)::INTEGER, "
        "i * 0.1, 'k' || (i % 7) FROM range(200) r(i)"
    )
    _sync(config)

    code, doc = _validate(config, "--deep")

    assert code == 0, doc["problems"]
    checks = _checks(doc)
    assert {check["status"] for check in checks.values()} == {"pass"}
    assert doc["results"][0]["message"].startswith("validate: 5/5 checks ok")
    bounds = checks["bounds_predicate"]["details"]["predicates"]
    assert {p["kind"] for p in bounds} == {"rare", "range", "absent"}
    partition = checks["partition_predicate"]["details"]["predicates"]
    assert {p["kind"] for p in partition} == {"frequent", "rare", "absent"}
    assert all(p["iceberg_rows"] == 0 for p in partition if p["kind"] == "absent")
    assert checks["row_sample"]["details"]["compared_rows"] > 0


def test_repeated_runs_choose_same_values_and_sample(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Same predicates and same sample across consecutive runs."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT i, i % 13 FROM range(5000) r(i)")
    _sync(config)

    runs = [_validate(config, "--sample-rows", "50")[1] for _ in range(2)]

    first, second = (_checks(doc) for doc in runs)
    for check_id in ("partition_predicate", "bounds_predicate"):
        assert first[check_id]["details"] == second[check_id]["details"]
    sample_a = first["row_sample"]["details"]
    sample_b = second["row_sample"]["details"]
    assert sample_a["sample_digest"] == sample_b["sample_digest"]
    assert sample_a["compared_rows"] == sample_b["compared_rows"] > 0


def test_json_envelope_matches_schema(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """The validate ``--json`` output validates against the envelope schema."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 10), (2, 20)")
    _sync(config)

    code, doc = _invoke(config, "validate", "--table", "main.t")

    assert code == 0, doc["problems"]
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(doc)
    assert doc["command"] == "validate"
    result = doc["results"][0]
    for key in (
        "table",
        "ducklake_snapshot_id",
        "iceberg_snapshot_id",
        "tolerances_applied",
        "checks",
    ):
        assert key in result
    for check in result["checks"]:
        assert {"id", "status", "duration_ms", "details", "divergences"} <= set(check)


def test_rich_output_prints_summary(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 10), (2, 20)")
    _sync(config)

    result = runner.invoke(
        app,
        ["validate", "--table", "main.t", "--only", "data", "--config", str(config)],
    )

    assert result.exit_code == 0, result.stdout
    assert "row_sample" in result.stdout
    assert "checks ok" in result.stdout


def test_concurrent_ducklake_write_does_not_diverge(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """``AT (VERSION => S_pub)``: a write after the sync is not a divergence."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 10), (2, 20)")
    _sync(config)
    catalog.write("INSERT INTO lake.main.t VALUES (3, 30)")
    catalog.write("DELETE FROM lake.main.t WHERE id = 1")

    code, doc = _validate(config)

    assert code == 0, doc["problems"]
    assert _checks(doc)["row_count"]["details"]["ducklake_rows"] == 2


# -- silent corruptions ----------------------------------------------------------


def test_data_file_removed_from_manifest_diverges(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Row count (a) diverges with ``--deep``."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 10), (2, 20)")
    catalog.write("INSERT INTO lake.main.t VALUES (3, 30)")
    _sync(config)
    manifest = next(path for path in _manifests() if _entry_count(path) > 1)
    _rewrite_manifest(manifest, lambda records: records[1:])

    code, doc = _validate(config, "--deep")

    assert code == 3
    row_count = _checks(doc)["row_count"]
    assert row_count["status"] == "divergent"
    assert row_count["details"]["iceberg_deep_rows"] < 3
    assert row_count["details"]["ducklake_rows"] == 3


def test_wrong_partition_value_diverges(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Wrongly written partition -> (c) with ``iceberg_rows = 0``."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, d DATE)")
    catalog.write("ALTER TABLE lake.main.t SET PARTITIONED BY (epoch_day(d))")
    catalog.write(
        "INSERT INTO lake.main.t SELECT i, DATE '2024-01-01' + "
        "(i % 3)::INTEGER FROM range(30) r(i)"
    )
    _sync(config)

    def shift(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for record in records:
            partition = record["data_file"]["partition"]
            for key, value in partition.items():
                # `day` comes out of Avro as `date` (logicalType); identity as int.
                if isinstance(value, date):
                    partition[key] = value + timedelta(days=1000)
                elif isinstance(value, int):
                    partition[key] = value + 1000
        return records

    for manifest in _manifests():
        _rewrite_manifest(manifest, shift)

    code, doc = _validate(config)

    assert code == 3
    check = _checks(doc)["partition_predicate"]
    assert check["status"] == "divergent"
    frequent = next(
        p for p in check["details"]["predicates"] if p["kind"] == "frequent"
    )
    assert frequent["ducklake_rows"] > 0
    assert frequent["iceberg_rows"] == 0
    assert "partition_predicate:" in check["message"]


def test_inflated_lower_bound_diverges(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Lower bound above the real value -> (d) with ``planned = 0``."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT i, i FROM range(100) r(i)")
    _sync(config)
    field_id = _field_id("id")

    def inflate(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for record in records:
            for bound in record["data_file"]["lower_bounds"] or []:
                if bound["key"] == field_id:
                    bound["value"] = struct.pack("<i", 1_000_000)
        return records

    for manifest in _manifests():
        _rewrite_manifest(manifest, inflate)

    code, doc = _validate(config)

    assert code == 3
    check = _checks(doc)["bounds_predicate"]
    assert check["status"] == "divergent"
    rare = next(
        p
        for p in check["details"]["predicates"]
        if p["kind"] == "rare" and p["column"] == "id"
    )
    assert rare["planned_files"] == 0
    assert rare["iceberg_rows"] == 0 < rare["ducklake_rows"]
    assert "planned files" in check["message"]


def test_wrong_field_id_reads_null_column(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Swapped field ID -> (b) with null min/max/sum on the Iceberg side."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 10), (2, 20)")
    _sync(config)

    def renumber(schema: dict[str, Any]) -> None:
        for item in schema["fields"]:
            if item["name"] == "v":
                item["id"] = 900

    _edit_current_schema(renumber)

    code, doc = _validate(config)

    assert code == 3
    aggregates = _checks(doc)["column_aggregates"]
    assert aggregates["status"] == "divergent"
    details = aggregates["details"]["aggregates"]
    for label in ("v.min", "v.max", "v.sum"):
        assert details[label]["iceberg"] is None
        assert details[label]["ducklake"] is not None


def test_column_missing_on_iceberg_side_is_column_divergence(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Column only on the DuckLake side -> ``column`` divergence in (b)."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 10), (2, 20)")
    _sync(config)

    def drop_v(schema: dict[str, Any]) -> None:
        schema["fields"] = [f for f in schema["fields"] if f["name"] != "v"]

    _edit_current_schema(drop_v)

    code, doc = _validate(config)

    assert code == 3
    aggregates = _checks(doc)["column_aggregates"]
    assert {"kind": "column", "column": "v"}.items() <= aggregates["divergences"][
        0
    ].items()
    assert aggregates["details"]["lake_only_columns"] == ["v"]
    # The other checks proceed with the intersection, without error.
    assert _checks(doc)["row_sample"]["status"] == "pass"


# -- tolerances, external write and deletes --------------------------------------


def test_timestamp_precision_ms_is_declared(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """``ms`` passes and is declared in tolerances_applied."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, ts TIMESTAMP)")
    catalog.write(
        "INSERT INTO lake.main.t VALUES (1, TIMESTAMP '2024-01-01 10:00:00.123456'), "
        "(2, TIMESTAMP '2024-01-02 11:00:00.654321')"
    )
    _sync(config)

    code, doc = _validate(config)
    assert code == 0, doc["problems"]
    assert doc["results"][0]["tolerances_applied"] == []

    code, doc = _validate(config, "--timestamp-precision", "ms")
    assert code == 0, doc["problems"]
    assert doc["results"][0]["tolerances_applied"] == ["timestamp_precision=ms"]


def test_float_sum_within_relative_tolerance(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """A float64 sum with a different aggregation order passes."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, x DOUBLE)")
    catalog.write(
        "INSERT INTO lake.main.t SELECT i, 1.0 / (i + 1) FROM range(3000) r(i)"
    )
    catalog.write("INSERT INTO lake.main.t SELECT i, 1e-3 * i FROM range(3000) r(i)")
    _sync(config)

    code, doc = _validate(config)

    assert code == 0, doc["problems"]
    assert _checks(doc)["column_aggregates"]["status"] == "pass"


def test_row_inserted_only_on_iceberg_side_is_row_extra(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Row only in Iceberg (e.g. written via Athena) -> ``row_extra``; exit 3."""
    import pyarrow as pa

    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 10)")
    _sync(config)
    GlueCatalog("t", **{"glue.region": REGION}).load_table(("analytics", "t")).append(
        pa.table(
            {
                "id": pa.array([2], type=pa.int32()),
                "v": pa.array([20], type=pa.int32()),
            }
        )
    )

    code, doc = _validate(config)

    assert code == 3
    result = doc["results"][0]
    assert result["external_write"] is True
    assert result["message"].startswith("external write detected")
    sample = _checks(doc)["row_sample"]
    assert [d["kind"] for d in sample["divergences"]] == ["row_extra"]
    assert sample["divergences"][0]["iceberg_value"] == {"id": 2, "v": 20}


def test_deleted_rows_absent_on_both_sides(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """An exported delete does not reappear on the Iceberg side."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT i, i * 10 FROM range(50) r(i)")
    _sync(config)
    catalog.write("DELETE FROM lake.main.t WHERE id IN (3, 7, 11)")
    catalog.write("UPDATE lake.main.t SET v = -1 WHERE id = 20")
    _sync(config)

    code, doc = _validate(config, "--deep")

    assert code == 0, doc["problems"]
    row_count = _checks(doc)["row_count"]["details"]
    assert row_count["ducklake_rows"] == row_count["iceberg_deep_rows"] == 47
    assert row_count["iceberg_summary_live_records"] == 47
    assert row_count["iceberg_summary_position_deletes"] > 0


# -- edge cases -------------------------------------------------------------------


def test_expired_published_snapshot_exits_3(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Expired published snapshot -> exit 3 with a retention message."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    catalog.write("INSERT INTO lake.main.t VALUES (1, 10)")
    _sync(config)
    catalog.write("INSERT INTO lake.main.t VALUES (2, 20)")
    catalog.write("CALL ducklake_expire_snapshots('lake', older_than => now())")

    code, doc = _validate(config)

    assert code == 3
    problem = doc["problems"][0]
    assert "no longer exists in DuckLake" in problem["message"]
    assert "retention" in (problem["hint"] or "")


def test_empty_table_on_both_sides_exits_4(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """Table empty on both sides -> exit 4."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v INTEGER)")
    _invoke(config, "sync", "--table", "main.t")

    code, doc = _invoke(config, "validate", "--table", "main.t")

    assert code == 4, doc["problems"]
    assert doc["results"][0]["message"] == (
        "validate: table is empty on both sides; nothing to validate"
    )


def test_non_comparable_columns_are_excluded(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """``blob`` and ``struct`` are excluded; the other checks still run."""
    catalog.write(
        "CREATE TABLE lake.main.t (id INTEGER, b BLOB, s STRUCT(a INTEGER), v INTEGER)"
    )
    catalog.write(
        "INSERT INTO lake.main.t VALUES (1, '\\xAA'::BLOB, {'a': 1}, 10), "
        "(2, '\\xBB'::BLOB, {'a': 2}, 20)"
    )
    _sync(config)

    code, doc = _validate(config)

    assert code == 0, doc["problems"]
    result = doc["results"][0]
    assert {"b", "s"} <= set(result["excluded_columns"])
    checks = _checks(doc)
    assert checks["column_aggregates"]["status"] == "pass"
    assert "v.sum" in checks["column_aggregates"]["details"]["aggregates"]
    assert checks["row_sample"]["status"] == "pass"
