"""Operational runbooks A, B and C exercised offline.

Real DuckLake (SQLite) -> ``iceduck sync`` -> moto Glue, with metadata under a
**local** prefix (PyArrow S3 is not intercepted by moto), as in
``test_status_clean``. Proves the limits the operational runbooks assume; a
rehearsal in staging is still a separate acceptance criterion.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import boto3
import pyarrow as pa
from moto import mock_aws
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.table import StaticTable
from typer.testing import CliRunner

from iceduck.cli import app

from .conftest import CatalogHarness

pytestmark = [pytest.mark.integration, pytest.mark.aws]

runner = CliRunner()
REGION = "us-east-1"


@pytest.fixture
def glue_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("ICEDUCK_STATE_DIR", str(tmp_path / "state"))
    with mock_aws(), mock.patch("iceduck.glue._validate_metadata_location"):
        yield


@pytest.fixture
def config(catalog: CatalogHarness, tmp_path: Path) -> Path:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    path = tmp_path / "iceduck.yaml"
    path.write_text(
        f"catalog_dsn: '{catalog.dsn}'\n"
        f"data_path: '{catalog.data_path}'\n"
        f"iceberg_metadata_prefix: '{tmp_path / 'iceberg'}'\n"
        f"glue_database: analytics\n"
        f"aws_region: {REGION}\n"
    )
    return path


@pytest.fixture
def location(tmp_path: Path) -> Path:
    return tmp_path / "iceberg" / "analytics" / "t"


def _invoke(config: Path, *args: str) -> tuple[int, dict[str, Any]]:
    result = runner.invoke(app, [*args, "--config", str(config), "--json"])
    return result.exit_code, json.loads(result.stdout)


def _sync(config: Path) -> None:
    code, doc = _invoke(config, "sync", "--table", "main.t")
    assert code == 0, doc["problems"]


def _catalog() -> GlueCatalog:
    return GlueCatalog("t", **{"glue.region": REGION})


def _glue_parameters() -> dict[str, str]:
    glue = boto3.client("glue", region_name=REGION)
    params = glue.get_table(DatabaseName="analytics", Name="t")["Table"]["Parameters"]
    return dict(params)


def _runbook_a_update_table(candidate_uri: str) -> None:
    """Runbook A step 4 (UpdateTable preserving the existing table fields)."""
    glue = boto3.client("glue", region_name=REGION)
    table = glue.get_table(DatabaseName="analytics", Name="t")["Table"]
    input_table = {
        key: table[key]
        for key in ("Name", "TableType", "Parameters", "StorageDescriptor")
        if key in table
    }
    input_table["Parameters"] = dict(table["Parameters"])
    input_table["Parameters"]["previous_metadata_location"] = table["Parameters"][
        "metadata_location"
    ]
    input_table["Parameters"]["metadata_location"] = candidate_uri
    glue.update_table(
        DatabaseName="analytics",
        TableInput=input_table,
        SkipArchive=False,
        VersionId=table["VersionId"],
    )


def _two_syncs(catalog: CatalogHarness, config: Path) -> tuple[str, str]:
    """Run two syncs; return (previous metadata, current metadata)."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT * FROM range(5)")
    _sync(config)
    first = _catalog().load_table(("analytics", "t")).metadata_location
    catalog.write("INSERT INTO lake.main.t SELECT * FROM range(5, 8)")
    _sync(config)
    current = _catalog().load_table(("analytics", "t")).metadata_location
    assert first != current
    return first, current


# -- Runbook A ------------------------------------------------------------------


def test_runbook_a_update_table_rolls_back_and_sync_recovers(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """UpdateTable rolls the snapshot back; the next sync moves forward."""
    previous, current = _two_syncs(catalog, config)
    expected_snapshot = StaticTable.from_metadata(previous).metadata.current_snapshot_id

    _runbook_a_update_table(previous)

    table = _catalog().load_table(("analytics", "t"))
    assert table.metadata_location == previous
    assert table.metadata.current_snapshot_id == expected_snapshot
    assert table.scan().to_arrow().num_rows == 5
    assert _glue_parameters()["previous_metadata_location"] == current

    # the mirror is "behind" (not diverged) and validate reconciles against the
    # DuckLake snapshot recorded in the restored metadata
    code, doc = _invoke(config, "status", "--table", "main.t")
    assert code == 3
    assert doc["results"][0]["tables"][0]["status"] == "behind"
    code, doc = _invoke(config, "validate", "--table", "main.t")
    assert code == 0, doc["problems"]

    # once the schedule is re-enabled, sync recomputes the delta and republishes
    _sync(config)
    assert _catalog().load_table(("analytics", "t")).scan().to_arrow().num_rows == 8
    code, doc = _invoke(config, "validate", "--table", "main.t")
    assert code == 0, doc["problems"]


def test_register_metadata_location_is_forward_only(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """``register --metadata-location`` does not roll back: it only moves forward.

    Glue must point to an ancestor of the target; going back to an earlier
    metadata is exit 3, which is why Runbook A uses ``UpdateTable``. Undoing the
    rollback (returning to the newest metadata) passes the CLI validations.
    """
    previous, current = _two_syncs(catalog, config)
    code, doc = _invoke(
        config, "register", "--table", "main.t", "--metadata-location", previous
    )
    assert code == 3
    assert doc["problems"][0]["code"] == "catalog_diverged"
    assert _catalog().load_table(("analytics", "t")).metadata_location == current

    _runbook_a_update_table(previous)
    code, doc = _invoke(
        config, "register", "--table", "main.t", "--metadata-location", current
    )
    assert code == 0, doc["problems"]
    assert _catalog().load_table(("analytics", "t")).metadata_location == current


def test_runbook_a_drop_and_register_loses_parameters(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """``drop_table`` + ``register_table`` works, but loses Parameters.

    ``drop_table`` only removes the Glue entry: the metadata.json is still there.
    """
    previous, current = _two_syncs(catalog, config)
    glue = boto3.client("glue", region_name=REGION)
    table = glue.get_table(DatabaseName="analytics", Name="t")["Table"]
    params = {**table["Parameters"], "classification_owner": "data-team"}
    glue.update_table(
        DatabaseName="analytics",
        TableInput={
            "Name": "t",
            "TableType": table["TableType"],
            "Parameters": params,
            "StorageDescriptor": table["StorageDescriptor"],
        },
    )

    cat = _catalog()
    cat.drop_table(("analytics", "t"))
    assert Path(current).exists()  # drop_table does not delete S3 objects/files
    cat.register_table(("analytics", "t"), previous)

    restored = _glue_parameters()
    assert restored["metadata_location"] == previous
    assert "classification_owner" not in restored  # extra Parameters are lost
    assert "previous_metadata_location" not in restored
    assert cat.load_table(("analytics", "t")).scan().to_arrow().num_rows == 5


def test_register_table_overwrite_is_not_implemented(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """``register_table(..., overwrite=True)`` is not implemented for Glue."""
    previous, _ = _two_syncs(catalog, config)
    with pytest.raises(NotImplementedError):
        _catalog().register_table(("analytics", "t"), previous, overwrite=True)


# -- Runbook B ------------------------------------------------------------------


def test_runbook_b_deleting_reachable_manifest_breaks_validate(
    catalog: CatalogHarness, config: Path, glue_env: None, location: Path
) -> None:
    """Deleting a reachable manifest breaks reading the mirror.

    Shows why the reachable set matters: ``validate`` stops being green and
    ``clean`` refuses to operate on a table with a missing reference.
    """
    _two_syncs(catalog, config)
    table = _catalog().load_table(("analytics", "t"))
    snapshot = table.current_snapshot()
    assert snapshot is not None
    manifest = Path(snapshot.manifests(table.io)[0].manifest_path)
    manifest.unlink()

    code, _ = _invoke(config, "validate", "--table", "main.t")
    assert code != 0
    code, doc = _invoke(config, "clean", "--table", "main.t", "--apply")
    assert code == 1
    assert "do not exist" in doc["problems"][0]["message"]


# -- Runbook C ------------------------------------------------------------------


def test_runbook_c_restore_after_external_write(
    catalog: CatalogHarness, config: Path, glue_env: None, location: Path
) -> None:
    """Write "via Athena" -> sync blocked -> UpdateTable to the last export.

    ``register`` alone does not restore (the external pointer is not an
    ancestor of the iceduck export); the way is Runbook A step 4 pointing to
    the ``metadata_location`` in ``_iceduck_last_export.json``.
    """
    catalog.write("CREATE TABLE lake.main.t (id INTEGER)")
    catalog.write("INSERT INTO lake.main.t SELECT * FROM range(5)")
    _sync(config)
    _catalog().load_table(("analytics", "t")).append(
        pa.table({"id": pa.array([42], pa.int32())})
    )

    code, doc = _invoke(config, "sync", "--table", "main.t")
    assert code == 3, doc["problems"]
    code, doc = _invoke(config, "validate", "--table", "main.t")
    assert code == 3

    pointer = json.loads((location / "_iceduck_last_export.json").read_text())
    code, doc = _invoke(
        config,
        "register",
        "--table",
        "main.t",
        "--metadata-location",
        pointer["metadata_location"],
    )
    assert code == 3  # register does not overwrite an external write

    _runbook_a_update_table(pointer["metadata_location"])
    code, doc = _invoke(config, "validate", "--table", "main.t")
    assert code == 0, doc["problems"]
    assert _catalog().load_table(("analytics", "t")).scan().to_arrow().num_rows == 5

    # the external writer's data file landed under the metadata prefix: clean
    # blocks (exit 2) until it is removed manually after forensics
    external = [p for p in location.rglob("*.parquet") if p.parent.name == "data"]
    assert len(external) == 1
    code, doc = _invoke(config, "clean", "--table", "main.t", "--apply")
    assert code == 2
    assert ".parquet" in doc["problems"][0]["message"]
    external[0].unlink()  # manual fallback, after preserving the evidence

    # the external metadata is still previous_metadata_location (a clean root);
    # it leaves the reachable set on the next sync, and clean collects it
    catalog.write("INSERT INTO lake.main.t VALUES (5)")
    _sync(config)
    old = time.time() - 3 * 86400
    for path in location.rglob("*"):
        if path.is_file():
            os.utime(path, (old, old))
    code, doc = _invoke(config, "clean", "--table", "main.t", "--apply")
    assert code == 0, doc["problems"]
    deleted = {Path(key).name for key in doc["results"][0]["deleted"]}
    assert any(name.endswith(".metadata.json") for name in deleted)
    code, doc = _invoke(config, "validate", "--table", "main.t")
    assert code == 0, doc["problems"]
    assert _catalog().load_table(("analytics", "t")).scan().to_arrow().num_rows == 6
