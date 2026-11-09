"""Partition field names follow the Iceberg convention, end to end.

Real DuckLake -> ``iceduck sync`` -> moto Glue, metadata under a local prefix
(same setup as ``test_validate``). Checks the name published in the
``metadata.json`` and that the names of already published specs are preserved.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from moto import mock_aws
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.table import Table
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


def _invoke(config: Path, *args: str) -> tuple[int, dict[str, Any]]:
    result = runner.invoke(app, [*args, "--config", str(config), "--json"])
    return result.exit_code, json.loads(result.stdout)


def _sync(config: Path) -> None:
    code, doc = _invoke(config, "sync", "--table", "main.t")
    assert code == 0, doc["problems"]


def _table() -> Table:
    return GlueCatalog("t", **{"glue.region": REGION}).load_table(("analytics", "t"))


def _names(spec_id: int | None = None) -> list[str]:
    table = _table()
    spec = table.spec() if spec_id is None else table.specs()[spec_id]
    return [field.name for field in spec.fields]


def _validate_ok(config: Path) -> None:
    code, doc = _invoke(config, "validate", "--table", "main.t", "--only", "data")
    assert code == 0, (doc["problems"], doc["results"])


def test_epoch_day_identity_and_bucket_use_standard_names(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, created_at DATE, uf VARCHAR)")
    catalog.write(
        "ALTER TABLE lake.main.t SET PARTITIONED BY "
        "(epoch_day(created_at), uf, bucket(16, id))"
    )
    catalog.write(
        "INSERT INTO lake.main.t SELECT i, DATE '2024-01-01' + (i % 3)::INTEGER, "
        "CASE WHEN i % 2 = 0 THEN 'SP' ELSE 'RJ' END FROM range(40) r(i)"
    )
    _sync(config)

    assert _names() == ["created_at_day", "uf", "id_bucket_16"]
    _validate_ok(config)


def test_published_spec_keeps_its_name_in_incremental_export(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """A spec published by an older version (``c<id>_day``) is not renamed."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, created_at DATE)")
    catalog.write("ALTER TABLE lake.main.t SET PARTITIONED BY (epoch_day(created_at))")
    catalog.write("INSERT INTO lake.main.t VALUES (1, DATE '2024-01-01')")
    _sync(config)
    legacy = f"c{_table().spec().fields[0].source_id}_day"
    location = Path(_table().metadata_location.removeprefix("file://"))
    metadata = json.loads(location.read_text(encoding="utf-8"))
    for spec in metadata["partition-specs"]:
        for field in spec["fields"]:
            field["name"] = legacy
    location.write_text(json.dumps(metadata), encoding="utf-8")

    catalog.write("INSERT INTO lake.main.t VALUES (2, DATE '2024-01-02')")
    _sync(config)

    assert _names() == [legacy]
    _validate_ok(config)


def test_new_spec_gets_standard_name_and_old_spec_is_kept(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    """A rename after creation does not change the name; a new spec uses the
    standard name."""
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, created_at DATE)")
    catalog.write("ALTER TABLE lake.main.t SET PARTITIONED BY (epoch_day(created_at))")
    catalog.write("INSERT INTO lake.main.t VALUES (1, DATE '2024-01-01')")
    _sync(config)
    old_spec_id = _table().spec().spec_id

    catalog.write("ALTER TABLE lake.main.t RENAME COLUMN created_at TO created_on")
    catalog.write("INSERT INTO lake.main.t VALUES (2, DATE '2024-01-02')")
    _sync(config)
    assert _names() == ["created_at_day"]

    catalog.write("ALTER TABLE lake.main.t SET PARTITIONED BY (bucket(4, id))")
    catalog.write("INSERT INTO lake.main.t VALUES (3, DATE '2024-01-03')")
    _sync(config)

    assert _names() == ["id_bucket_4"]
    assert _names(old_spec_id) == ["created_at_day"]
    _validate_ok(config)


def test_first_export_after_rename_uses_name_at_spec_creation(
    catalog: CatalogHarness, config: Path, glue_env: None
) -> None:
    catalog.write("CREATE TABLE lake.main.t (id INTEGER, created_at DATE)")
    catalog.write("ALTER TABLE lake.main.t SET PARTITIONED BY (epoch_day(created_at))")
    catalog.write("INSERT INTO lake.main.t VALUES (1, DATE '2024-01-01')")
    catalog.write("ALTER TABLE lake.main.t RENAME COLUMN created_at TO created_on")
    _sync(config)

    assert _names() == ["created_at_day"]
    _validate_ok(config)
