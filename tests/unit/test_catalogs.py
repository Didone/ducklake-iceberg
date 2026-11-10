"""Catalog-neutral helpers: identifier, lock component, supported-type gate."""

from __future__ import annotations

from typing import Any

import pytest

from ducklake_iceberg.catalogs import (
    catalog_key,
    catalog_properties,
    ensure_supported,
    table_identifier,
)
from ducklake_iceberg.config import DuckLakeIcebergSettings
from ducklake_iceberg.errors import ConfigError, ExitCode

_BASE: dict[str, Any] = {
    "catalog_dsn": "ducklake:sqlite:/srv/lake.db",
    "data_path": "/srv/data/",
    "iceberg_metadata_prefix": "/srv/iceberg/",
    "aws_region": "us-east-1",
}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    for key in list(os.environ):
        if key.startswith("DUCKLAKE_ICEBERG_"):
            monkeypatch.delenv(key, raising=False)


def _settings(**kwargs: Any) -> DuckLakeIcebergSettings:
    return DuckLakeIcebergSettings(**{**_BASE, **kwargs})


def test_table_identifier_uses_namespace_parts() -> None:
    glue = _settings(glue_database="analytics")
    assert table_identifier(glue, "Orders-2024") == ("analytics", "orders_2024")
    rest = _settings(catalog={"type": "rest", "namespace": "lake.sales"})
    assert table_identifier(rest, "t") == ("lake", "sales", "t")


def test_catalog_key_ignores_label_and_secrets() -> None:
    a = _settings(
        catalog={
            "type": "rest",
            "name": "a",
            "namespace": "n",
            "properties": {"uri": "http://x", "credential": "one"},
        }
    )
    b = _settings(
        catalog={
            "type": "rest",
            "name": "b",
            "namespace": "n",
            "properties": {"uri": "http://x", "credential": "two"},
        }
    )
    assert catalog_key(a) == catalog_key(b)
    assert "one" not in catalog_key(a)


def test_catalog_key_glue_scoped_by_profile() -> None:
    default = _settings(glue_database="db")
    prod = _settings(glue_database="db", aws_profile="prod")
    assert catalog_key(default) != catalog_key(prod)
    assert catalog_key(default).startswith("glue|us-east-1|default")


def test_ensure_supported() -> None:
    ensure_supported(_settings(glue_database="db"))
    with pytest.raises(ConfigError) as exc:
        ensure_supported(_settings(catalog={"type": "rest", "namespace": "n"}))
    assert exc.value.exit_code is ExitCode.ERROR
    assert exc.value.problem_code == "config_invalid"


def test_catalog_properties_user_values_win(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("ducklake_iceberg.aws.s3_credential_properties", lambda *_: {})
    settings = _settings(
        catalog={
            "type": "glue",
            "namespace": "db",
            "properties": {"glue.region": "eu-west-1"},
        },
        s3_endpoint="http://minio:9000",
    )
    props = catalog_properties(settings)
    assert props["type"] == "glue"
    assert props["glue.region"] == "eu-west-1"
    assert props["s3.region"] == "us-east-1"
    assert props["s3.endpoint"] == "http://minio:9000"
