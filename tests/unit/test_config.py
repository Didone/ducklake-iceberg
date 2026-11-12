"""Configuration loading: precedence, validation, and edge cases."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from ducklake_iceberg.config import (
    CONFIG_FILE_ENV,
    DuckLakeIcebergSettings,
    load_settings,
    resolve_config_file,
)
from ducklake_iceberg.errors import ConfigError

_BASE: dict[str, Any] = {
    "catalog_dsn": "ducklake:postgres:dbname=lake host=pg.internal port=5432",
    "data_path": "s3://datalake-prod/ducklake/data/",
    "iceberg_metadata_prefix": "s3://datalake-prod/iceberg/",
    "glue_database": "analytics",
    "aws_region": "sa-east-1",
    "aws_profile": "lakeprod",
}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove any DUCKLAKE_ICEBERG_* variable to isolate precedence."""
    import os

    for key in list(os.environ):
        if key.startswith("DUCKLAKE_ICEBERG_"):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv(CONFIG_FILE_ENV, raising=False)


def _write_yaml(
    tmp_path: Path, data: dict[str, Any], name: str = "ducklake-iceberg.yaml"
) -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


# --- YAML only: defaults ---------------------------------------------------
def test_yaml_only_uses_defaults(tmp_path: Path) -> None:
    settings = load_settings(config_file=_write_yaml(tmp_path, _BASE))
    assert settings.catalog_alias == "lake"
    assert settings.metadata_catalog == "lake_meta"
    assert settings.glue_database == "analytics"
    assert settings.aws_profile == "lakeprod"


# --- env beats YAML --------------------------------------------------------
def test_env_overrides_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DUCKLAKE_ICEBERG_AWS_PROFILE", "envprof")
    settings = load_settings(config_file=_write_yaml(tmp_path, _BASE))
    assert settings.aws_profile == "envprof"


# --- flag beats env --------------------------------------------------------
def test_flag_overrides_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DUCKLAKE_ICEBERG_AWS_PROFILE", "envprof")
    settings = load_settings(
        config_file=_write_yaml(tmp_path, _BASE),
        overrides={"aws_profile": "cliprof"},
    )
    assert settings.aws_profile == "cliprof"


# --- a None override is ignored --------------------------------------------
def test_none_override_preserves_yaml(tmp_path: Path) -> None:
    settings = load_settings(
        config_file=_write_yaml(tmp_path, _BASE),
        overrides={"aws_profile": None},
    )
    assert settings.aws_profile == "lakeprod"


# --- metadata prefix inside data_path --------------------------------------
def test_prefix_inside_data_path(tmp_path: Path) -> None:
    data = {
        **_BASE,
        "iceberg_metadata_prefix": "s3://datalake-prod/ducklake/data/meta/",
    }
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=_write_yaml(tmp_path, data))
    assert "must not overlap" in exc.value.message


# --- a sibling with a suffix is accepted (trailing slash) ------------------
def test_neighbor_prefix_accepted(tmp_path: Path) -> None:
    data = {
        **_BASE,
        "data_path": "s3://b/ducklake/data/",
        "iceberg_metadata_prefix": "s3://b/ducklake/data-iceberg/",
    }
    settings = load_settings(config_file=_write_yaml(tmp_path, data))
    assert settings.iceberg_metadata_prefix == "s3://b/ducklake/data-iceberg/"


# --- data_path and prefix are equal ----------------------------------------
def test_equal_paths_rejected(tmp_path: Path) -> None:
    data = {**_BASE, "data_path": "s3://b/x", "iceberg_metadata_prefix": "s3://b/x"}
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=_write_yaml(tmp_path, data))
    assert "must not overlap" in exc.value.message


# --- inverse direction (prefix contains data_path) -------------------------
def test_inverse_containment_rejected(tmp_path: Path) -> None:
    data = {
        **_BASE,
        "data_path": "s3://b/iceberg/sub/",
        "iceberg_metadata_prefix": "s3://b/iceberg/",
    }
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=_write_yaml(tmp_path, data))
    assert "must not overlap" in exc.value.message


# --- DSN with credentials --------------------------------------------------
def test_dsn_with_credentials_rejected(tmp_path: Path) -> None:
    data = {**_BASE, "catalog_dsn": "ducklake:postgres:dbname=lake user=ro password=x"}
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=_write_yaml(tmp_path, data))
    assert "CREATE SECRET" in exc.value.message


@pytest.mark.parametrize("token", ["password=x", "pwd=x", "user=ro"])
def test_dsn_rejects_each_credential_token(tmp_path: Path, token: str) -> None:
    data = {**_BASE, "catalog_dsn": f"ducklake:postgres:dbname=lake {token} host=pg"}
    with pytest.raises(ConfigError):
        load_settings(config_file=_write_yaml(tmp_path, data))


def test_dsn_must_start_with_ducklake(tmp_path: Path) -> None:
    data = {**_BASE, "catalog_dsn": "postgres:dbname=lake host=pg"}
    with pytest.raises(ConfigError):
        load_settings(config_file=_write_yaml(tmp_path, data))


# --- invalid identifier and alias == metadata_catalog ----------------------
def test_metadata_catalog_not_identifier(tmp_path: Path) -> None:
    data = {**_BASE, "metadata_catalog": "lake meta"}
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=_write_yaml(tmp_path, data))
    assert "SQL identifier" in exc.value.message


def test_alias_equals_metadata_catalog(tmp_path: Path) -> None:
    data = {**_BASE, "catalog_alias": "lake_meta"}
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=_write_yaml(tmp_path, data))
    assert "must be different" in exc.value.message


# --- unknown key -----------------------------------------------------------
def test_unknown_key_rejected(tmp_path: Path) -> None:
    data = {**_BASE, "metadata_prefix": "s3://b/x/"}
    path = _write_yaml(tmp_path, data)
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=path)
    assert "metadata_prefix" in exc.value.message
    assert str(path) in exc.value.message


# --- environment only, no file ---------------------------------------------
def test_env_only_no_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import ducklake_iceberg.config as cfg

    for key, value in _BASE.items():
        monkeypatch.setenv(f"DUCKLAKE_ICEBERG_{key.upper()}", value)
    # cwd without ducklake-iceberg.yaml and a restricted search: no real config
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cfg,
        "CONFIG_SEARCH_PATH",
        (Path("ducklake-iceberg.yaml"), Path("ducklake-iceberg.yml")),
    )
    settings = load_settings(config_file=None)
    assert settings.glue_database == "analytics"
    assert settings.redacted()["config_file"] is None


# --- explicit --config that does not exist --------------------------------
def test_explicit_missing_config_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=tmp_path / "does-not-exist.yaml")
    assert "not found" in exc.value.message


# --- redacted() masks the host and preserves the rest ----------------------
def test_redacted_hides_host(tmp_path: Path) -> None:
    path = _write_yaml(tmp_path, _BASE)
    settings = load_settings(config_file=path)
    red = settings.redacted()
    assert "host=***" in red["catalog_dsn"]
    assert "pg.internal" not in red["catalog_dsn"]
    assert red["data_path"] == _BASE["data_path"]
    assert red["glue_database"] == _BASE["glue_database"]
    assert red["config_file"] == str(path)


# --- field and resolution edge cases ---------------------------------------
def test_missing_required_field(tmp_path: Path) -> None:
    data = {k: v for k, v in _BASE.items() if k != "catalog_dsn"}
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=_write_yaml(tmp_path, data))
    assert exc.value.message == "incomplete configuration: catalog_dsn"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("glue_database", "Analytics-01"),
        ("aws_region", "SA_EAST_1"),
        ("data_path", "relative/without/scheme/"),
        ("data_path", "s3://bucket"),
        ("data_path", "s3:///no-bucket/"),
    ],
)
def test_field_format_rejected(tmp_path: Path, field: str, value: str) -> None:
    data = {**_BASE, field: value}
    with pytest.raises(ConfigError):
        load_settings(config_file=_write_yaml(tmp_path, data))


def test_local_absolute_path_accepted(tmp_path: Path) -> None:
    data = {
        **_BASE,
        "data_path": "/srv/ducklake/data",
        "iceberg_metadata_prefix": "/srv/iceberg",
    }
    settings = load_settings(config_file=_write_yaml(tmp_path, data))
    assert settings.data_path == "/srv/ducklake/data/"
    assert settings.iceberg_metadata_prefix == "/srv/iceberg/"


def test_malformed_yaml(tmp_path: Path) -> None:
    path = tmp_path / "ducklake-iceberg.yaml"
    path.write_text("catalog_dsn: [unclosed\n", encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=path)
    assert "invalid YAML" in exc.value.message


def test_yaml_not_a_mapping(tmp_path: Path) -> None:
    path = tmp_path / "ducklake-iceberg.yaml"
    path.write_text("- one\n- two\n", encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        load_settings(config_file=path)
    assert "must be a mapping" in exc.value.message


def test_resolve_config_file_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_yaml(tmp_path, _BASE)
    monkeypatch.setenv(CONFIG_FILE_ENV, str(path))
    assert resolve_config_file(None) == path


def test_resolve_config_file_env_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(CONFIG_FILE_ENV, str(tmp_path / "missing.yaml"))
    with pytest.raises(ConfigError):
        resolve_config_file(None)


def test_resolve_config_file_search_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ducklake_iceberg.config as cfg

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cfg,
        "CONFIG_SEARCH_PATH",
        (Path("ducklake-iceberg.yaml"), Path("ducklake-iceberg.yml")),
    )
    found = _write_yaml(tmp_path, _BASE, name="ducklake-iceberg.yaml")
    assert resolve_config_file(None) == Path("ducklake-iceberg.yaml")
    assert found.exists()


def test_resolve_config_file_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ducklake_iceberg.config as cfg

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cfg,
        "CONFIG_SEARCH_PATH",
        (Path("ducklake-iceberg.yaml"), Path("ducklake-iceberg.yml")),
    )
    assert resolve_config_file(None) is None


def test_env_prefix_and_fields_declared() -> None:
    assert DuckLakeIcebergSettings.model_config["env_prefix"] == "DUCKLAKE_ICEBERG_"
    expected = {
        "catalog_dsn",
        "catalog_alias",
        "metadata_catalog",
        "data_path",
        "iceberg_metadata_prefix",
        "glue_database",
        "aws_region",
        "aws_profile",
        "log_level",
        "stats_source",
        "max_footer_workers",
        "max_io_workers",
        "max_total_workers",
        "duckdb_threads",
        "duckdb_memory_limit",
        "pyiceberg_max_workers",
        "duckdb_extension_directory",
        "s3_endpoint",
        "snapshot_retention_hours",
    }
    assert set(DuckLakeIcebergSettings.model_fields) == expected


def test_snapshot_retention_hours(monkeypatch: pytest.MonkeyPatch) -> None:
    assert DuckLakeIcebergSettings(**_BASE).snapshot_retention_hours is None
    monkeypatch.setenv("DUCKLAKE_ICEBERG_SNAPSHOT_RETENTION_HOURS", "24")
    assert DuckLakeIcebergSettings(**_BASE).snapshot_retention_hours == 24
    with pytest.raises(ValueError, match="greater than or equal to 1"):
        DuckLakeIcebergSettings(**_BASE, snapshot_retention_hours=0)


def test_pyiceberg_max_workers_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    """No flag; ``PYICEBERG_MAX_WORKERS`` > YAML > default 8."""
    monkeypatch.delenv("PYICEBERG_MAX_WORKERS", raising=False)
    monkeypatch.delenv("DUCKLAKE_ICEBERG_PYICEBERG_MAX_WORKERS", raising=False)
    assert DuckLakeIcebergSettings(**_BASE).pyiceberg_max_workers == 8
    monkeypatch.setenv("PYICEBERG_MAX_WORKERS", "3")
    assert DuckLakeIcebergSettings(**_BASE).pyiceberg_max_workers == 3
    monkeypatch.delenv("PYICEBERG_MAX_WORKERS")
    monkeypatch.setenv("DUCKLAKE_ICEBERG_PYICEBERG_MAX_WORKERS", "5")
    assert DuckLakeIcebergSettings(**_BASE).pyiceberg_max_workers == 5


# --- custom S3 endpoint ----------------------------------------------------
def test_s3_endpoint_defaults_to_aws(tmp_path: Path) -> None:
    assert load_settings(config_file=_write_yaml(tmp_path, _BASE)).s3_endpoint is None


def test_s3_endpoint_is_normalized(tmp_path: Path) -> None:
    data = {**_BASE, "s3_endpoint": "http://minio:9000/"}
    settings = load_settings(config_file=_write_yaml(tmp_path, data))
    assert settings.s3_endpoint == "http://minio:9000"


@pytest.mark.parametrize("value", ["minio:9000", "ftp://minio", "http://", ""])
def test_s3_endpoint_rejects_invalid_values(tmp_path: Path, value: str) -> None:
    data = {**_BASE, "s3_endpoint": value}
    with pytest.raises(ConfigError, match="s3_endpoint"):
        load_settings(config_file=_write_yaml(tmp_path, data))


@pytest.mark.parametrize(
    "dsn",
    [
        "ducklake:postgres:dbname=lake password = 'fake secret with spaces'",
        "ducklake:postgres:dbname=lake user\t= someone",
        "ducklake:postgres:dbname=lake pwd = fake",
        "ducklake:postgres:postgresql://someone:fake@localhost/lake",
    ],
)
def test_dsn_credentials_cannot_bypass_validation_with_whitespace(
    tmp_path: Path,
    dsn: str,
) -> None:
    path = _write_yaml(tmp_path, _BASE | {"catalog_dsn": dsn})
    with pytest.raises(ConfigError, match="credentials in the DSN"):
        load_settings(config_file=path)
