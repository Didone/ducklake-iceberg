"""A credential injected as ``s3.*`` never leaks.

With ``aws_profile``, the profile credential is passed explicitly to the
PyIceberg FileIO. This test runs ``sync``
through the CLI (``--json``, ``log_level: DEBUG``, with all loggers at DEBUG)
using a fake profile with marked keys, and checks that the access key, secret
and token appear nowhere: not in stdout, stderr, logs, error messages, the
artifacts written under the prefix (``metadata.json``, manifests, delete
files, pointer) nor in Glue (moto).
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import boto3
import duckdb
from moto import mock_aws
from pyiceberg.io import S3_SECRET_ACCESS_KEY
from typer.testing import CliRunner

import ducklake_iceberg.register
from ducklake_iceberg.catalog.connection import DuckLakeConnection, open_catalog
from ducklake_iceberg.cli import app
from ducklake_iceberg.errors import AwsError

from .conftest import CatalogHarness

pytestmark = [pytest.mark.integration, pytest.mark.aws]

runner = CliRunner()
REGION = "us-east-1"
PROFILE = "ducklake_iceberg_leak"
KEY = "AKIALEAKMARKER000001"
SECRET = "SECRET-LEAK-0123456789"
TOKEN = "TOKEN-LEAK-9876543210"
MARKERS = (KEY, SECRET, TOKEN)
#: botocore wire loggers, which print signed headers at DEBUG
_BOTOCORE_WIRE = frozenset({"botocore.endpoint", "botocore.auth"})


@pytest.fixture
def fake_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Profile with marked keys; no credentials in the environment."""
    aws_dir = tmp_path / "aws"
    aws_dir.mkdir()
    (aws_dir / "config").write_text(f"[profile {PROFILE}]\nregion = {REGION}\n")
    (aws_dir / "credentials").write_text(
        f"[{PROFILE}]\n"
        f"aws_access_key_id = {KEY}\n"
        f"aws_secret_access_key = {SECRET}\n"
        f"aws_session_token = {TOKEN}\n"
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", str(aws_dir / "config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(aws_dir / "credentials"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")  # no IMDS
    for name in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def glue_env(fake_profile: None) -> Iterator[None]:
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
        f"aws_profile: {PROFILE}\n"
        f"log_level: DEBUG\n"
    )
    return path


def _assert_clean(where: str, text: str) -> None:
    for marker in MARKERS:
        assert marker not in text, f"credential {marker[:8]}... leaked in {where}"


def test_profile_credentials_never_leak(
    catalog: CatalogHarness,
    config: Path,
    glue_env: None,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
) -> None:
    botocore_level = logging.getLogger("botocore").level
    caplog.set_level(logging.DEBUG)  # pyiceberg, botocore, fsspec...
    injected: list[dict[str, str]] = []
    from pyiceberg.io import load_file_io as real_load

    def spy(properties: Any = None, location: str | None = None) -> Any:
        injected.append(dict(properties or {}))
        return real_load(properties=properties, location=location)

    outputs: list[tuple[str, str]] = []

    def cli(*args: str) -> int:
        result = runner.invoke(app, [*args, "--config", str(config), "--json"])
        outputs.append((f"stdout of {args[0]}", result.stdout))
        outputs.append((f"stderr of {args[0]}", result.stderr))
        if result.exception is not None and not isinstance(
            result.exception, SystemExit
        ):
            outputs.append((f"exception of {args[0]}", repr(result.exception)))
        return result.exit_code

    catalog.write("CREATE TABLE lake.main.t (id INTEGER, v VARCHAR)")
    catalog.write("INSERT INTO lake.main.t SELECT i, 'v' || i FROM range(20) r(i)")
    with mock.patch.object(ducklake_iceberg.register, "load_file_io", spy):
        assert cli("sync", "--table", "main.t") == 0
        catalog.write("DELETE FROM lake.main.t WHERE id < 5")
        catalog.write("INSERT INTO lake.main.t VALUES (100, 'x')")
        assert cli("sync", "--table", "main.t") == 0
        assert cli("sync", "--table", "main.t") == 4  # already published
        assert cli("sync", "--table", "main.t", "--dry-run") == 4

    # without the credential in the FileIO the test would prove nothing
    assert any(p.get(S3_SECRET_ACCESS_KEY) == SECRET for p in injected)

    for where, text in outputs:
        _assert_clean(where, text)
    # The botocore wire log (``botocore.endpoint``/``botocore.auth``, DEBUG)
    # prints the signed headers (``Credential=<access key>``,
    # ``X-Amz-Security-Token``) of the Glue calls: that comes from the AWS SDK
    # and only appears if the caller enables DEBUG on botocore - the CLI never
    # does (checked below). The secret does not appear even there.
    for record in caplog.records:
        message = record.getMessage()
        assert SECRET not in message, f"secret leaked in {record.name}"
        if record.name not in _BOTOCORE_WIRE:
            _assert_clean(f"log {record.name}", message)
    assert logging.getLogger("botocore").level == botocore_level
    captured = capfd.readouterr()
    _assert_clean("process stdout", captured.out)
    _assert_clean("process stderr", captured.err)

    written = [p for p in (tmp_path / "iceberg").rglob("*") if p.is_file()]
    assert any(p.name.endswith(".metadata.json") for p in written)
    for path in written:
        _assert_clean(str(path), path.read_bytes().decode("latin-1"))

    glue = boto3.Session(profile_name=PROFILE, region_name=REGION).client("glue")
    tables = glue.get_tables(DatabaseName="analytics")["TableList"]
    assert tables
    _assert_clean("Glue", json.dumps(tables, default=str))


def test_cli_traceback_never_shows_locals() -> None:
    """An unexpected error does not print locals (``properties`` with ``s3.*``)."""
    assert app.pretty_exceptions_show_locals is False


# -- DuckDB secret with the profile credential -------------------------------


def _s3_settings(catalog: CatalogHarness, profile: str | None = PROFILE) -> Any:
    return catalog.settings(
        data_path="s3://bucket/ducklake/data/",
        iceberg_metadata_prefix="s3://bucket/iceberg/",
        aws_region=REGION,
        aws_profile=profile,
    )


@pytest.mark.usefixtures("fake_profile")
@pytest.mark.parametrize("via", ["aws_profile", "AWS_PROFILE"])
def test_duckdb_secret_uses_profile_credentials(
    catalog: CatalogHarness,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    via: str,
) -> None:
    """The secret does not depend on the AWS SDK C++ SSO provider: the
    credential comes from boto3, via ``aws_profile`` or ``AWS_PROFILE``."""
    caplog.set_level(logging.DEBUG)
    if via == "AWS_PROFILE":
        monkeypatch.setenv("AWS_PROFILE", PROFILE)
    settings = _s3_settings(catalog, PROFILE if via == "aws_profile" else None)
    with open_catalog(settings) as con:
        provider, key_id = con.raw.execute(
            "SELECT provider, regexp_extract(secret_string, 'key_id=([^;]*)', 1) "
            "FROM duckdb_secrets() WHERE name = 'ducklake_iceberg_s3'"
        ).fetchone() or (None, None)
        selected = con.raw.execute(
            "SELECT name FROM which_secret('s3://bucket/ducklake/data/x.parquet', 's3')"
        ).fetchone()
        environment = repr(con.environment())
    assert provider == "config"
    assert key_id == KEY  # the profile credential, not the C++ chain
    assert selected == ("ducklake_iceberg_s3",)
    _assert_clean("environment()", environment)
    _assert_clean("logs", caplog.text)


class _FailingConnection:
    """Connection that echoes the SQL in the error, like DuckDB's parser errors."""

    def execute(self, sql: str, *_args: object) -> None:
        raise duckdb.ParserException(f"Parser Error: syntax error at: {sql}")


@pytest.mark.usefixtures("fake_profile")
def test_duckdb_secret_failure_never_leaks(
    catalog: CatalogHarness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    con = DuckLakeConnection(_s3_settings(catalog))
    con._con = _FailingConnection()  # type: ignore[assignment]
    with pytest.raises(AwsError) as exc:
        con._create_s3_secret()
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True
    _assert_clean("message", f"{exc.value.message} {exc.value.hint}")
    _assert_clean("exception repr", repr(exc.value))
    _assert_clean("logs", caplog.text)
