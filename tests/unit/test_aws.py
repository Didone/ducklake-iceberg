"""PyIceberg S3 I/O uses the credentials of ``aws_profile``.

``PyArrowFileIO`` ignores ``client.profile-name``; without explicit credentials,
export/register would read and write with the default chain
(``AWS_PROFILE``/``default``). The tests use a fake ``~/.aws`` with static keys
and no network access.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pyiceberg.io import S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN

from iceduck.aws import s3_credential_properties
from iceduck.config import IceduckSettings
from iceduck.errors import AwsError
from iceduck.glue import GlueTarget, _catalog_file_io, build_catalog
from iceduck.register import build_file_io

_KEY = "AKIAICEDUCKTESTKEY01"
_SECRET = "test-secret"
_TOKEN = "test-token"


@pytest.fixture
def fake_aws(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Profile ``iceduck_test`` with static keys; nothing in the environment."""
    config = tmp_path / "config"
    config.write_text("[profile iceduck_test]\nregion = us-east-2\n")
    credentials = tmp_path / "credentials"
    credentials.write_text(
        "[iceduck_test]\n"
        f"aws_access_key_id = {_KEY}\n"
        f"aws_secret_access_key = {_SECRET}\n"
        f"aws_session_token = {_TOKEN}\n"
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
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


def _settings(profile: str | None) -> IceduckSettings:
    return IceduckSettings(
        catalog_dsn="ducklake:sqlite:/tmp/iceduck-test.sqlite",
        data_path="s3://bucket/ducklake/data/",
        iceberg_metadata_prefix="s3://bucket/iceberg/",
        glue_database="analytics",
        aws_region="us-east-2",
        aws_profile=profile,
    )


def _assert_profile_credentials(props: dict[str, str]) -> None:
    assert props.get(S3_ACCESS_KEY_ID) == _KEY
    assert props.get(S3_SECRET_ACCESS_KEY) == _SECRET
    assert props.get(S3_SESSION_TOKEN) == _TOKEN


@pytest.mark.usefixtures("fake_aws")
def test_build_file_io_uses_profile_credentials() -> None:
    io = build_file_io(_settings("iceduck_test"))
    _assert_profile_credentials(dict(io.properties))


@pytest.mark.usefixtures("fake_aws")
def test_glue_catalog_file_io_uses_profile_credentials() -> None:
    target = GlueTarget(
        database="analytics",
        table="pedidos",
        region="us-east-2",
        profile="iceduck_test",
        catalog_id=None,
    )
    catalog = build_catalog(target)
    io = _catalog_file_io(catalog, "s3://bucket/iceberg/analytics/pedidos/v1.json")
    _assert_profile_credentials(dict(io.properties))


@pytest.mark.usefixtures("fake_aws")
def test_no_profile_without_credentials_keeps_default_chain() -> None:
    """No profile and no boto3 credentials: previous behavior."""
    assert s3_credential_properties("us-east-2", None) == {}
    props = dict(build_file_io(_settings(None)).properties)
    assert S3_ACCESS_KEY_ID not in props


@pytest.mark.usefixtures("fake_aws")
def test_no_profile_uses_boto3_default_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without ``aws_profile``, boto3 picks up ``AWS_PROFILE`` from the environment."""
    monkeypatch.setenv("AWS_PROFILE", "iceduck_test")
    _assert_profile_credentials(s3_credential_properties("us-east-2", None))
    _assert_profile_credentials(dict(build_file_io(_settings(None)).properties))


@pytest.mark.usefixtures("fake_aws")
def test_no_profile_uses_env_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _SECRET)
    monkeypatch.setenv("AWS_SESSION_TOKEN", _TOKEN)
    _assert_profile_credentials(s3_credential_properties("us-east-2", None))


@pytest.mark.usefixtures("fake_aws")
def test_missing_env_profile_is_aws_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_PROFILE", "nonexistent_profile_iceduck_xyz")
    with pytest.raises(AwsError) as exc:
        s3_credential_properties("us-east-2", None)
    assert "AWS profile not found" in exc.value.message


@pytest.mark.usefixtures("fake_aws")
def test_missing_profile_is_aws_error() -> None:
    with pytest.raises(AwsError) as exc:
        s3_credential_properties("us-east-2", "nonexistent_profile_iceduck_xyz")
    assert "AWS profile not found" in exc.value.message
