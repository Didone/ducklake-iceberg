"""``aws_profile`` credentials for PyIceberg's S3 I/O.

``PyArrowFileIO``, which PyIceberg prefers for ``s3://``, ignores
``client.profile-name`` (only ``FsspecFileIO`` reads it): Arrow's
``S3FileSystem`` falls back to the default AWS SDK C++ chain, which only knows
``AWS_PROFILE`` and the ``default`` profile. Without this bridge, Iceberg
metadata would be read and written with an identity other than Glue's.

Credentials are resolved by boto3 (the same path used by Glue and ``clean``,
including SSO profiles, with or without ``aws_profile``) and passed as ``s3.*``
properties, which work for both PyArrow and fsspec, and as
``KEY_ID``/``SECRET``/``SESSION_TOKEN`` in the DuckDB secret. They are frozen
when the FileIO/secret is built: each command builds its own, so they only
need to last one command.

One ``boto3.Session`` per process (per region, profile, and ``AWS_*``
environment): botocore keeps the credential in memory only, inside the Session,
so every new Session repeated the SSO ``GetRoleCredentials`` call (~0.5 s).
When reused, the credential is resolved once and refreshed by botocore itself
near expiry. Clients (thread-safe) are reused as well.
"""

from __future__ import annotations

import os
import threading
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ProfileNotFound
from pyiceberg.io import (
    S3_ACCESS_KEY_ID,
    S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
)

from ducklake_iceberg import timing
from ducklake_iceberg.errors import AwsError

_LOCK = threading.RLock()
_SESSIONS: dict[tuple[Any, ...], boto3.Session] = {}
_CLIENTS: dict[tuple[Any, ...], Any] = {}


def _key(region: str, profile: str | None) -> tuple[Any, ...]:
    """Region, profile, and the ``AWS_*`` environment the boto3 chain consults."""
    env = tuple(sorted((k, v) for k, v in os.environ.items() if k.startswith("AWS_")))
    return (region, profile, env)


def session(region: str, profile: str | None) -> boto3.Session:
    """The process-wide ``boto3.Session`` for ``(region, profile)``.

    Raises:
        ProfileNotFound: profile does not exist (not cached).
    """
    key = _key(region, profile)
    with _LOCK:
        cached = _SESSIONS.get(key)
        if cached is None:
            cached = boto3.Session(profile_name=profile, region_name=region)
            timing.count("boto3.session")
            timing.instrument_session(cached)
            _SESSIONS[key] = cached
        return cached


def client(service: str, region: str, profile: str | None) -> Any:
    """Boto3 client for ``service`` on the process-wide Session."""
    key = (service, *_key(region, profile))
    with _LOCK:
        cached = _CLIENTS.get(key)
        if cached is None:
            cached = session(region, profile).client(service)
            _CLIENTS[key] = cached
        return cached


def clear_cache() -> None:
    """Drop Sessions and clients (tests that change the AWS environment)."""
    with _LOCK:
        _SESSIONS.clear()
        _CLIENTS.clear()


def s3_endpoint_properties(endpoint: str | None) -> dict[str, str]:
    """``s3.*`` property that points PyIceberg's FileIO at a custom endpoint."""
    return {S3_ENDPOINT: endpoint} if endpoint else {}


def s3_credential_properties(region: str, profile: str | None) -> dict[str, str]:
    """``s3.*`` properties with the credential resolved by boto3.

    With ``aws_profile``, the profile's credential; without it, the default
    boto3 chain (``AWS_PROFILE`` from the environment, key variables, SSO,
    instance role). Empty only when, without a profile, boto3 finds no
    credential: the caller then follows the AWS SDK C++ chain (previous
    behavior).

    Raises:
        AwsError: profile missing, no credential, or expired SSO token.
    """
    label = repr(profile) if profile is not None else "from the environment"
    try:
        with timing.phase("credentials"):
            credentials = session(region, profile).get_credentials()
            frozen = credentials.get_frozen_credentials() if credentials else None
    except ProfileNotFound as exc:
        raise AwsError(
            f"AWS profile not found: {label}",
            hint="fix aws_profile, AWS_PROFILE, or ~/.aws/config",
        ) from exc
    except BotoCoreError as exc:
        raise AwsError(
            f"credential for AWS profile {label} unavailable: {type(exc).__name__}",
            hint="aws sso login (or renew the credential)",
        ) from exc
    if frozen is None and profile is None:
        return {}
    if frozen is None or not frozen.access_key or not frozen.secret_key:
        raise AwsError(
            f"AWS profile {label} has no credential",
            hint="aws sso login (or renew the credential)",
        )
    props = {
        S3_ACCESS_KEY_ID: frozen.access_key,
        S3_SECRET_ACCESS_KEY: frozen.secret_key,
    }
    if frozen.token:
        props[S3_SESSION_TOKEN] = frozen.token
    return props


__all__ = ["clear_cache", "client", "s3_credential_properties", "session"]
