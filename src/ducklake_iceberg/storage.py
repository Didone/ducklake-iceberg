"""Shared FileIO construction, independent of Glue publication."""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING

from pyiceberg.io import AWS_PROFILE_NAME, AWS_REGION, load_file_io

from ducklake_iceberg import timing
from ducklake_iceberg.aws import (
    client,
    s3_credential_properties,
    s3_endpoint_properties,
)
from ducklake_iceberg.metadata_io import MetadataFileIO

if TYPE_CHECKING:
    from pyiceberg.io import FileIO

    from ducklake_iceberg.config import DuckLakeIcebergSettings


def build_file_io(settings: DuckLakeIcebergSettings) -> FileIO:
    """Reuse FileIO by effective endpoint, region and resolved credentials."""
    props: dict[str, str] = {AWS_REGION: settings.aws_region}
    if settings.aws_profile is not None:
        props[AWS_PROFILE_NAME] = settings.aws_profile
    props.update(s3_credential_properties(settings.aws_region, settings.aws_profile))
    props.update(s3_endpoint_properties(settings.s3_endpoint))
    return _file_io(
        tuple(sorted(props.items())),
        settings.iceberg_metadata_prefix,
        settings.aws_region,
        settings.aws_profile,
        settings.s3_endpoint,
    )


@functools.lru_cache(maxsize=8)
def _file_io(
    props: tuple[tuple[str, str], ...],
    prefix: str,
    region: str,
    profile: str | None,
    endpoint: str | None,
) -> FileIO:
    timing.count("fileio")
    delegate = load_file_io(properties=dict(props))
    if prefix.startswith("s3://"):
        return MetadataFileIO(
            delegate, client("s3", region, profile, endpoint_url=endpoint), prefix
        )
    return delegate


def clear_file_io_cache() -> None:
    """Reset process-wide FileIO state for tests."""
    _file_io.cache_clear()
