"""Orphan cleanup: listing, classification, and deletion of objects.

The candidate universe is the metadata prefix **of one table**
(``{iceberg_metadata_prefix}/{glue_database}/{table}``). Listing is always
recursive and uses the service clock (S3 ``LastModified``), never the local one.

Two stores sit behind the same ``ObjectStore`` protocol:

- ``S3Store``: paginated ``ListObjectsV2`` and ``DeleteObjects`` in batches of
  1000 (boto3, the same client moto intercepts in tests);
- ``LocalStore``: absolute path (or ``file://``) via ``pathlib``, used with
  local prefixes in development and in end-to-end tests.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Final, Protocol

from botocore.exceptions import BotoCoreError, ClientError

from ducklake_iceberg.catalog.files import canonical
from ducklake_iceberg.deletes.convert import parse_source_key
from ducklake_iceberg.errors import AwsError, DuckLakeIcebergError, ExitCode
from ducklake_iceberg.register import POINTER_NAME

LOG = logging.getLogger("ducklake_iceberg")

#: Maximum keys per ``DeleteObjects`` call (API limit).
DELETE_BATCH: Final[int] = 1000

_MANIFEST_LIST_RE: Final[re.Pattern[str]] = re.compile(r"^snap-.*\.avro$")
_MULTI_SLASH: Final[re.Pattern[str]] = re.compile(r"/{2,}")
_DOT_SEGMENT: Final[re.Pattern[str]] = re.compile(r"/\./")


# -- structures -----------------------------------------------------------------


class OrphanKind(StrEnum):
    """Classification of a file under the metadata prefix."""

    METADATA_JSON = "metadata_json"
    MANIFEST_LIST = "manifest_list"
    MANIFEST = "manifest"
    #: position delete file written by ducklake-iceberg in ``{location}/deletes/``
    POSITION_DELETE = "position_delete"
    UNKNOWN = "unknown"  # never deleted; only reported


@dataclass(frozen=True)
class S3Object:
    """Listed object. ``key`` is the absolute URI (``s3://b/k`` or a local path)."""

    key: str
    size_bytes: int
    last_modified: datetime


class StorageError(AwsError):
    """Listing/deletion failure in the object store (exit 1)."""


class PrefixGuardError(DuckLakeIcebergError):
    """Prefix guard G1/G3 failed (exit 1)."""


class PrefixOverlapError(PrefixGuardError):
    """Guard G2/G4: data and metadata mixed, which is not allowed (exit 2)."""

    exit_code = ExitCode.BLOCKED


# -- paths ----------------------------------------------------------------------


def canonical_uri(uri: str) -> str:
    """Canonical form to compare metadata paths and listing paths.

    ``file:///x`` and ``/x`` are the same local file; the rest is
    ``ducklake_iceberg.catalog.files.canonical`` (duplicate slashes, ``./``, scheme).
    """
    if uri.startswith("file://"):
        uri = uri.removeprefix("file://")
    if "://" not in uri:
        # local path: same normalization (without resolving "..")
        uri = _DOT_SEGMENT.sub("/", _MULTI_SLASH.sub("/", uri))
    return canonical(uri)


def _dir(uri: str) -> str:
    """Canonical prefix ending in ``/`` (segment boundary)."""
    return canonical_uri(uri).rstrip("/") + "/"


def is_under(path: str, prefix: str) -> bool:
    """``path`` is under ``prefix``, respecting the ``/`` boundary."""
    return _dir(path).startswith(_dir(prefix))


def _split_s3(uri: str) -> tuple[str, str]:
    rest = canonical_uri(uri).removeprefix("s3://")
    bucket, _, key = rest.partition("/")
    return bucket, key


# -- classification -------------------------------------------------------------


def classify_object(key: str) -> OrphanKind:
    """Classify an object by name (allowlist)."""
    basename = key.rstrip("/").rsplit("/", 1)[-1]
    if basename == POINTER_NAME:
        # handoff pointer: never a candidate
        return OrphanKind.UNKNOWN
    if basename.endswith(".metadata.json"):
        return OrphanKind.METADATA_JSON
    if _MANIFEST_LIST_RE.match(basename):
        return OrphanKind.MANIFEST_LIST
    if basename.endswith(".avro"):
        return OrphanKind.MANIFEST
    if parse_source_key(key) is not None:
        return OrphanKind.POSITION_DELETE
    # everything else (data .parquet, .puffin, .crc, _locks/...)
    return OrphanKind.UNKNOWN


# -- object store ---------------------------------------------------------------


class ObjectStore(Protocol):
    """Recursive listing and batch deletion under a prefix."""

    def list_objects(self, prefix: str) -> Iterator[S3Object]: ...

    def delete_objects(
        self, keys: Sequence[str]
    ) -> tuple[list[str], list[tuple[str, str]]]:
        """Delete ``keys``; return ``(deleted, [(key, error), ...])``."""
        ...


class S3Store:
    """Paginated ``ListObjectsV2`` + batched ``DeleteObjects`` (boto3)."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list_objects(self, prefix: str) -> Iterator[S3Object]:
        bucket, key_prefix = _split_s3(prefix)
        key_prefix = key_prefix.rstrip("/") + "/" if key_prefix else ""
        paginator = self._client.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
                for obj in page.get("Contents", []):
                    last_modified = obj["LastModified"]
                    if last_modified.tzinfo is None:
                        last_modified = last_modified.replace(tzinfo=UTC)
                    yield S3Object(
                        key=f"s3://{bucket}/{obj['Key']}",
                        size_bytes=int(obj["Size"]),
                        last_modified=last_modified,
                    )
        except (ClientError, BotoCoreError) as exc:
            raise StorageError(f"failed to list {prefix}: {exc}") from exc

    def delete_objects(
        self, keys: Sequence[str]
    ) -> tuple[list[str], list[tuple[str, str]]]:
        deleted: list[str] = []
        failed: list[tuple[str, str]] = []
        by_bucket: dict[str, list[str]] = {}
        for uri in keys:
            bucket, key = _split_s3(uri)
            by_bucket.setdefault(bucket, []).append(key)
        for bucket, bucket_keys in by_bucket.items():
            for start in range(0, len(bucket_keys), DELETE_BATCH):
                batch = bucket_keys[start : start + DELETE_BATCH]
                try:
                    response = self._client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": [{"Key": k} for k in batch], "Quiet": False},
                    )
                except (ClientError, BotoCoreError) as exc:
                    failed.extend((f"s3://{bucket}/{k}", str(exc)) for k in batch)
                    continue
                deleted.extend(
                    f"s3://{bucket}/{d['Key']}" for d in response.get("Deleted", [])
                )
                failed.extend(
                    (
                        f"s3://{bucket}/{e.get('Key')}",
                        f"{e.get('Code')}: {e.get('Message')}",
                    )
                    for e in response.get("Errors", [])
                )
        return deleted, failed


class LocalStore:
    """Absolute local prefix (development and tests)."""

    def list_objects(self, prefix: str) -> Iterator[S3Object]:
        root = Path(canonical_uri(prefix))
        if not root.exists():
            return
        if not root.is_dir():
            raise StorageError(f"local prefix {root} is not a directory")
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            stat = path.stat()
            yield S3Object(
                key=str(path),
                size_bytes=stat.st_size,
                last_modified=datetime.fromtimestamp(stat.st_mtime, UTC),
            )

    def delete_objects(
        self, keys: Sequence[str]
    ) -> tuple[list[str], list[tuple[str, str]]]:
        deleted: list[str] = []
        failed: list[tuple[str, str]] = []
        for key in keys:
            try:
                Path(canonical_uri(key)).unlink()
            except OSError as exc:
                failed.append((key, str(exc)))
            else:
                deleted.append(key)
        return deleted, failed


def object_store_for(
    prefix: str, *, region: str, profile: str | None = None
) -> ObjectStore:
    """``S3Store`` for ``s3://``; ``LocalStore`` for an absolute path/``file://``."""
    if prefix.startswith("s3://"):
        import boto3

        session = boto3.Session(profile_name=profile, region_name=region)
        return S3Store(session.client("s3"))
    if prefix.startswith("file://") or prefix.startswith("/"):
        return LocalStore()
    raise PrefixGuardError(
        f"metadata prefix with unsupported scheme: {prefix}",
        hint="use s3://bucket/prefix (or an absolute local path in tests)",
    )


# -- prefix guards --------------------------------------------------------------


def _segments(prefix: str) -> list[str]:
    canon = canonical_uri(prefix)
    if canon.startswith("s3://"):
        _bucket, key = _split_s3(canon)
        return [p for p in key.split("/") if p]
    if "://" in canon:
        canon = canon.split("://", 1)[1].partition("/")[2]
    return [p for p in canon.split("/") if p]


def validate_prefix_guards(
    *,
    prefix: str,
    metadata_location: str,
    data_path: str,
) -> None:
    """Guards G1-G3, run before any listing.

    Raises:
        PrefixGuardError: G1 (prefix does not contain the current
            metadata_location) or G3 (fewer than 2 segments beyond the
            bucket/root), exit 1.
        PrefixOverlapError: G2 (prefix and ``data_path`` overlap), exit 2.
    """
    # G1: the target prefix contains the current metadata_location
    if not is_under(metadata_location, prefix):
        raise PrefixGuardError(
            f"prefix {prefix} does not contain the current metadata_location "
            f"{metadata_location}; it would delete files of another table"
        )
    # G2: neither inside the DuckLake data_path nor containing it
    if is_under(prefix, data_path) or is_under(data_path, prefix):
        raise PrefixOverlapError(
            f"metadata prefix {prefix} overlaps data_path {data_path}: "
            f"iceberg_metadata_prefix must not overlap data_path",
            hint="move the metadata outside data_path and republish",
        )
    # G3: at least 2 segments beyond the bucket (root protection)
    if len(_segments(prefix)) < 2:
        raise PrefixGuardError(
            f"prefix {prefix} has fewer than 2 segments beyond the bucket; "
            f"root protection"
        )


def check_parquet_in_prefix(objects: Sequence[S3Object]) -> list[str]:
    """G4: ``.parquet`` under the prefix that is **not** a position delete we wrote."""
    return [
        o.key
        for o in objects
        if o.key.endswith(".parquet")
        and classify_object(o.key) is not OrphanKind.POSITION_DELETE
    ]
