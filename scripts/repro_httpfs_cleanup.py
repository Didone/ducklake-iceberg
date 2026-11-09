"""Minimal repro: ``ducklake_cleanup_old_files`` on S3 (httpfs).

With the ``aws`` extension loaded, ``CALL ducklake_cleanup_old_files(...)`` on
an ``s3://`` ``DATA_PATH`` aborts with::

    INTERNAL Error: Information loss on integer cast: value -1000000000000
    outside of target range [0, 18446744073709551615]

Cause: ``S3FileSystem::RemoveFiles`` (httpfs ``77392a7207``) builds the identity
of the ``DeleteObjects`` batch by looking up the secret of each S3 type (``s3``,
``r2``, ``gcs``, ``aws``) and converts the match score with
``NumericCast<idx_t>(match.score)``. The score is ``100 * prefix - tie_break``;
a secret with an empty scope (prefix 0) yields a negative score. The ``aws``
extension (``16ae9c1353``) registers the ``aws_default`` storage with tie-break
``1000000000000`` and the ``__aws_default`` secret (type ``aws``, scope ``''``):
score ``-1000000000000``. A plain ``CREATE SECRET (TYPE aws, ...)`` with an
empty scope in temporary storage yields ``-10`` and fails the same way. Fixed
upstream in duckdb/duckdb-httpfs#507 (2026-10-29), with no published build yet
for v2.0.0.

Each variant opens a fresh DuckDB database (the INTERNAL Error invalidates the
database), creates a DuckLake (SQLite catalog) with ``DATA_PATH`` in the bucket,
schedules two files for deletion (two INSERTs +
``ducklake_merge_adjacent_files``) and runs
``ducklake_cleanup_old_files(..., cleanup_all => true)``.

    # Ephemeral MinIO via testcontainers (requires Docker)
    python scripts/repro_httpfs_cleanup.py

    # already-running S3-compatible endpoint
    python scripts/repro_httpfs_cleanup.py --endpoint localhost:9000 \\
        --key-id minioadmin --secret minioadmin

    # Real S3 (credential_chain, like the PoC Lambda); deletes only what it creates
    python scripts/repro_httpfs_cleanup.py --real-s3 s3://bucket/tmp/repro \\
        --region us-east-2

Exits 1 if the ``aws`` variant reproduces the error (bug present), 0 otherwise.
Uses only the already-installed extensions (``INSTALL`` without network is a
no-op).
"""

from __future__ import annotations

import argparse
import re
import sys
import tempfile
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

DEFAULT_IMAGE = "pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
BUCKET = "repro-httpfs"
BASE_EXTENSIONS = ("ducklake", "httpfs", "sqlite")


@dataclass(frozen=True)
class Variant:
    name: str
    load_aws: bool
    pre: tuple[str, ...] = field(default=())


VARIANTS = (
    Variant("no-aws", load_aws=False),
    Variant("aws", load_aws=True),
    Variant(
        "aws+secret-aws",
        load_aws=True,
        pre=(
            "CREATE SECRET extra_aws (TYPE aws, PROVIDER config, "
            "KEY_ID 'x', SECRET 'y', REGION 'us-east-1')",
        ),
    ),
    Variant(
        "aws+default_off", load_aws=True, pre=("SET aws_default_credentials = false",)
    ),
    Variant("aws+drop_default", load_aws=True, pre=("DROP SECRET __aws_default",)),
)


@dataclass(frozen=True)
class Target:
    secret_sql: str
    data_root: str  # s3://bucket/prefix (no trailing slash)


@contextmanager
def _minio(image: str) -> Iterator[tuple[str, str, str]]:
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    container = (
        DockerContainer(image)
        .with_env("MINIO_ROOT_USER", "minioadmin")
        .with_env("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_command("server /data")
        .with_exposed_ports(9000)
        .waiting_for(LogMessageWaitStrategy(re.compile("API:")))
    )
    with container:
        endpoint = (
            f"{container.get_container_host_ip()}:{container.get_exposed_port(9000)}"
        )
        yield endpoint, "minioadmin", "minioadmin"


def _create_bucket(endpoint: str, key_id: str, secret: str, region: str) -> None:
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name=region,
    )
    if BUCKET not in {b["Name"] for b in client.list_buckets().get("Buckets", [])}:
        client.create_bucket(Bucket=BUCKET)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _static_secret(endpoint: str, key_id: str, secret: str, region: str) -> str:
    return (
        "CREATE SECRET repro_s3 (TYPE s3, PROVIDER config, "
        f"KEY_ID {_literal(key_id)}, SECRET {_literal(secret)}, "
        f"REGION {_literal(region)}, ENDPOINT {_literal(endpoint)}, "
        "URL_STYLE 'path', USE_SSL false)"
    )


def _connect(variant: Variant) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
        },
    )
    extensions = (*BASE_EXTENSIONS, "aws") if variant.load_aws else BASE_EXTENSIONS
    for name in extensions:
        con.execute(f"INSTALL {name}")
        con.execute(f"LOAD {name}")
    return con


def _versions() -> list[tuple[str, str, str]]:
    con = _connect(Variant("versions", load_aws=True))
    try:
        rows = con.execute(
            "SELECT extension_name, extension_version, coalesce(installed_from, '') "
            "FROM duckdb_extensions() WHERE loaded "
            "AND extension_name IN ('ducklake', 'httpfs', 'aws', 'sqlite_scanner') "
            "ORDER BY extension_name"
        ).fetchall()
        version = con.execute(
            "SELECT version(), source_id FROM pragma_version()"
        ).fetchone()
    finally:
        con.close()
    assert version is not None
    return [("duckdb", f"{version[0]} ({version[1]})", "python"), *rows]


def _object_count(con: duckdb.DuckDBPyConnection, prefix: str) -> int:
    row = con.execute("SELECT count(*) FROM glob(?)", [f"{prefix}/**"]).fetchone()
    return int(row[0]) if row else 0


def run_variant(variant: Variant, target: Target, workdir: Path) -> tuple[bool, str]:
    """Return ``(reproduced, detail)`` for the variant."""
    data_path = f"{target.data_root}/{variant.name}-{uuid.uuid4().hex[:8]}"
    con = _connect(variant)
    try:
        con.execute(target.secret_sql)
        for sql in variant.pre:
            con.execute(sql)
        catalog = workdir / f"{variant.name}.sqlite"
        con.execute(
            f"ATTACH {_literal('ducklake:sqlite:' + str(catalog))} AS lake "
            f"(DATA_PATH {_literal(data_path + '/')}, METADATA_CATALOG 'lake_meta', "
            "DATA_INLINING_ROW_LIMIT 0)"
        )
        con.execute("CREATE TABLE lake.t (id INTEGER)")
        con.execute("INSERT INTO lake.t SELECT * FROM range(1000)")
        con.execute("INSERT INTO lake.t SELECT * FROM range(1000, 2000)")
        con.execute("CALL ducklake_merge_adjacent_files('lake')")
        before = _object_count(con, data_path)
        try:
            con.execute(
                "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)"
            ).fetchall()
        except duckdb.Error as exc:
            first = str(exc).splitlines()[0]
            reproduced = "Information loss on integer cast" in first
            return reproduced, f"{type(exc).__name__}: {first}"
        after = _object_count(con, data_path)
        row = con.execute(
            "SELECT count(*) FROM lake_meta.ducklake_files_scheduled_for_deletion"
        ).fetchone()
        queue = int(row[0]) if row else -1
        return False, f"ok: objects {before} -> {after}, queue {queue}"
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--endpoint", help="S3-compatible host:port (without http://)")
    parser.add_argument("--key-id", default="minioadmin")
    parser.add_argument("--secret", default="minioadmin")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--image", default=DEFAULT_IMAGE, help="MinIO image")
    parser.add_argument(
        "--real-s3",
        metavar="S3_URI",
        help="prefix on real S3; uses PROVIDER credential_chain like the PoC",
    )
    parser.add_argument(
        "--variants",
        default=",".join(v.name for v in VARIANTS),
        help="comma-separated list",
    )
    args = parser.parse_args(argv)
    chosen = [v for v in VARIANTS if v.name in set(args.variants.split(","))]

    print("versions:")
    for name, version, source in _versions():
        print(f"  {name:<16} {version:<24} {source}")

    with tempfile.TemporaryDirectory() as tmp, contextmanager_target(args) as target:
        results = {v.name: run_variant(v, target, Path(tmp)) for v in chosen}

    print("\nvariants:")
    for name, (reproduced, detail) in results.items():
        flag = "REPRODUCES" if reproduced else "-"
        print(f"  {name:<18} {flag:<9} {detail}")
    return 1 if results.get("aws", (False, ""))[0] else 0


@contextmanager
def contextmanager_target(args: argparse.Namespace) -> Iterator[Target]:
    if args.real_s3:
        secret_sql = (
            "CREATE SECRET repro_s3 (TYPE s3, PROVIDER credential_chain, "
            f"REGION {_literal(args.region)})"
        )
        yield Target(secret_sql, args.real_s3.rstrip("/"))
        return
    if args.endpoint:
        _create_bucket(args.endpoint, args.key_id, args.secret, args.region)
        yield Target(
            _static_secret(args.endpoint, args.key_id, args.secret, args.region),
            f"s3://{BUCKET}/repro",
        )
        return
    with _minio(args.image) as (endpoint, key_id, secret):
        _create_bucket(endpoint, key_id, secret, args.region)
        yield Target(
            _static_secret(endpoint, key_id, secret, args.region),
            f"s3://{BUCKET}/repro",
        )


if __name__ == "__main__":
    sys.exit(main())
