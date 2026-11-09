from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Final, Self
from urllib.parse import urlparse

import duckdb
import pyarrow as pa

from ducklake_iceberg import timing
from ducklake_iceberg.aws import s3_credential_properties
from ducklake_iceberg.config import DuckLakeIcebergSettings
from ducklake_iceberg.errors import AwsError, CatalogError

LOG = logging.getLogger(__name__)

#: name used in LOAD -> name registered in duckdb_extensions()
EXTENSIONS: Final[Mapping[str, str]] = {
    "ducklake": "ducklake",
    "postgres": "postgres_scanner",
    "httpfs": "httpfs",
    "aws": "aws",
    "sqlite": "sqlite_scanner",
}

S3_SECRET: Final[str] = "ducklake_iceberg_s3"

#: ``credential_chain`` providers, in order: static profile keys and the
#: SSO profile (``sso_session``/``sso_*``), which the ``config`` provider
#: does not read.
S3_CREDENTIAL_CHAIN: Final[str] = "config;sso"

CONNECT_CONFIG: Final[Mapping[str, Any]] = {
    "autoinstall_known_extensions": False,
    "autoload_known_extensions": False,
}

#: Extensions required by any backend.
_BASE_EXTENSIONS: Final[tuple[str, ...]] = ("ducklake", "httpfs", "aws")

#: Backends recognized in the DuckLake DSN.
_KNOWN_BACKENDS: Final[frozenset[str]] = frozenset(
    {"postgres", "sqlite", "mysql", "duckdb"}
)

Params = Sequence[Any] | Mapping[str, Any] | None


@dataclass(frozen=True, slots=True)
class Environment:
    """Environment diagnostics, consumed by `doctor`."""

    duckdb_version: str
    extensions: dict[str, bool]  # registered name -> loaded
    attached: tuple[str, ...]  # duckdb_databases()
    secrets: tuple[str, ...]  # duckdb_secrets()
    read_only: bool


def _quote(value: str) -> str:
    """SQL literal in single quotes, escaped by doubling."""
    return "'" + value.replace("'", "''") + "'"


def _escape(value: str) -> str:
    """Escape single quotes for interpolation inside an already quoted literal."""
    return value.replace("'", "''")


def _backend_of(dsn: str) -> str:
    head = dsn.removeprefix("ducklake:").split(":", 1)[0].lower()
    return head if head in _KNOWN_BACKENDS else "duckdb"


def _translate(exc: duckdb.Error, sql: str) -> CatalogError | AwsError:
    """Translate a DuckDB exception into the domain error."""
    text = str(exc)
    lowered = text.lower()

    if "read-only mode" in lowered:
        return CatalogError(
            "connection opened as READ_ONLY",
            hint="the catalog is read-only except for the flush of inlined data",
        )
    if 'provider "credential_chain"' in lowered or (
        "credential_chain" in lowered and "aws extension" in lowered
    ):
        return CatalogError("aws extension not loaded")
    if "no profile" in lowered or ("profile" in lowered and "found" in lowered):
        return AwsError(
            "AWS profile not found",
            hint="adjust --profile or ~/.aws/config",
        )
    if isinstance(exc, duckdb.HTTPException) or ("http" in lowered and "s3" in lowered):
        return AwsError(f"S3 access failure: {text}")
    if "probe ducklake metadata" in lowered or "unable to open database" in lowered:
        return CatalogError("catalog unreachable")
    if "cannot start a transaction within a transaction" in lowered:
        return CatalogError("unexpected nested transaction")
    return CatalogError(text)


class DuckLakeConnection:
    """The single point in the tool that opens a DuckDB connection."""

    def __init__(
        self,
        settings: DuckLakeIcebergSettings,
        *,
        read_only: bool = True,
        s3: bool = True,
    ) -> None:
        self._settings = settings
        self._read_only = read_only
        #: loads httpfs/aws and creates the S3 secret. Metadata-only reads
        #: (doctor, detection, enumeration) do not touch S3 and skip all
        #: three - including credential resolution.
        self._s3 = s3
        self._backend = _backend_of(settings.catalog_dsn)
        self._con: duckdb.DuckDBPyConnection | None = None
        self._in_transaction = False
        #: SQL of the last ATTACH issued; exposed for contract tests.
        self._last_attach_sql: str | None = None

    @property
    def alias(self) -> str:
        """ATTACH alias (`settings.catalog_alias`)."""
        return self._settings.catalog_alias

    @property
    def meta(self) -> str:
        """Name of the METADATA_CATALOG; prefix of `<meta>.ducklake_*`."""
        return self._settings.metadata_catalog

    @property
    def raw(self) -> duckdb.DuckDBPyConnection:
        """Raw DuckDB connection, consumed by the doctor context."""
        return self._require_con()

    @property
    def read_only(self) -> bool:
        return self._read_only

    @property
    def backend(self) -> str:
        return self._backend

    # -- lifecycle ------------------------------------------------------

    def open(self) -> Self:
        timing.count("duckdb.connect")
        with timing.phase("catalog_open"):
            self._con = duckdb.connect(":memory:", config=self._connect_config())
            if self._settings.duckdb_threads is not None:
                self.execute(f"SET threads = {self._settings.duckdb_threads}")
            if self._settings.duckdb_memory_limit is not None:
                self.execute(
                    "SET memory_limit = " + _quote(self._settings.duckdb_memory_limit)
                )
            self._load_extensions()
            if self._backend == "postgres":
                self.execute("CALL postgres_clear_cache()")
            self._create_s3_secret()
            self._attach()
            self._probe_metadata_catalog()
        return self

    def _connect_config(self) -> dict[str, Any]:
        """Config for ``duckdb.connect``; includes the extension directory.

        It goes in the connect call, not in a later ``SET``, so that it applies
        before the first ``INSTALL``/``LOAD`` and without interpolating the
        path into SQL.
        """
        config = dict(CONNECT_CONFIG)
        if self._settings.duckdb_extension_directory is not None:
            config["extension_directory"] = self._settings.duckdb_extension_directory
        return config

    def _probe_metadata_catalog(self) -> None:
        """Confirm that `<meta>.ducklake_*` is visible."""
        try:
            self.query_arrow(f"SELECT count(*) AS n FROM {self.meta}.ducklake_snapshot")
        except CatalogError as exc:
            raise CatalogError(
                f"metadata catalog {self.meta!r} not found",
                hint="check 'metadata_catalog' in the configuration",
            ) from exc

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def __enter__(self) -> Self:
        if self._con is None:
            self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # -- execution -----------------------------------------------------------

    def _require_con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise CatalogError("connection closed")
        return self._con

    def execute(self, sql: str, params: Params = None) -> None:
        """Execute without materializing a result (DDL, CALL, SET)."""
        con = self._require_con()
        try:
            if params is None:
                con.execute(sql)
            else:
                con.execute(sql, params)
        except duckdb.Error as exc:
            LOG.debug("failed to execute SQL: %s", sql, exc_info=exc)
            raise _translate(exc, sql) from exc

    def query_arrow(self, sql: str, params: Params = None) -> pa.Table:
        """Execute and return a `pyarrow.Table`."""
        con = self._require_con()
        try:
            if params is None:
                return con.sql(sql).to_arrow_table()
            return con.sql(sql, params=params).to_arrow_table()
        except duckdb.Error as exc:
            LOG.debug("failed to query SQL: %s", sql, exc_info=exc)
            raise _translate(exc, sql) from exc

    @contextmanager
    def transaction(self) -> Iterator[Self]:
        """BEGIN/COMMIT; ROLLBACK on exception. Not reentrant."""
        if self._in_transaction:
            raise CatalogError("transaction() is not reentrant")
        self.execute("BEGIN TRANSACTION")
        self._in_transaction = True
        try:
            yield self
        except BaseException:
            self.execute("ROLLBACK")
            raise
        else:
            self.execute("COMMIT")
        finally:
            self._in_transaction = False

    @contextmanager
    def read_transaction(self) -> Iterator[Self]:
        """Consistent read transaction; not reentrant."""
        with self.transaction():
            yield self

    # -- opening steps -------------------------------------------------

    def _required_extensions(self) -> tuple[str, ...]:
        extras: tuple[str, ...] = ()
        if self._backend in {"postgres", "sqlite"}:
            extras = (self._backend,)
        base = _BASE_EXTENSIONS if self._s3 else ("ducklake",)
        return (*base, *extras)

    def _load_extensions(self) -> None:
        for name in self._required_extensions():
            try:
                self.execute(f"INSTALL {name}")
                self.execute(f"LOAD {name}")
            except CatalogError as exc:
                raise CatalogError(
                    f"could not load extension '{name}'",
                    hint=(
                        "no network or read-only HOME? point "
                        "duckdb_extension_directory (DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_"
                        "DIRECTORY) to a directory with the extensions "
                        "pre-installed"
                    ),
                ) from exc
        loaded = self.environment().extensions
        missing = [
            EXTENSIONS[n]
            for n in self._required_extensions()
            if not loaded.get(EXTENSIONS[n], False)
        ]
        if missing:
            raise CatalogError(f"extensions not loaded: {', '.join(missing)}")

    def _uses_s3(self) -> bool:
        if not self._s3:
            return False
        return self._settings.data_path.startswith(
            "s3://"
        ) or self._settings.iceberg_metadata_prefix.startswith("s3://")

    def _create_s3_secret(self) -> None:
        if not self._uses_s3():
            return
        props = s3_credential_properties(
            self._settings.aws_region, self._settings.aws_profile
        )
        if props:
            self._create_static_s3_secret(props)
            return
        # boto3 without credentials: AWS SDK C++ chain (previous behavior)
        parts = [
            "TYPE s3",
            "PROVIDER credential_chain",
            f"CHAIN {_quote(S3_CREDENTIAL_CHAIN)}",
            f"REGION {_quote(self._settings.aws_region)}",
        ]
        try:
            self.execute(f"CREATE OR REPLACE SECRET {S3_SECRET} ({', '.join(parts)})")
        except AwsError:
            raise
        except CatalogError as exc:
            # Profile/validation messages are translated to AwsError.
            raise AwsError(
                "could not create the S3 secret",
                hint="check AWS_PROFILE/--profile and ~/.aws/config",
            ) from exc

    def _create_static_s3_secret(self, props: Mapping[str, str]) -> None:
        """Secret with the credential resolved by boto3.

        The SSO provider of the AWS SDK C++ (``aws`` extension) intermittently
        rejects SSO profiles close to token renewal, whereas boto3 handles it;
        resolving through boto3 keeps Glue, FileIO and DuckDB on the same
        source. The credential is passed as a parameter, never in the SQL
        text, and the DuckDB exception is neither chained nor logged.
        """
        from pyiceberg.io import (
            S3_ACCESS_KEY_ID,
            S3_SECRET_ACCESS_KEY,
            S3_SESSION_TOKEN,
        )

        options = [
            "TYPE s3",
            "PROVIDER config",
            "KEY_ID $key_id",
            "SECRET $secret",
            "REGION $region",
        ]
        params = {
            "key_id": props[S3_ACCESS_KEY_ID],
            "secret": props[S3_SECRET_ACCESS_KEY],
            "region": self._settings.aws_region,
        }
        if S3_SESSION_TOKEN in props:
            options.append("SESSION_TOKEN $session_token")
            params["session_token"] = props[S3_SESSION_TOKEN]
        endpoint = self._settings.s3_endpoint
        if endpoint:
            parsed = urlparse(endpoint)
            options += [
                "ENDPOINT $endpoint",
                "URL_STYLE 'path'",
                f"USE_SSL {'true' if parsed.scheme == 'https' else 'false'}",
            ]
            params["endpoint"] = parsed.netloc
        try:
            self._require_con().execute(
                f"CREATE OR REPLACE SECRET {S3_SECRET} ({', '.join(options)})",
                params,
            )
        except duckdb.Error:
            raise AwsError(
                "could not create the S3 secret with the resolved credential",
                hint="check the aws/httpfs extensions and the AWS credential",
            ) from None

    def _attach(self) -> None:
        opts = [f"METADATA_CATALOG '{self.meta}'"]
        if self._read_only:
            opts.append("READ_ONLY")
        sql = (
            f"ATTACH '{_escape(self._settings.catalog_dsn)}' "
            f"AS {self.alias} ({', '.join(opts)})"
        )
        self._last_attach_sql = sql
        self.execute(sql)

    # -- diagnostics --------------------------------------------------------

    def environment(self) -> Environment:
        con = self._require_con()
        version_rows = con.execute("SELECT version()").fetchall()
        version = str(version_rows[0][0])

        registered = tuple(EXTENSIONS[n] for n in EXTENSIONS)
        placeholders = ", ".join("?" for _ in registered)
        ext_rows = con.execute(
            "SELECT extension_name, loaded FROM duckdb_extensions() "
            f"WHERE extension_name IN ({placeholders})",
            list(registered),
        ).fetchall()
        extensions = {str(name): bool(loaded) for name, loaded in ext_rows}

        db_rows = con.execute(
            "SELECT database_name FROM duckdb_databases() ORDER BY database_name"
        ).fetchall()
        attached = tuple(str(r[0]) for r in db_rows)

        secret_rows = con.execute(
            "SELECT name FROM duckdb_secrets() ORDER BY name"
        ).fetchall()
        secrets = tuple(str(r[0]) for r in secret_rows)

        return Environment(
            duckdb_version=version,
            extensions=extensions,
            attached=attached,
            secrets=secrets,
            read_only=self._read_only,
        )


def open_catalog(
    settings: DuckLakeIcebergSettings, *, read_only: bool = True, s3: bool = True
) -> DuckLakeConnection:
    """Factory used by the CLI; equivalent to `DuckLakeConnection(...).open()`."""
    return DuckLakeConnection(settings, read_only=read_only, s3=s3).open()


@contextmanager
def metadata_catalog(
    settings: DuckLakeIcebergSettings, con: DuckLakeConnection | None = None
) -> Iterator[DuckLakeConnection]:
    """READ_ONLY metadata connection: reuses ``con`` or opens one without S3.

    ``sync`` opens a connection and lends it to doctor, checkpoint and export;
    standalone commands pass ``None`` and the connection is opened and closed
    here.
    """
    if con is not None:
        yield con
        return
    with open_catalog(settings, read_only=True, s3=False) as own:
        yield own
