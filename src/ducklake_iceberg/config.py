from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar, Literal
from urllib.parse import urlparse

import yaml
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from ducklake_iceberg.errors import ConfigError

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]
StatsSource = Literal["catalog", "footer"]
#: PyIceberg catalog types the tool can be configured with
CatalogType = Literal["glue", "rest", "sql", "hive"]
#: catalog types the ``sync`` can publish to in this release
SUPPORTED_CATALOG_TYPES: frozenset[str] = frozenset({"glue"})


SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
GLUE_DATABASE = re.compile(r"^[a-z0-9_]{1,255}$")
AWS_REGION = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d$")

#: Environment variable that points to an explicit configuration file.
CONFIG_FILE_ENV = "DUCKLAKE_ICEBERG_CONFIG_FILE"

#: Locations tried when no file is given.
CONFIG_SEARCH_PATH: tuple[Path, ...] = (
    Path("ducklake-iceberg.yaml"),
    Path("ducklake-iceberg.yml"),
    Path.home() / ".config" / "ducklake-iceberg" / "config.yaml",
)

#: Credential tokens forbidden in the DSN; compared case-insensitively.
_CREDENTIAL_TOKENS: tuple[str, ...] = ("password=", "pwd=", "user=")
#: Catalog property names whose values are secrets (redacted from the output)
_SECRET_PROPERTY = re.compile(
    r"(?i)(credential|secret|token|password|passwd|private|key-id|access-key|signature)"
)
CATALOG_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
NAMESPACE_PART = re.compile(r"^[^.\s/]{1,255}$")


class CatalogSettings(BaseModel):
    """The Iceberg catalog the table is published to.

    ``type`` and ``properties`` are handed to PyIceberg's ``load_catalog``, so
    every option of the chosen catalog type is available (``uri``,
    ``warehouse``, ``credential``, ``glue.region``...). Secrets should come
    from the environment (``DUCKLAKE_ICEBERG_CATALOG__PROPERTIES__CREDENTIAL``)
    rather than the YAML; they are redacted from logs and envelopes.
    """

    model_config = ConfigDict(extra="forbid")

    type: CatalogType = "glue"
    #: label of the catalog in logs, the envelope and the lock key
    name: str = "default"
    #: namespace of the published tables: ``db`` or ``catalog.schema``
    namespace: str
    properties: dict[str, str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _valid_name(cls, value: str) -> str:
        if not CATALOG_NAME.fullmatch(value):
            raise ValueError(f"invalid catalog.name: {value!r}")
        return value

    @field_validator("namespace")
    @classmethod
    def _valid_namespace(cls, value: str) -> str:
        parts = value.split(".")
        if not all(NAMESPACE_PART.fullmatch(p) for p in parts):
            raise ValueError(f"invalid catalog.namespace: {value!r}")
        return value

    @model_validator(mode="after")
    def _glue_namespace(self) -> CatalogSettings:
        if self.type == "glue" and not GLUE_DATABASE.fullmatch(self.namespace):
            raise ValueError(
                f"invalid catalog.namespace for Glue: {self.namespace!r} "
                "(one level, lowercase letters, digits and underscores)"
            )
        return self

    @property
    def namespace_parts(self) -> tuple[str, ...]:
        return tuple(self.namespace.split("."))

    def redacted(self) -> dict[str, Any]:
        data = self.model_dump()
        data["properties"] = {
            key: ("***" if _SECRET_PROPERTY.search(key) else value)
            for key, value in self.properties.items()
        }
        return data


class DuckLakeIcebergSettings(BaseSettings):
    """Validated configuration of the tool."""

    model_config = SettingsConfigDict(
        env_prefix="DUCKLAKE_ICEBERG_",
        case_sensitive=False,
        extra="forbid",
        validate_default=True,
        env_nested_delimiter="__",
    )

    catalog_dsn: str = Field(
        description="DuckLake DSN, e.g. 'ducklake:postgres:dbname=lake host=pg'"
    )
    catalog_alias: str = Field(default="lake", description="ATTACH alias")
    metadata_catalog: str = Field(
        default="lake_meta", description="ATTACH METADATA_CATALOG"
    )
    data_path: str = Field(description="root of the DuckLake Parquet files")
    iceberg_metadata_prefix: str = Field(
        description="S3 prefix of the Iceberg metadata"
    )
    #: Iceberg catalog; without it, ``glue_database`` selects a Glue catalog
    catalog: CatalogSettings | None = None
    #: legacy shortcut for ``catalog: {type: glue, namespace: <glue_database>}``
    glue_database: str | None = None
    #: region of the S3 storage (and of Glue, for a Glue catalog)
    aws_region: str
    aws_profile: str | None = None
    log_level: LogLevel = "INFO"
    #: source of the statistics of new data files: ``catalog`` reads
    #: ``ducklake_file_column_stats`` (no S3 GET); ``footer`` reads the Parquet
    #: footer of each file. A file without stats in the catalog falls back to
    #: the footer.
    stats_source: StatsSource = "catalog"
    max_footer_workers: int = Field(default=8, ge=1)
    max_io_workers: int = Field(default=8, ge=1)
    max_total_workers: int = Field(default=32, ge=1)
    duckdb_threads: int | None = Field(default=None, ge=1)
    duckdb_memory_limit: str | None = None
    #: PyIceberg singleton pool. No flag: the variable is
    #: ``PYICEBERG_MAX_WORKERS`` itself (or ``DUCKLAKE_ICEBERG_PYICEBERG_MAX_WORKERS``);
    #: the effective value, capped by ``max_total_workers``, is written back.
    pyiceberg_max_workers: int = Field(
        default=8,
        ge=1,
        validation_alias=AliasChoices(
            "pyiceberg_max_workers", "ducklake_iceberg_pyiceberg_max_workers"
        ),
    )
    #: DuckDB extension directory, passed as ``extension_directory``
    #: on connect. Allows a read-only ``HOME``; the
    #: ``DUCKDB_EXTENSION_DIRECTORY`` variable has no effect in duckdb 2.0.
    duckdb_extension_directory: str | None = None
    #: Custom S3 endpoint (MinIO, LocalStack, any S3-compatible store), e.g.
    #: ``http://minio:9000``. Applied to the Iceberg metadata I/O and to the
    #: DuckDB S3 secret, always with path-style addressing. ``None`` = AWS S3.
    s3_endpoint: str | None = None
    #: Iceberg snapshot retention, in hours: each export expires history
    #: older than this, except the current snapshot. ``None`` retains
    #: everything. It must be shorter than the DuckLake file cleanup retention,
    #: otherwise a retained snapshot references Parquet files already deleted.
    snapshot_retention_hours: int | None = Field(default=None, ge=1)

    #: Resolved YAML file, kept on the instance for `redacted()`.
    _config_file: Path | None = PrivateAttr(default=None)

    #: Transient channel for `settings_customise_sources`: `load_settings`
    #: sets this class attribute before building and removes it right after.
    #: `ClassVar` keeps pydantic from treating it as a field or as an instance
    #: private attr.
    _yaml_source: ClassVar[Path | None] = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """init (flags) > env > YAML > default."""
        yaml_file = settings_cls.__dict__.get("_yaml_source")
        return (
            init_settings,
            env_settings,
            YamlConfigSettingsSource(settings_cls, yaml_file=yaml_file),
        )

    @field_validator("catalog_alias", "metadata_catalog")
    @classmethod
    def _sql_identifier(cls, value: str) -> str:
        if not SQL_IDENTIFIER.fullmatch(value):
            raise ValueError(f"{value!r} is not a valid SQL identifier")
        return value

    @field_validator("catalog_dsn")
    @classmethod
    def _dsn_without_credentials(cls, value: str) -> str:
        if not value.startswith("ducklake:"):
            raise ValueError("DSN must start with 'ducklake:'")
        lowered = value.lower()
        if any(token in lowered for token in _CREDENTIAL_TOKENS):
            raise ValueError("credentials in the DSN; use CREATE SECRET")
        return value

    @field_validator("data_path", "iceberg_metadata_prefix")
    @classmethod
    def _normalized_location(cls, value: str) -> str:
        return _normalize_location(value)

    @field_validator("glue_database")
    @classmethod
    def _valid_glue_database(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not GLUE_DATABASE.fullmatch(value):
            raise ValueError(f"invalid glue_database: {value!r}")
        return value

    @field_validator("s3_endpoint")
    @classmethod
    def _valid_s3_endpoint(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = urlparse(value)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(
                f"invalid s3_endpoint: {value!r}; use 'http(s)://host[:port]'"
            )
        return value.rstrip("/")

    @field_validator("aws_region")
    @classmethod
    def _valid_aws_region(cls, value: str) -> str:
        if not AWS_REGION.fullmatch(value):
            raise ValueError(f"invalid aws_region: {value!r}")
        return value

    @model_validator(mode="after")
    def _metadata_prefix_outside_data_path(self) -> DuckLakeIcebergSettings:
        if _overlaps(self.data_path, self.iceberg_metadata_prefix):
            raise ValueError(
                "iceberg_metadata_prefix must not overlap data_path "
                f"(data_path={self.data_path!r}, "
                f"iceberg_metadata_prefix={self.iceberg_metadata_prefix!r})"
            )
        return self

    @model_validator(mode="after")
    def _resolve_catalog(self) -> DuckLakeIcebergSettings:
        """``catalog`` or the legacy ``glue_database``, never both."""
        if self.catalog is None:
            if self.glue_database is None:
                raise ValueError(
                    "incomplete configuration: catalog (or the legacy glue_database)"
                )
            self.catalog = CatalogSettings(
                type="glue", name="glue", namespace=self.glue_database
            )
        elif self.glue_database is not None:
            raise ValueError(
                "use catalog.namespace or the legacy glue_database, not both"
            )
        return self

    @property
    def iceberg_catalog(self) -> CatalogSettings:
        """The resolved catalog (always set after validation)."""
        assert self.catalog is not None  # set by _resolve_catalog
        return self.catalog

    @property
    def namespace(self) -> str:
        """Namespace of the published tables (``glue_database`` for Glue)."""
        return self.iceberg_catalog.namespace

    @model_validator(mode="after")
    def _alias_differs_from_metadata_catalog(self) -> DuckLakeIcebergSettings:
        if self.catalog_alias == self.metadata_catalog:
            raise ValueError("catalog_alias and metadata_catalog must be different")
        return self

    def redacted(self) -> dict[str, Any]:
        """Dump for `--json` and `doctor`, without the DSN host/port."""
        data = self.model_dump()
        data["catalog_dsn"] = _redact_dsn(self.catalog_dsn)
        data["catalog"] = self.iceberg_catalog.redacted()
        data["config_file"] = (
            str(self._config_file) if self._config_file is not None else None
        )
        return data


def _normalize_location(value: str) -> str:
    """Accept `s3://bucket/key` or an absolute local path; ends with `/`."""
    if value.startswith("s3://"):
        rest = value[len("s3://") :]
        bucket, _, key = rest.partition("/")
        if not bucket:
            raise ValueError(f"S3 location without bucket: {value!r}")
        if not key.strip("/"):
            raise ValueError(f"S3 location without key: {value!r}")
        return value.rstrip("/") + "/"
    if value.startswith("/"):
        return value.rstrip("/") + "/"
    raise ValueError(
        f"invalid location: {value!r}; use 's3://bucket/key' or an absolute path"
    )


def _overlaps(data_path: str, metadata_prefix: str) -> bool:
    a = data_path.rstrip("/") + "/"
    b = metadata_prefix.rstrip("/") + "/"
    return a.startswith(b) or b.startswith(a)


def _redact_dsn(dsn: str) -> str:
    """Redact the `host=` value (port included) in the DSN for safe output."""
    return re.sub(
        r"(?i)(host=)\S+",
        r"\1***",
        dsn,
    )


def resolve_config_file(explicit: Path | None) -> Path | None:
    """--config > DUCKLAKE_ICEBERG_CONFIG_FILE > CONFIG_SEARCH_PATH > None."""
    if explicit is not None:
        if not explicit.is_file():
            raise ConfigError(f"configuration file not found: {explicit}")
        return explicit

    env_value = os.environ.get(CONFIG_FILE_ENV)
    if env_value:
        candidate = Path(env_value)
        if not candidate.is_file():
            raise ConfigError(f"configuration file not found: {candidate}")
        return candidate

    for candidate in CONFIG_SEARCH_PATH:
        if candidate.is_file():
            return candidate
    return None


def load_settings(
    *,
    config_file: Path | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> DuckLakeIcebergSettings:
    """Load and validate the configuration.

    `overrides` receives only the CLI flags actually given (a `None` value is
    omitted, so a default does not override YAML).
    Raises `ConfigError` on any validation failure.
    """
    resolved = resolve_config_file(config_file)

    if resolved is not None:
        _ensure_valid_yaml(resolved)

    init_kwargs = {k: v for k, v in (overrides or {}).items() if v is not None}

    DuckLakeIcebergSettings._yaml_source = resolved
    try:
        settings = DuckLakeIcebergSettings(**init_kwargs)
    except ValidationError as exc:
        raise ConfigError(_format_validation_error(exc, resolved)) from exc
    finally:
        DuckLakeIcebergSettings._yaml_source = None

    # Keep the resolved file on the instance (the class channel is transient
    # and used only during construction by settings_customise_sources).
    object.__setattr__(settings, "_config_file", resolved)
    return settings


def _ensure_valid_yaml(path: Path) -> None:
    """Fail early with an actionable message if the YAML is malformed."""
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}: {exc}") from exc
    if content is not None and not isinstance(content, dict):
        raise ConfigError(f"invalid YAML in {path}: content must be a mapping")


def _format_validation_error(exc: ValidationError, path: Path | None) -> str:
    """Translate the pydantic error into the tool's messages."""
    first = exc.errors()[0]
    etype = first["type"]
    loc = first["loc"]
    field = str(loc[0]) if loc else "(model)"

    if etype == "missing":
        return f"incomplete configuration: {field}"
    if etype == "extra_forbidden":
        where = f" in {path}" if path is not None else ""
        return f"unknown key {field!r}{where}"
    # value_error / assertion_error carry the validator message.
    msg = first.get("msg", "")
    return msg.removeprefix("Value error, ") or f"invalid configuration: {field}"
