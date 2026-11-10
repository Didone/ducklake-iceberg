"""The Iceberg catalog the tables are published to.

Catalog-neutral helpers: the table identifier, the lock key component, the
supported-type gate and the construction of the PyIceberg ``Catalog`` from the
``catalog`` section of the settings. The publication itself (register/commit)
lives in the stage modules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ducklake_iceberg.config import SUPPORTED_CATALOG_TYPES
from ducklake_iceberg.errors import ConfigError

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

    from ducklake_iceberg.config import DuckLakeIcebergSettings

__all__ = [
    "catalog_key",
    "ensure_supported",
    "load",
    "normalize_table_name",
    "table_identifier",
]

#: catalog properties that identify where the catalog lives (lock key)
_LOCATION_PROPERTIES: tuple[str, ...] = ("uri", "warehouse", "glue.id", "glue.region")


def normalize_table_name(name: str) -> str:
    """``strip`` + lowercase + replace anything not ``[a-z0-9_]`` with ``_``.

    Kept for every catalog type in this release so that the published name of
    a table does not depend on the catalog it is published to.
    """
    lowered = name.strip().lower()
    return "".join(
        c if (c.isalnum() and c.isascii()) or c == "_" else "_" for c in lowered
    )


def table_identifier(
    settings: DuckLakeIcebergSettings, table_name: str
) -> tuple[str, ...]:
    """Identifier of the published table: ``(*namespace, table)``."""
    return (
        *settings.iceberg_catalog.namespace_parts,
        normalize_table_name(table_name),
    )


def catalog_key(settings: DuckLakeIcebergSettings) -> str:
    """Stable description of the target catalog, used in the lock key.

    Two configurations that publish to the same catalog produce the same key
    (the label ``name`` is deliberately left out); secrets are never part of
    it.
    """
    catalog = settings.iceberg_catalog
    parts: list[str] = [catalog.type]
    if catalog.type == "glue":
        region = catalog.properties.get("glue.region", settings.aws_region)
        profile = catalog.properties.get("glue.profile-name", settings.aws_profile)
        parts += [
            region,
            profile or "default",
            catalog.properties.get("glue.id", ""),
        ]
    else:
        parts += [catalog.properties.get(key, "") for key in _LOCATION_PROPERTIES]
    return "|".join(parts)


def ensure_supported(settings: DuckLakeIcebergSettings) -> None:
    """Reject catalog types this release cannot publish to (exit 1)."""
    catalog = settings.iceberg_catalog
    if catalog.type not in SUPPORTED_CATALOG_TYPES:
        supported = ", ".join(sorted(SUPPORTED_CATALOG_TYPES))
        raise ConfigError(
            f"catalog type {catalog.type!r} is not supported yet "
            f"(supported: {supported})",
            hint="set catalog.type to glue (or use the legacy glue_database)",
        )


def catalog_properties(settings: DuckLakeIcebergSettings) -> dict[str, str]:
    """Properties for PyIceberg's ``load_catalog``.

    The storage settings of the tool (region, profile, custom endpoint) are
    the defaults; anything in ``catalog.properties`` wins.
    """
    from ducklake_iceberg.aws import s3_credential_properties, s3_endpoint_properties

    catalog = settings.iceberg_catalog
    props: dict[str, str] = {"type": catalog.type}
    if catalog.type == "glue":
        props["glue.region"] = settings.aws_region
        if settings.aws_profile is not None:
            props["glue.profile-name"] = settings.aws_profile
    props["s3.region"] = settings.aws_region
    props.update(s3_credential_properties(settings.aws_region, settings.aws_profile))
    props.update(s3_endpoint_properties(settings.s3_endpoint))
    props.update(catalog.properties)
    return props


def load(settings: DuckLakeIcebergSettings) -> Catalog:
    """PyIceberg ``Catalog`` for the configured catalog."""
    from pyiceberg.catalog import load_catalog

    ensure_supported(settings)
    return load_catalog(settings.iceberg_catalog.name, **catalog_properties(settings))
