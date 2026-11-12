"""Validate a completed export and publish it to Glue; no DuckDB access."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ducklake_iceberg.errors import DivergenceError
from ducklake_iceberg.glue import (
    GlueTarget,
    RegisterResult,
    TableAlreadyRegistered,
    describe_table,
)
from ducklake_iceberg.glue import register as glue_register
from ducklake_iceberg.handoff import (
    ExportPointer,
    PointerError,
    normalize_table_name,
    read_pointer,
    table_location,
)
from ducklake_iceberg.metadata_io import KnownMetadataFileIO
from ducklake_iceberg.storage import build_file_io
from ducklake_iceberg.sync.state import PROP_SNAPSHOT_ID

if TYPE_CHECKING:
    from pyiceberg.io import FileIO
    from pyiceberg.table.metadata import TableMetadata

    from ducklake_iceberg.config import DuckLakeIcebergSettings


def resolve_metadata_location(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    explicit: str | None = None,
) -> tuple[str, ExportPointer | None]:
    """Use an explicit location or the completed handoff; never scan metadata."""
    if explicit is not None:
        return explicit, None
    location = table_location(
        settings.iceberg_metadata_prefix, settings.glue_database, table_name
    )
    pointer = read_pointer(build_file_io(settings), location)
    return pointer.metadata_location, pointer


def _read_metadata_properties(io: FileIO, metadata_location: str) -> dict[str, str]:
    from pyiceberg.serializers import FromInputFile

    input_file = io.new_input(metadata_location)
    if not input_file.exists():
        raise PointerError(
            f"pointer points to {metadata_location}, which does not exist",
            hint="re-export with ducklake-iceberg sync",
        )
    return dict(FromInputFile.table_metadata(input_file).properties)


def _check_coherence(
    properties: dict[str, str],
    *,
    pointer: ExportPointer | None,
    schema_name: str,
    table_name: str,
    metadata_location: str,
) -> None:
    meta_snapshot = properties.get(PROP_SNAPSHOT_ID)
    meta_table = properties.get("ducklake.table")
    if pointer is not None:
        if meta_snapshot is not None and meta_snapshot != str(
            pointer.ducklake_snapshot_id
        ):
            raise DivergenceError(
                f"pointer (snapshot {pointer.ducklake_snapshot_id}) does not "
                f"match {metadata_location} (snapshot {meta_snapshot})"
            )
    elif meta_table is not None and meta_table != table_name:
        raise DivergenceError(
            f"{metadata_location} belongs to table {meta_table!r}, not {table_name!r}"
        )


def run_register(
    settings: DuckLakeIcebergSettings,
    schema_name: str,
    table_name: str,
    *,
    metadata_location: str | None = None,
    create_database: bool = True,
    dry_run: bool = False,
    handoff: ExportPointer | None = None,
    metadata: TableMetadata | None = None,
) -> RegisterResult:
    """Publish a validated handoff, reusing in-memory export metadata."""
    target = GlueTarget(
        database=settings.glue_database,
        table=normalize_table_name(table_name),
        region=settings.aws_region,
        profile=settings.aws_profile,
        catalog_id=None,
        s3_endpoint=settings.s3_endpoint,
    )
    if handoff is not None and metadata is None:
        _raise_if_published(target, handoff.metadata_location)
    pointer: ExportPointer | None
    if handoff is not None:
        resolved, pointer = handoff.metadata_location, handoff
    else:
        resolved, pointer = resolve_metadata_location(
            settings, schema_name, table_name, explicit=metadata_location
        )
    properties = (
        dict(metadata.properties)
        if metadata is not None
        else _read_metadata_properties(build_file_io(settings), resolved)
    )
    _check_coherence(
        properties,
        pointer=pointer,
        schema_name=schema_name,
        table_name=table_name,
        metadata_location=resolved,
    )
    return glue_register(
        target,
        resolved,
        create_database=create_database,
        dry_run=dry_run,
        data_path=settings.data_path,
        metadata=metadata,
        file_io=(
            KnownMetadataFileIO(
                build_file_io(settings),
                resolved,
                metadata.model_dump_json(by_alias=True, exclude_none=True).encode(
                    "utf-8"
                ),
            )
            if metadata is not None
            else None
        ),
    )


def _raise_if_published(target: GlueTarget, metadata_location: str) -> None:
    table = describe_table(target)
    if table is None:
        return
    params = table.get("Parameters")
    if not isinstance(params, dict):
        return
    if (
        str(params.get("table_type", "")).upper() == "ICEBERG"
        and params.get("metadata_location") == metadata_location
    ):
        raise TableAlreadyRegistered(
            f"{target.database}.{target.table} already published at "
            f"{metadata_location}",
            metadata_location=metadata_location,
        )
