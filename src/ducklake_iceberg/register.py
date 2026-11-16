"""Validate a completed export and publish it to Glue; no DuckDB access."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ducklake_iceberg.errors import DivergenceError
from ducklake_iceberg.glue import (
    CommentProjection,
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


def _read_metadata(io: FileIO, metadata_location: str) -> TableMetadata:
    from pyiceberg.serializers import FromInputFile

    try:
        return FromInputFile.table_metadata(io.new_input(metadata_location))
    except FileNotFoundError as exc:
        raise PointerError(
            "completed export metadata is missing",
            hint="re-export with ducklake-iceberg sync",
        ) from exc


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


def registration_target(
    settings: DuckLakeIcebergSettings, table_name: str
) -> GlueTarget:
    """One target construction shared with pending-publication recovery."""
    return GlueTarget(
        database=settings.glue_database,
        table=normalize_table_name(table_name),
        region=settings.aws_region,
        profile=settings.aws_profile,
        catalog_id=None,
        s3_endpoint=settings.s3_endpoint,
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
    comments: CommentProjection | None = None,
    existing: dict[str, object] | None = None,
    existing_known: bool = False,
) -> RegisterResult:
    """Publish a validated handoff, reusing in-memory export metadata."""
    target = registration_target(settings, table_name)
    if handoff is not None and metadata is None and comments is not None:
        if not existing_known:
            existing = describe_table(target)
            existing_known = True
        if existing is not None:
            params = existing.get("Parameters")
            if (
                isinstance(params, dict)
                and params.get("table_type", "").upper() == "ICEBERG"
                and params.get("metadata_location") == handoff.metadata_location
            ):
                from ducklake_iceberg.glue import _handle_existing

                if comments.matches(existing):
                    raise TableAlreadyRegistered(
                        "metadata and comments already published",
                        metadata_location=handoff.metadata_location,
                    )

                def unavailable_metadata() -> TableMetadata:
                    raise DivergenceError(
                        "metadata pointer changed during comment repair"
                    )

                return _handle_existing(
                    target,
                    unavailable_metadata,
                    existing,
                    handoff.metadata_location,
                    dry_run=dry_run,
                    verify=False,
                    comments=comments,
                )
    pointer: ExportPointer | None
    if handoff is not None:
        resolved, pointer = handoff.metadata_location, handoff
    else:
        resolved, pointer = resolve_metadata_location(
            settings, schema_name, table_name, explicit=metadata_location
        )
    if metadata is None:
        metadata = _read_metadata(build_file_io(settings), resolved)
    properties = dict(metadata.properties)
    comments = CommentProjection.from_metadata(metadata)
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
        comments=comments,
        existing=existing,
        existing_known=existing_known,
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
