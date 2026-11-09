"""Reading deletes.

For a table and a DuckLake snapshot S, produces a normalized collection of
positional deletions in the format (data_file_path, pos, snapshot_id, origin),
reading both representations DuckLake uses: Parquet delete files referenced
in lake_meta.ducklake_delete_file and inlined deletions in the dynamic table
ducklake_inlined_delete_<table_id>.
"""

from __future__ import annotations

import dataclasses
import enum
import logging
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.compute as pc

from ducklake_iceberg.catalog.files import PathLayers, compose_path
from ducklake_iceberg.doctor.model import quote_ident
from ducklake_iceberg.errors import (
    CompatibilityError,
    DivergenceError,
    DuckLakeIcebergError,
)

if TYPE_CHECKING:
    from ducklake_iceberg.catalog.connection import DuckLakeConnection

LOG = logging.getLogger(__name__)

# DuckLake field IDs for delete files
_DUCKLAKE_FILE_PATH_ID: Final[int] = 2147483646
_DUCKLAKE_POS_ID: Final[int] = 2147483645
_DUCKLAKE_SNAPSHOT_ID: Final[int] = 2147483539

# Prefix of the synthetic path given to an inlined deletion whose ``file_id``
# does not exist in ``ducklake_data_file``: it never belongs to
# ``live_data_files``, so the visibility step classifies it as dangling
# (``data_file_live = false``).
ORPHAN_DATA_FILE_PREFIX: Final[str] = "ducklake-orphan-data-file:"

# Arrow schema of DeleteSet.positions (stable; shared contract with the
# visibility and writer steps). ``source_id`` is the ``delete_file_id`` for
# ``DELETE_FILE`` and the ``file_id`` (``data_file_id``) for ``INLINED``, which
# has no delete file of its own.
POSITIONS_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("data_file_path", pa.large_string(), nullable=False),
        pa.field("pos", pa.int64(), nullable=False),
        pa.field("snapshot_id", pa.int64(), nullable=False),
        pa.field(
            "origin",
            pa.dictionary(pa.int8(), pa.string()),
            nullable=False,
        ),
        pa.field("source_id", pa.int64(), nullable=False),
    ]
)


# -- exceptions ---------------------------------------------------------------


class PuffinDeleteFormatError(CompatibilityError):
    """Puffin delete file is not readable by Athena in v2."""

    def __init__(self, path: str) -> None:
        super().__init__(
            f"delete file {path} is in puffin format; "
            "Athena cannot read deletion vectors in Iceberg v2",
            hint="Use 'ducklake-iceberg checkpoint' with --materialize-deletes "
            "or rewrite the deletes in DuckLake",
        )


class UnknownDeleteFormatError(CompatibilityError):
    """Unsupported delete file format."""

    def __init__(self, format_name: str, path: str) -> None:
        super().__init__(
            f"unsupported delete file format: {format_name} in {path}",
            hint="Check the DuckLake version or open an issue",
        )


class EncryptedDeleteFileError(CompatibilityError):
    """Encrypted delete file is not supported (out of scope)."""

    def __init__(self, path: str) -> None:
        super().__init__(
            f"encrypted delete file is not supported: {path}",
            hint="File encryption is out of scope",
        )


class InlinedDeletesPendingError(CompatibilityError):
    """Pending inlined deletions without --include-inlined-deletes."""

    def __init__(self, table_name: str, count: int) -> None:
        super().__init__(
            f"{count} pending inlined deletions in table {table_name}",
            hint="Run 'ducklake-iceberg checkpoint' to materialize them, "
            "or use --include-inlined-deletes",
        )


class DeleteFileCountMismatchError(DivergenceError):
    """Row count in the file does not match the catalog."""

    def __init__(self, path: str, catalog_count: int, file_count: int) -> None:
        super().__init__(
            f"delete file {path}: catalog says {catalog_count} rows, "
            f"file has {file_count}",
            hint="Check for a race with DuckLake maintenance",
        )


class DeleteFileEnumerationMismatchError(DivergenceError):
    """Set of delete files differs between catalog and ducklake_list_files."""

    def __init__(self, only_catalog: int, only_function: int) -> None:
        super().__init__(
            f"mismatch between ducklake_delete_file and ducklake_list_files: "
            f"{only_catalog} only in the catalog, {only_function} only in the function",
            hint="Rerun 'ducklake-iceberg doctor' or report a range-handling bug",
        )


class DeleteFileSchemaError(DuckLakeIcebergError):
    """Physical schema of the delete file is not the expected one."""

    def __init__(self, path: str, schema_description: str) -> None:
        super().__init__(
            f"unexpected schema in {path}: {schema_description}",
            hint="Pin the DuckLake version or review the delete file layout",
        )


class InvalidDeletePositionError(DuckLakeIcebergError):
    """Delete position outside the data file range."""

    def __init__(self, pos: int, data_file: str, record_count: int) -> None:
        super().__init__(
            f"position {pos} outside [0,{record_count}) in {data_file}",
            hint="Report the error; do not export this table",
        )


# -- data contracts -----------------------------------------------------


class DeleteOrigin(enum.StrEnum):
    DELETE_FILE = "delete_file"  # ducklake_delete_file, format='parquet'
    INLINED = "inlined"  # ducklake_inlined_delete_<table_id>


@dataclasses.dataclass(frozen=True, slots=True)
class DuckLakeDeleteFile:
    delete_file_id: int
    data_file_id: int
    path: str  # absolute path, already composed
    file_format: str  # 'parquet' | 'puffin' | other
    delete_count: int
    file_size_bytes: int
    footer_size: int | None
    row_group_count: int | None
    begin_snapshot: int
    end_snapshot: int | None
    partial_max: int | None
    encryption_key: str | None
    data_file_path: str | None  # None when the data file is not live
    data_record_count: int | None
    data_file_live: bool
    has_snapshot_column: bool  # probed physical schema


@dataclasses.dataclass(frozen=True, slots=True)
class InlinedDeleteTable:
    table_name: str  # ducklake_inlined_delete_<table_id>
    rows_total: int
    rows_visible: int  # begin_snapshot <= S


@dataclasses.dataclass(frozen=True, slots=True)
class DeleteSet:
    table_id: int
    snapshot_id: int
    positions: pa.Table  # POSITIONS_SCHEMA, unfiltered
    files: tuple[DuckLakeDeleteFile, ...]
    inlined: InlinedDeleteTable | None
    warnings: tuple[str, ...]

    @property
    def is_empty(self) -> bool:
        """Return True if there are no deletions."""
        return len(self.positions) == 0

    def positions_by_data_file(self) -> Mapping[str, pa.Table]:
        """Group positions by data_file_path."""
        if self.is_empty:
            return {}

        result: dict[str, pa.Table] = {}
        for data_file_path in self.positions["data_file_path"].unique():
            mask = pc.equal(
                self.positions["data_file_path"],
                data_file_path,
            )
            result[data_file_path.as_py()] = self.positions.filter(mask)

        return result


# -- read functions -----------------------------------------------------


def _compose_delete_path(
    base_path: str,
    schema_path: str | None,
    schema_path_is_relative: bool,
    table_path: str | None,
    table_path_is_relative: bool,
    delete_path: str,
    delete_path_is_relative: bool,
) -> str:
    """Compose the absolute path with the catalog path helper."""
    layers = PathLayers(
        data_path=base_path,
        schema_path=schema_path,
        schema_path_is_relative=schema_path_is_relative,
        table_path=table_path,
        table_path_is_relative=table_path_is_relative,
    )
    return compose_path(layers, delete_path, delete_path_is_relative)


def list_delete_files(
    con: DuckLakeConnection,
    table_id: int,
    snapshot_id: int,
    data_path: str,
    schema_path: str | None,
    schema_path_is_relative: bool,
    table_path: str | None,
    table_path_is_relative: bool,
) -> list[DuckLakeDeleteFile]:
    """Enumerate the delete files live at the snapshot."""
    meta = quote_ident(con.meta)
    sql = f"""
        SELECT
            del.delete_file_id,
            del.data_file_id,
            del.path                AS delete_path,
            del.path_is_relative    AS delete_path_is_relative,
            del.format              AS delete_format,
            del.delete_count,
            del.file_size_bytes     AS delete_file_size_bytes,
            del.footer_size         AS delete_footer_size,
            del.row_group_count     AS delete_row_group_count,
            del.encryption_key      AS delete_encryption_key,
            del.begin_snapshot,
            del.end_snapshot,
            del.partial_max,
            df.path                 AS data_path,
            df.path_is_relative     AS data_path_is_relative,
            df.record_count         AS data_record_count,
            (df.data_file_id IS NOT NULL) AS data_file_live
        FROM {meta}.ducklake_delete_file AS del
        LEFT JOIN {meta}.ducklake_data_file AS df
               ON df.data_file_id = del.data_file_id
              AND df.table_id     = del.table_id
              AND $snapshot_id >= df.begin_snapshot
              AND (df.end_snapshot IS NULL OR $snapshot_id < df.end_snapshot)
        WHERE del.table_id = $table_id
          AND $snapshot_id >= del.begin_snapshot
          AND (del.end_snapshot IS NULL OR $snapshot_id < del.end_snapshot)
        ORDER BY del.delete_file_id;
    """

    params = {"table_id": table_id, "snapshot_id": snapshot_id}
    rows = con.query_arrow(sql, params)

    files = []
    for i in range(len(rows)):
        delete_path_composed = _compose_delete_path(
            data_path,
            schema_path,
            schema_path_is_relative,
            table_path,
            table_path_is_relative,
            rows["delete_path"][i].as_py(),
            rows["delete_path_is_relative"][i].as_py(),
        )

        data_path_composed: str | None = None
        if rows["data_file_live"][i].as_py():
            data_path_composed = _compose_delete_path(
                data_path,
                schema_path,
                schema_path_is_relative,
                table_path,
                table_path_is_relative,
                rows["data_path"][i].as_py(),
                rows["data_path_is_relative"][i].as_py(),
            )

        files.append(
            DuckLakeDeleteFile(
                delete_file_id=rows["delete_file_id"][i].as_py(),
                data_file_id=rows["data_file_id"][i].as_py(),
                path=delete_path_composed,
                file_format=rows["delete_format"][i].as_py(),
                delete_count=rows["delete_count"][i].as_py(),
                file_size_bytes=rows["delete_file_size_bytes"][i].as_py(),
                footer_size=rows["delete_footer_size"][i].as_py(),
                row_group_count=rows["delete_row_group_count"][i].as_py(),
                begin_snapshot=rows["begin_snapshot"][i].as_py(),
                end_snapshot=rows["end_snapshot"][i].as_py(),
                partial_max=rows["partial_max"][i].as_py(),
                encryption_key=rows["delete_encryption_key"][i].as_py(),
                data_file_path=data_path_composed,
                data_record_count=rows["data_record_count"][i].as_py(),
                data_file_live=rows["data_file_live"][i].as_py(),
                has_snapshot_column=False,  # filled in when the file is read
            )
        )

    return files


def assert_supported(files: Sequence[DuckLakeDeleteFile]) -> None:
    """Validate that the delete files are supported."""
    for f in files:
        if f.encryption_key is not None:
            raise EncryptedDeleteFileError(f.path)
        if f.file_format == "puffin":
            raise PuffinDeleteFormatError(f.path)
        if f.file_format not in {"parquet", "puffin"}:
            raise UnknownDeleteFormatError(f.file_format, f.path)


def _probe_delete_schema(con: DuckLakeConnection, path: str) -> tuple[bool, str]:
    """Probe the physical schema of the delete file.

    Returns (has_snapshot_column, schema_description).
    """

    sql = """
        SELECT name, type, field_id
        FROM parquet_schema($path)
        WHERE name <> 'duckdb_schema'
        ORDER BY name;
    """

    schema = con.query_arrow(sql, {"path": path})

    # Validate the expected schema
    expected_file_path = False
    expected_pos = False
    has_snapshot = False

    schema_parts = []
    for i in range(len(schema)):
        name = schema["name"][i].as_py()
        type_name = schema["type"][i].as_py()
        field_id = schema["field_id"][i].as_py()

        schema_parts.append(f"{name}:{type_name}#{field_id}")

        if name == "file_path" and field_id == _DUCKLAKE_FILE_PATH_ID:
            expected_file_path = True
        elif name == "pos" and field_id == _DUCKLAKE_POS_ID:
            expected_pos = True
        elif (
            name == "_ducklake_internal_snapshot_id"
            and field_id == _DUCKLAKE_SNAPSHOT_ID
        ):
            has_snapshot = True

    schema_description = ", ".join(schema_parts)

    if not expected_file_path or not expected_pos:
        raise DeleteFileSchemaError(path, schema_description)

    return has_snapshot, schema_description


def read_delete_file(
    con: DuckLakeConnection,
    f: DuckLakeDeleteFile,
) -> tuple[pa.Table, bool]:
    """Read the positions of a delete file.

    Returns (positions_table, has_snapshot_column).
    """
    # Probe the schema first
    has_snapshot_column, _ = _probe_delete_schema(con, f.path)

    # Read with the appropriate SQL; values always via named parameters
    snapshot_expr = (
        "CAST(_ducklake_internal_snapshot_id AS BIGINT)"
        if has_snapshot_column
        else "CAST($begin_snapshot AS BIGINT)"
    )
    sql = f"""
        SELECT
            CAST(file_path AS VARCHAR)      AS file_path,
            CAST(pos AS BIGINT)             AS pos,
            {snapshot_expr}                 AS snapshot_id,
            CAST($delete_file_id AS BIGINT) AS source_id
        FROM read_parquet($path);
    """
    params: dict[str, object] = {"path": f.path, "delete_file_id": f.delete_file_id}
    if not has_snapshot_column:
        params["begin_snapshot"] = f.begin_snapshot

    positions = con.query_arrow(sql, params)

    # Validations
    if len(positions) != f.delete_count:
        raise DeleteFileCountMismatchError(f.path, f.delete_count, len(positions))

    # Validate that file_path and pos are not null
    if positions["file_path"].null_count > 0:
        raise DuckLakeIcebergError(f"null file_path in {f.path}")
    if positions["pos"].null_count > 0:
        raise DuckLakeIcebergError(f"null pos in {f.path}")

    # Validate pos is within range (only if the data file is live)
    if f.data_file_live and f.data_record_count is not None and len(positions):
        min_pos = pc.min(positions["pos"]).as_py()
        if min_pos < 0:
            raise InvalidDeletePositionError(
                min_pos,
                f.data_file_path or "<unknown>",
                f.data_record_count,
            )
        max_pos = pc.max(positions["pos"]).as_py()
        if max_pos >= f.data_record_count:
            raise InvalidDeletePositionError(
                max_pos,
                f.data_file_path or "<unknown>",
                f.data_record_count,
            )

    return positions, has_snapshot_column


def _check_delete_file_enumeration(
    con: DuckLakeConnection,
    catalog: str,
    schema_name: str,
    table_name: str,
    snapshot_id: int,
    composed_paths: set[str],
) -> None:
    """Check the set of delete files against ducklake_list_files."""

    sql = """
        SELECT delete_file
        FROM ducklake_list_files(
                 $catalog, $table_name,
                 schema => $schema_name,
                 snapshot_version => $snapshot_id)
        WHERE delete_file IS NOT NULL;
    """

    params = {
        "catalog": catalog,
        "table_name": table_name,
        "schema_name": schema_name,
        "snapshot_id": snapshot_id,
    }
    function_result = con.query_arrow(sql, params)
    function_paths = {
        function_result["delete_file"][i].as_py() for i in range(len(function_result))
    }

    only_catalog = len(composed_paths - function_paths)
    only_function = len(function_paths - composed_paths)

    if only_catalog > 0 or only_function > 0:
        raise DeleteFileEnumerationMismatchError(only_catalog, only_function)


def _list_data_file_paths(
    con: DuckLakeConnection,
    table_id: int,
    data_path: str,
    schema_path: str | None,
    schema_path_is_relative: bool,
    table_path: str | None,
    table_path_is_relative: bool,
) -> dict[int, str]:
    """Map ``data_file_id`` -> composed path of **all** data files.

    Includes data files outside the snapshot (``end_snapshot <= S``): a
    deletion that references them is not discarded here; it reaches the
    visibility step with the real path and is classified as dangling
    (``data_file_live = false``).
    """
    meta = quote_ident(con.meta)
    sql = f"""
        SELECT data_file_id, path, path_is_relative
        FROM {meta}.ducklake_data_file
        WHERE table_id = $table_id;
    """
    rows = con.query_arrow(sql, {"table_id": table_id}).to_pylist()
    return {
        int(r["data_file_id"]): _compose_delete_path(
            data_path,
            schema_path,
            schema_path_is_relative,
            table_path,
            table_path_is_relative,
            r["path"],
            bool(r["path_is_relative"]),
        )
        for r in rows
    }


def _origin_column(origin: DeleteOrigin, length: int) -> pa.Array:
    """Constant ``origin`` column, in the dictionary type of POSITIONS_SCHEMA."""
    return pa.array(
        [origin.value] * length,
        type=pa.dictionary(pa.int8(), pa.string()),
    )


def _inlined_delete_table_name(table_id: int) -> str:
    """Physical name of the dynamic table; ``int()`` guarantees only digits in SQL."""
    return f"ducklake_inlined_delete_{int(table_id)}"


def read_inlined_deletes(
    con: DuckLakeConnection,
    table_id: int,
    snapshot_id: int,
    data_file_path_by_id: Mapping[int, str],
) -> tuple[pa.Table, InlinedDeleteTable | None]:
    """Read inlined deletions.

    A ``file_id`` that does not resolve in ``data_file_path_by_id`` is an
    orphan delete: the row is kept with a synthetic path
    (``ORPHAN_DATA_FILE_PREFIX``), which is never a live data file, and the
    visibility step decides its fate. ``source_id`` is the ``file_id`` itself
    (there is no delete file).

    Returns (positions_table, inlined_table_info).
    """
    table_name = _inlined_delete_table_name(table_id)

    # Detect the dynamic table; values via named parameters
    sql_detect = """
        SELECT table_name
        FROM duckdb_tables()
        WHERE database_name = $meta_catalog
          AND table_name    = $table_name;
    """
    result = con.query_arrow(
        sql_detect, {"meta_catalog": con.meta, "table_name": table_name}
    )
    if len(result) == 0:
        # There is no inlined table
        return POSITIONS_SCHEMA.empty_table(), None

    # Count total and visible rows
    meta = quote_ident(con.meta)
    sql_count_total = f"SELECT count(*) AS total FROM {meta}.{table_name};"
    rows_total = int(con.query_arrow(sql_count_total)["total"][0].as_py())

    sql_visible = f"""
        SELECT
            CAST(file_id AS BIGINT)        AS file_id,
            CAST(row_id AS BIGINT)         AS pos,
            CAST(begin_snapshot AS BIGINT) AS snapshot_id
        FROM {meta}.{table_name}
        WHERE begin_snapshot <= $snapshot_id;
    """
    inlined = con.query_arrow(sql_visible, {"snapshot_id": snapshot_id})
    rows_visible = len(inlined)
    info = InlinedDeleteTable(
        table_name=table_name,
        rows_total=rows_total,
        rows_visible=rows_visible,
    )

    if rows_visible == 0:
        return POSITIONS_SCHEMA.empty_table(), info

    if inlined["file_id"].null_count > 0 or inlined["pos"].null_count > 0:
        raise DuckLakeIcebergError(f"null file_id or row_id in {table_name}")

    # Resolve file_id to a path; orphans stay marked for the visibility step
    file_ids: list[int] = inlined["file_id"].to_pylist()
    orphans = sorted({fid for fid in file_ids if fid not in data_file_path_by_id})
    if orphans:
        LOG.warning(
            "%d data_file_id without a data file in the catalog in %s: %s",
            len(orphans),
            table_name,
            orphans[:10],
        )
    paths = [
        data_file_path_by_id.get(fid, f"{ORPHAN_DATA_FILE_PREFIX}{fid}")
        for fid in file_ids
    ]

    positions_table = pa.table(
        {
            "data_file_path": pa.array(paths, type=pa.large_string()),
            "pos": inlined["pos"],
            "snapshot_id": inlined["snapshot_id"],
            "origin": _origin_column(DeleteOrigin.INLINED, rows_visible),
            "source_id": inlined["file_id"],
        },
        schema=POSITIONS_SCHEMA,
    )
    return positions_table, info


def _delete_file_positions(
    f: DuckLakeDeleteFile,
    pos_table: pa.Table,
    data_file_path_by_id: Mapping[int, str],
    warnings: list[str],
) -> pa.Table:
    """Project the positions of a delete file onto POSITIONS_SCHEMA.

    The association is by ``data_file_id`` (the catalog is authoritative); the
    Parquet ``file_path`` column is only a cross-check (warning on mismatch)
    and a last resort when the data file vanished from the catalog.
    """
    n = len(pos_table)
    catalog_path = f.data_file_path or data_file_path_by_id.get(f.data_file_id)
    file_paths = pc.cast(pos_table["file_path"], pa.large_string())
    if catalog_path is None:
        data_file_paths: pa.Array | pa.ChunkedArray = file_paths
    else:
        data_file_paths = pa.array([catalog_path] * n, type=pa.large_string())
        if pc.any(pc.not_equal(file_paths, catalog_path)).as_py():
            warnings.append(
                f"file_path of delete file {f.path} differs from the catalog "
                f"({catalog_path}); the catalog wins"
            )
    return pa.table(
        {
            "data_file_path": data_file_paths,
            "pos": pos_table["pos"],
            "snapshot_id": pos_table["snapshot_id"],
            "origin": _origin_column(DeleteOrigin.DELETE_FILE, n),
            "source_id": pos_table["source_id"],
        },
        schema=POSITIONS_SCHEMA,
    )


def read_deletes(
    con: DuckLakeConnection,
    table_id: int,
    snapshot_id: int,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    data_path: str,
    schema_path: str | None,
    schema_path_is_relative: bool,
    table_path: str | None,
    table_path_is_relative: bool,
    *,
    include_inlined: bool = False,
) -> DeleteSet:
    """Read all deletes for a table/snapshot.

    Delete files are read sequentially: the DuckDB connection is a single
    one, is not thread-safe, and lives inside the catalog read transaction.

    Args:
        con: DuckDB connection with the catalog attached
        table_id: Table ID in the catalog
        snapshot_id: Target snapshot
        catalog_name: DuckLake catalog name (for ducklake_list_files)
        schema_name: Schema name
        table_name: Table name
        data_path: Data base path
        schema_path: Schema path (may be None)
        schema_path_is_relative: Whether schema_path is relative
        table_path: Table path (may be None)
        table_path_is_relative: Whether table_path is relative
        include_inlined: Whether to include inlined deletions without
            materializing them
    """
    warnings: list[str] = []
    layers = (
        data_path,
        schema_path,
        schema_path_is_relative,
        table_path,
        table_path_is_relative,
    )

    # 1. Enumerate delete files
    files = list_delete_files(con, table_id, snapshot_id, *layers)

    # 2. Validate support (before any I/O)
    assert_supported(files)

    # 3. Check against ducklake_list_files
    if files:
        _check_delete_file_enumeration(
            con,
            catalog_name,
            schema_name,
            table_name,
            snapshot_id,
            {f.path for f in files},
        )

    # Map of ALL data files of the table (live or not): resolves inlined
    # deletions and delete files whose data file left the snapshot
    data_file_path_by_id = _list_data_file_paths(con, table_id, *layers)

    # 4. Read delete files
    positions_tables: list[pa.Table] = []
    updated_files: list[DuckLakeDeleteFile] = []
    seen_data_file_ids: set[int] = set()
    for f in files:
        if f.delete_count == 0:
            warnings.append(f"delete file {f.path} has delete_count = 0")
        if f.data_file_id in seen_data_file_ids:
            warnings.append(
                f"more than one delete file for data_file_id {f.data_file_id} "
                f"in snapshot {snapshot_id}"
            )
        seen_data_file_ids.add(f.data_file_id)

        pos_table, has_snapshot = read_delete_file(con, f)
        updated_files.append(dataclasses.replace(f, has_snapshot_column=has_snapshot))
        if len(pos_table) > 0:
            positions_tables.append(
                _delete_file_positions(f, pos_table, data_file_path_by_id, warnings)
            )
    files = updated_files

    # 5. Read inlined deletions
    inlined_positions, inlined_table_info = read_inlined_deletes(
        con, table_id, snapshot_id, data_file_path_by_id
    )
    if inlined_table_info is not None and inlined_table_info.rows_visible > 0:
        if not include_inlined:
            raise InlinedDeletesPendingError(
                f"{schema_name}.{table_name}",
                inlined_table_info.rows_visible,
            )
        positions_tables.append(inlined_positions)
        warnings.append(
            f"{inlined_table_info.rows_visible} inlined deletions included "
            "without materialization"
        )

    # 6. Concatenate all positions; only count duplicates
    if positions_tables:
        all_positions = pa.concat_tables(positions_tables)
        grouped = all_positions.group_by(["data_file_path", "pos"]).aggregate(
            [("snapshot_id", "count")]
        )
        duplicates = pc.sum(pc.greater(grouped["snapshot_id_count"], 1)).as_py()
        if duplicates:
            warnings.append(f"{duplicates} duplicate positions found")
    else:
        all_positions = POSITIONS_SCHEMA.empty_table()

    return DeleteSet(
        table_id=table_id,
        snapshot_id=snapshot_id,
        positions=all_positions,
        files=tuple(files),
        inlined=inlined_table_info,
        warnings=tuple(warnings),
    )
