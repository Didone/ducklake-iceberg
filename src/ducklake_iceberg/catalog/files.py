"""Enumeration of live files at the target snapshot.

Produces the exact list of live Parquet files, with absolute S3 paths and the
catalog fields the manifest writer needs, using ``ducklake_list_files`` as the
**primary source** (public, stable API) and ``ducklake_data_file`` as the
**complementary source** (the fields the function does not return:
``record_count``, ``partition_id``, ``partial_max``, ``mapping_id``).

The **cross-check** by canonical path is the central defense, and the only
cheap one, against the most dangerous class of bug in the project - exporting
too few files (rows vanish in Athena) or too many (removed rows reappear).
There is no flag to disable it.

Three reads on the apparently-empty path distinguish a truly empty table from
one masked by *data inlining*: an export over pending inlined data produces a
syntactically valid Iceberg table with no rows at all.

The pure functions (``compose_path``, ``canonical``, ``reconcile_files``,
``classify_empty`` and the blocker ones) do not touch the database and are
testable with literal rows; only ``read_path_layers``, ``list_files``,
``read_data_files`` and ``enumerate_files`` perform I/O.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, cast

from ducklake_iceberg.checkpoint.detect import detect_inlined
from ducklake_iceberg.checkpoint.model import TableRef
from ducklake_iceberg.doctor.model import Finding, Severity, quote_ident

if TYPE_CHECKING:
    import duckdb

    from ducklake_iceberg.doctor.model import DoctorTarget

#: Internal files are always Parquet in specification 1.1.
_PARQUET: Final[str] = "parquet"

#: Maximum number of paths listed per side in the divergence report.
_MAX_REPORTED: Final[int] = 20

_RULE_ID: Final[str] = "file-enumeration"


# -- data contracts -----------------------------------------------------


@dataclass(frozen=True, slots=True)
class LiveDataFile:
    """A live Parquet file at the target snapshot, with both sources merged."""

    data_file_id: int
    path: str  # absolute, as returned by ducklake_list_files
    canonical_path: str  # normalized form, for comparison only
    file_size_in_bytes: int
    footer_size: int | None
    record_count: int
    file_format: str  # always 'parquet' in 1.1
    partition_id: int | None
    partial_max: int | None
    mapping_id: int | None
    row_id_start: int | None
    row_group_count: int | None
    begin_snapshot: int  # snapshot that added the file
    file_order: int | None
    delete_file: LiveDeleteFile | None = None  # live deletes of the file


@dataclass(frozen=True, slots=True)
class LiveDeleteFile:
    """DuckLake delete file live at the target snapshot, paired with its data file.

    DuckLake keeps **one** live delete file per data file, cumulative: a new
    ``DELETE`` writes another file with all the positions and replaces the
    catalog row (the original ``begin_snapshot`` is kept). Hence the path
    identifies the content, and the delete delta is by path, not by snapshot
    interval.
    """

    delete_file_id: int
    path: str  # absolute, as returned by ducklake_list_files
    canonical_path: str
    file_size_in_bytes: int
    delete_count: int
    file_format: str  # 'parquet'; 'puffin' is a blocker in v2


@dataclass(frozen=True, slots=True)
class PathLayers:
    data_path: str  # ducklake_metadata, key='data_path'
    schema_path: str | None
    schema_path_is_relative: bool
    table_path: str | None
    table_path_is_relative: bool


class EmptyReason(StrEnum):
    TRULY_EMPTY = "truly-empty"  # table with no rows at all
    INLINED_PENDING = "inlined"  # masked by data inlining
    UNKNOWN = "unknown"  # could not be decided


@dataclass(frozen=True, slots=True)
class FileSet:
    files: tuple[LiveDataFile, ...]
    layers: PathLayers
    min_begin_snapshot: int | None  # None when there are no files
    total_size_in_bytes: int
    total_record_count: int
    empty_reason: EmptyReason | None


# -- exceptions ---------------------------------------------------------------


class FileSetDivergence(Exception):  # noqa: N818 - name is part of the public contract
    """Path sets differ between the two sources (exit 3)."""

    exit_code = 3

    def __init__(
        self,
        only_in_list_files: Sequence[str],
        only_in_data_file: Sequence[tuple[int, str]],
        *,
        n_list_files: int,
        n_data_file: int,
        table: str,
        snapshot_id: int,
        duplicate: str | None = None,
    ) -> None:
        self.only_in_list_files = tuple(only_in_list_files)
        self.only_in_data_file = tuple(only_in_data_file)
        self.n_list_files = n_list_files
        self.n_data_file = n_data_file
        self.table = table
        self.snapshot_id = snapshot_id
        self.duplicate = duplicate
        super().__init__(self.render())

    def render(self) -> str:
        if self.duplicate is not None:
            return f"repeated path in the enumeration: {self.duplicate}"
        lines = [
            f"file enumeration divergence for {self.table} "
            f"@ snapshot {self.snapshot_id}",
            f"  ducklake_list_files : {self.n_list_files} files",
            f"  ducklake_data_file  : {self.n_data_file} files",
        ]
        lines.append(
            f"  only in list_files ({len(self.only_in_list_files)}):"
            + _render_list(p for p in self.only_in_list_files)
        )
        lines.append(
            f"  only in data_file  ({len(self.only_in_data_file)}):"
            + _render_list(
                f"data_file_id={fid}  {path}" for fid, path in self.only_in_data_file
            )
        )
        if self.only_in_data_file and not self.only_in_list_files:
            lines.append(
                f"we would export {len(self.only_in_data_file)} file(s) too many; "
                f"export aborted"
            )
        elif self.only_in_list_files and not self.only_in_data_file:
            lines.append(
                f"we would export {len(self.only_in_list_files)} file(s) too "
                f"few; export aborted"
            )
        if (
            self.only_in_list_files
            and self.only_in_data_file
            and len(self.only_in_list_files) == len(self.only_in_data_file)
        ):
            lines.append(
                "hint: equal sizes and no common path suggest a path "
                "composition error, not an interval error"
            )
        return "\n".join(lines)

    def to_json(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "snapshot_id": self.snapshot_id,
            "n_list_files": self.n_list_files,
            "n_data_file": self.n_data_file,
            "only_in_list_files": list(self.only_in_list_files),
            "only_in_data_file": [
                {"data_file_id": fid, "path": path}
                for fid, path in self.only_in_data_file
            ],
            "duplicate": self.duplicate,
        }


class FileEnumerationError(Exception):
    """Inconsistent catalog during enumeration (exit 1)."""

    exit_code = 1


class FileEnumerationBlocked(Exception):  # noqa: N818 - public name
    """Compatibility blocker detected during enumeration (exit 2)."""

    exit_code = 2

    def __init__(self, findings: Sequence[Finding]) -> None:
        self.findings = tuple(findings)
        detail = "; ".join(f.detail for f in self.findings) or "enumeration blocked"
        super().__init__(detail)


def _render_list(items: Any) -> str:
    rendered = list(items)
    if not rendered:
        return "  (none)"
    shown = rendered[:_MAX_REPORTED]
    text = "".join(f"\n    {item}" for item in shown)
    if len(rendered) > _MAX_REPORTED:
        text += f"\n    ... and {len(rendered) - _MAX_REPORTED} more"
    return text


# -- path composition and canonicalization (pure) ---------------------------


def _join(prefix: str, suffix: str) -> str:
    """Concatenate preserving the scheme; never ``posixpath.join``/``urljoin``.

    ``posixpath.join`` discards the prefix when the suffix starts with '/';
    ``urljoin`` does not treat 's3://' as hierarchical.
    """
    return f"{prefix.rstrip('/')}/{suffix.lstrip('/')}"


def compose_path(layers: PathLayers, path: str, is_relative: bool) -> str:
    """Compose the absolute path; an absolute path stops the composition."""
    if not is_relative:
        return path
    if layers.table_path and not layers.table_path_is_relative:
        return _join(layers.table_path, path)
    prefix = layers.data_path
    if layers.schema_path:
        prefix = (
            layers.schema_path
            if not layers.schema_path_is_relative
            else _join(prefix, layers.schema_path)
        )
    if layers.table_path:
        prefix = _join(prefix, layers.table_path)
    return _join(prefix, path)


_MULTI_SLASH: Final[re.Pattern[str]] = re.compile(r"/{2,}")
_DOT_SEGMENT: Final[re.Pattern[str]] = re.compile(r"(?:^|/)\./")


def canonical(uri: str) -> str:
    """Normalize just enough to compare sets.

    It does **not** resolve ``..``, normalize percent-encoding or lowercase the
    path: in S3 the key is case-sensitive and ``%2F`` is not equivalent to ``/``.
    """
    scheme, sep, rest = uri.partition("://")
    if not sep:
        return uri.rstrip("/")
    authority, _, key = rest.partition("/")
    key = _MULTI_SLASH.sub("/", key)
    key = _DOT_SEGMENT.sub("/", key).lstrip("/")
    return f"{scheme.lower()}://{authority}/{key}"


# -- catalog reads (I/O) ---------------------------------------------


def _q(
    con: duckdb.DuckDBPyConnection, sql: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    used = {name: value for name, value in params.items() if f"${name}" in sql}
    relation = cast("Any", con).sql(sql, params=used)
    table = relation.to_arrow_table()
    rows: list[dict[str, Any]] = table.to_pylist()
    return rows


_LIST_FILES_SQL: Final[str] = (
    "FROM ducklake_list_files("
    "  $catalog, $table_name, schema => $schema_name, "
    "  snapshot_version => $snapshot_id"
    ")"
)

_DATA_FILE_SQL: Final[str] = """\
SELECT
    df.data_file_id,
    df.path,
    df.path_is_relative,
    df.file_format,
    df.record_count,
    df.file_size_bytes,
    df.footer_size,
    df.row_id_start,
    df.partition_id,
    df.encryption_key,
    df.mapping_id,
    df.partial_max,
    df.row_group_count,
    df.file_order,
    df.begin_snapshot
FROM {data_file} AS df
WHERE df.table_id = $table_id
  AND $snapshot_id >= df.begin_snapshot
  AND (df.end_snapshot IS NULL OR $snapshot_id < df.end_snapshot)
ORDER BY df.data_file_id"""

_DELETE_FILE_SQL: Final[str] = """\
SELECT
    dl.delete_file_id,
    dl.data_file_id,
    dl.path,
    dl.path_is_relative,
    lower(dl.format) AS format,
    dl.delete_count,
    dl.file_size_bytes,
    dl.encryption_key,
    EXISTS (
        SELECT 1 FROM {data_file} AS df
        WHERE df.data_file_id = dl.data_file_id
          AND df.table_id = dl.table_id
          AND df.end_snapshot IS NOT NULL
          AND df.end_snapshot <= $snapshot_id
    ) AS data_file_ended
FROM {delete_file} AS dl
WHERE dl.table_id = $table_id
  AND $snapshot_id >= dl.begin_snapshot
  AND (dl.end_snapshot IS NULL OR $snapshot_id < dl.end_snapshot)
ORDER BY dl.delete_file_id"""

_PATH_LAYERS_SQL: Final[str] = """\
SELECT
    (SELECT m.value FROM {metadata} AS m
      WHERE m.key = 'data_path' AND m.scope IS NULL) AS data_path,
    s.path             AS schema_path,
    s.path_is_relative AS schema_path_is_relative,
    t.path             AS table_path,
    t.path_is_relative AS table_path_is_relative
FROM {table} AS t
JOIN {schema} AS s
  ON s.schema_id = t.schema_id
 AND $snapshot_id >= s.begin_snapshot
 AND (s.end_snapshot IS NULL OR $snapshot_id < s.end_snapshot)
WHERE t.table_id = $table_id
  AND $snapshot_id >= t.begin_snapshot
  AND (t.end_snapshot IS NULL OR $snapshot_id < t.end_snapshot)"""


def list_files(
    con: duckdb.DuckDBPyConnection,
    catalog: str,
    schema_name: str,
    table_name: str,
    snapshot_id: int,
) -> list[dict[str, object]]:
    """Primary source: ``ducklake_list_files``."""
    return _q(
        con,
        _LIST_FILES_SQL,
        {
            "catalog": catalog,
            "table_name": table_name,
            "schema_name": schema_name,
            "snapshot_id": snapshot_id,
        },
    )


def read_data_files(
    con: duckdb.DuckDBPyConnection, meta: str, table_id: int, snapshot_id: int
) -> list[dict[str, object]]:
    """Complementary source: ``ducklake_data_file`` in the live range."""
    sql = _DATA_FILE_SQL.format(data_file=f"{quote_ident(meta)}.ducklake_data_file")
    return _q(con, sql, {"table_id": table_id, "snapshot_id": snapshot_id})


def read_delete_files(
    con: duckdb.DuckDBPyConnection, meta: str, table_id: int, snapshot_id: int
) -> list[dict[str, object]]:
    """Complementary source for deletes: live ``ducklake_delete_file`` rows."""
    q = quote_ident(meta)
    sql = _DELETE_FILE_SQL.format(
        delete_file=f"{q}.ducklake_delete_file", data_file=f"{q}.ducklake_data_file"
    )
    return _q(con, sql, {"table_id": table_id, "snapshot_id": snapshot_id})


def read_path_layers(
    con: duckdb.DuckDBPyConnection, meta: str, table_id: int, snapshot_id: int
) -> PathLayers:
    """Path layers and the global ``data_path``."""
    q = quote_ident(meta)
    sql = _PATH_LAYERS_SQL.format(
        metadata=f"{q}.ducklake_metadata",
        table=f"{q}.ducklake_table",
        schema=f"{q}.ducklake_schema",
    )
    rows = _q(con, sql, {"table_id": table_id, "snapshot_id": snapshot_id})
    if not rows:
        raise FileEnumerationError(
            f"table table_id={table_id} missing at snapshot {snapshot_id}"
        )
    row = rows[0]
    data_path = row["data_path"]
    if data_path is None:
        raise FileEnumerationError("global data_path not found in ducklake_metadata")
    return PathLayers(
        data_path=str(data_path),
        schema_path=_opt_str(row["schema_path"]),
        schema_path_is_relative=bool(row["schema_path_is_relative"]),
        table_path=_opt_str(row["table_path"]),
        table_path_is_relative=bool(row["table_path_is_relative"]),
    )


# -- coercions ---------------------------------------------------------------


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return int(str(value))


def _opt_int(value: object) -> int | None:
    return None if value is None else _as_int(value)


def _opt_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


# -- cross-check and reconciliation (pure) ---------------------------


def reconcile_files(
    list_files_rows: Sequence[Mapping[str, object]],
    data_file_rows: Sequence[Mapping[str, object]],
    layers: PathLayers,
    *,
    table: str,
    snapshot_id: int,
) -> tuple[LiveDataFile, ...]:
    """Reconcile the two sources by canonical path. Pure.

    Raises ``FileSetDivergence`` (exit 3) on the smallest divergence,
    including a duplicate path in either source.
    """
    # A: canonical form of the list_files paths
    a_canon: dict[str, str] = {}  # canonical -> original path (list_files)
    for row in list_files_rows:
        raw = row["data_file"]
        path = str(raw)
        canon = canonical(path)
        if canon in a_canon:
            raise FileSetDivergence(
                [],
                [],
                n_list_files=0,
                n_data_file=0,
                table=table,
                snapshot_id=snapshot_id,
                duplicate=path,
            )
        a_canon[canon] = path

    # B: canonical form of the composed data_file paths
    b_canon: dict[str, Mapping[str, object]] = {}
    b_path_by_canon: dict[str, tuple[int, str]] = {}
    for row in data_file_rows:
        composed = compose_path(layers, str(row["path"]), bool(row["path_is_relative"]))
        canon = canonical(composed)
        if canon in b_canon:
            raise FileSetDivergence(
                [],
                [],
                n_list_files=0,
                n_data_file=0,
                table=table,
                snapshot_id=snapshot_id,
                duplicate=composed,
            )
        b_canon[canon] = row
        b_path_by_canon[canon] = (_as_int(row["data_file_id"]), composed)

    set_a = set(a_canon)
    set_b = set(b_canon)
    if set_a != set_b:
        only_a = sorted(a_canon[c] for c in (set_a - set_b))
        only_b = sorted(
            (b_path_by_canon[c] for c in (set_b - set_a)),
            key=lambda t: t[1],
        )
        raise FileSetDivergence(
            only_a,
            only_b,
            n_list_files=len(a_canon),
            n_data_file=len(b_canon),
            table=table,
            snapshot_id=snapshot_id,
        )

    # union: one LiveDataFile per file, path taken from list_files
    files: list[LiveDataFile] = []
    for canon in sorted(set_a):
        df = b_canon[canon]
        list_path = a_canon[canon]
        files.append(_build_live(df, list_path, canon))
    return tuple(files)


def _build_live(df: Mapping[str, object], list_path: str, canon: str) -> LiveDataFile:
    record_count = df["record_count"]
    if record_count is None:
        raise FileEnumerationError(
            f"file {df['data_file_id']} has no record_count in the catalog"
        )
    return LiveDataFile(
        data_file_id=_as_int(df["data_file_id"]),
        path=list_path,
        canonical_path=canon,
        file_size_in_bytes=_as_int(df["file_size_bytes"]),
        footer_size=_opt_int(df["footer_size"]),
        record_count=_as_int(record_count),
        file_format=str(df["file_format"]),
        partition_id=_opt_int(df["partition_id"]),
        partial_max=_opt_int(df["partial_max"]),
        mapping_id=_opt_int(df["mapping_id"]),
        row_id_start=_opt_int(df["row_id_start"]),
        row_group_count=_opt_int(df["row_group_count"]),
        begin_snapshot=_as_int(df["begin_snapshot"]),
        file_order=_opt_int(df["file_order"]),
    )


def attach_delete_files(
    files: Sequence[LiveDataFile],
    list_files_rows: Sequence[Mapping[str, object]],
    delete_file_rows: Sequence[Mapping[str, object]],
    layers: PathLayers,
    *,
    table: str,
    snapshot_id: int,
) -> tuple[LiveDataFile, ...]:
    """Pair each data file with its live delete file, cross-checking both sources.

    The same defense as for data files: the (data file, delete file) pair from
    ``ducklake_list_files`` must match ``ducklake_delete_file`` by canonical
    path. A forgotten delete resurrects rows in Athena, an extra one erases
    live rows - both silently. Pure.
    """
    listed: dict[str, str] = {}  # canonical(data_file) -> delete_file
    for row in list_files_rows:
        delete_path = row.get("delete_file")
        if delete_path is not None:
            listed[canonical(str(row["data_file"]))] = str(delete_path)

    by_data_file_id: dict[int, Mapping[str, object] | None] = {}
    for row in delete_file_rows:
        if row.get("data_file_ended"):
            # Dangling delete file: when a write removes all rows of a data
            # file, DuckLake ends the data file and leaves the previous delete
            # file open. ``ducklake_list_files`` omits it and the positions
            # are dropped (DanglingPolicy.DROP); this is not a divergence.
            continue
        data_file_id = _as_int(row["data_file_id"])
        if data_file_id in by_data_file_id:
            raise FileEnumerationError(
                f"data file {data_file_id} has more than one live delete file"
            )
        by_data_file_id[data_file_id] = row

    out: list[LiveDataFile] = []
    only_listed: list[str] = []
    only_catalog: list[tuple[int, str]] = []
    for f in files:
        found = by_data_file_id.pop(f.data_file_id, None)
        listed_path = listed.get(f.canonical_path)
        if found is None:
            if listed_path is not None:
                only_listed.append(listed_path)
            out.append(f)
            continue
        composed = compose_path(
            layers, str(found["path"]), bool(found["path_is_relative"])
        )
        if listed_path is None or canonical(listed_path) != canonical(composed):
            only_catalog.append((_as_int(found["delete_file_id"]), composed))
            if listed_path is not None:
                only_listed.append(listed_path)
            out.append(f)
            continue
        delete = LiveDeleteFile(
            delete_file_id=_as_int(found["delete_file_id"]),
            path=listed_path,
            canonical_path=canonical(listed_path),
            file_size_in_bytes=_as_int(found["file_size_bytes"]),
            delete_count=_as_int(found["delete_count"]),
            file_format=str(found["format"]),
        )
        out.append(dataclasses.replace(f, delete_file=delete))
    # delete files whose data file never existed or has not started yet: the
    # catalog diverges from itself
    only_catalog.extend(
        (_as_int(r["delete_file_id"]), str(r["path"]))
        for r in by_data_file_id.values()
        if r is not None
    )
    if only_listed or only_catalog:
        raise FileSetDivergence(
            sorted(only_listed),
            sorted(only_catalog, key=lambda t: t[1]),
            n_list_files=len(listed),
            n_data_file=len(delete_file_rows),
            table=f"{table} (delete files)",
            snapshot_id=snapshot_id,
        )
    return tuple(out)


# -- blockers (pure) ---------------------------------------------------


def check_blockers(
    files: Sequence[LiveDataFile],
    list_files_rows: Sequence[Mapping[str, object]],
    data_file_rows: Sequence[Mapping[str, object]],
    snapshot_id: int,
) -> list[Finding]:
    """Re-run the doctor rules over the data already read. Pure."""
    findings: list[Finding] = []

    enc_count = sum(
        1 for r in list_files_rows if r.get("data_file_encryption_key") is not None
    ) + sum(1 for r in data_file_rows if r.get("encryption_key") is not None)
    if enc_count:
        findings.append(
            _blocker(
                "encryption",
                f"{enc_count} encrypted file(s); metadata conversion is not enough",
                "out of scope",
            )
        )

    del_enc = sum(
        1 for r in list_files_rows if r.get("delete_file_encryption_key") is not None
    )
    if del_enc:
        findings.append(
            _blocker(
                "encryption",
                f"{del_enc} encrypted delete file(s)",
                "out of scope",
            )
        )

    puffin = sum(
        1
        for f in files
        if f.delete_file is not None and f.delete_file.file_format != _PARQUET
    )
    if puffin:
        findings.append(
            _blocker(
                "delete-format",
                f"{puffin} delete file(s) not in parquet format (puffin/DV); "
                f"Athena does not read deletion vectors in Iceberg v2",
                "rewrite the data files in DuckLake "
                "(CALL ducklake_rewrite_data_files) to remove the deletion vectors",
            )
        )

    bad_format = sorted(
        {f.file_format for f in files if f.file_format.lower() != _PARQUET}
    )
    if bad_format:
        findings.append(
            _blocker(
                "file-format",
                f"unsupported format: {', '.join(bad_format)}",
                "rewrite the files",
            )
        )

    mapping_ids = {f.mapping_id for f in files if f.mapping_id is not None}
    if len(mapping_ids) > 1:
        findings.append(
            _blocker(
                "mapping-id",
                f"{len(mapping_ids)} distinct mapping_id values among the live files",
                "rewrite the diverging files",
            )
        )

    partial = sum(
        1 for f in files if f.partial_max is not None and f.partial_max > snapshot_id
    )
    if partial:
        findings.append(
            _blocker(
                "partial-max",
                f"{partial} partial file(s) beyond snapshot {snapshot_id}",
                "export only the current snapshot",
            )
        )
    return findings


def _blocker(subject: str, detail: str, remediation: str) -> Finding:
    return Finding(
        rule_id=_RULE_ID,
        severity=Severity.BLOCKER,
        subject=subject,
        detail=detail,
        remediation=remediation,
        data={},
    )


# -- truly empty vs inlining (light I/O) --------------------------------


def classify_empty(
    con: duckdb.DuckDBPyConnection,
    meta: str,
    target: DoctorTarget,
) -> EmptyReason:
    """Distinguish TRULY_EMPTY, INLINED_PENDING and UNKNOWN."""
    status = detect_inlined(
        con,
        meta,
        TableRef(target.schema_name, target.table_name, target.table_id),
        target.snapshot_id,
    )
    if status.has_pending:
        return EmptyReason.INLINED_PENDING

    record_count = _table_stats_record_count(con, meta, target.table_id)
    if record_count is not None and record_count > 0:
        return EmptyReason.UNKNOWN
    return EmptyReason.TRULY_EMPTY


def _table_stats_record_count(
    con: duckdb.DuckDBPyConnection, meta: str, table_id: int
) -> int | None:
    q = quote_ident(meta)
    try:
        rows = _q(
            con,
            f"SELECT ts.record_count AS record_count "
            f"FROM {q}.ducklake_table_stats AS ts "
            f"WHERE ts.table_id = $table_id",
            {"table_id": table_id},
        )
    except Exception:
        return None
    if not rows or rows[0]["record_count"] is None:
        return None
    return _as_int(rows[0]["record_count"])


# -- current snapshot ------------------------------------------------------


def _current_snapshot_id(con: duckdb.DuckDBPyConnection, meta: str) -> int:
    q = quote_ident(meta)
    rows = _q(
        con,
        f"SELECT max(snapshot_id) AS current_id FROM {q}.ducklake_snapshot",
        {},
    )
    if not rows or rows[0]["current_id"] is None:
        raise FileEnumerationError("catalog has no snapshots")
    return _as_int(rows[0]["current_id"])


# -- orchestration -----------------------------------------------------------


def enumerate_files(
    con: duckdb.DuckDBPyConnection,
    target: DoctorTarget,
    *,
    allow_historic: bool = False,
) -> FileSet:
    """Orchestrate the two reads, reconcile and return the ``FileSet``.

    Exactly two queries on the happy path with files (``list_files`` +
    ``data_file``), plus the path one; the current-snapshot and emptiness
    queries only run in their respective branches. Raises
    ``FileSetDivergence`` (exit 3), ``FileEnumerationBlocked`` (exit 2) or
    ``FileEnumerationError`` (exit 1).

    ``allow_historic`` accepts a snapshot older than the current one: the
    export targets the most recent snapshot without pending inlined data
    (``detect.clean_snapshot``). The cross-check still applies and reports
    (exit 3) if ``ducklake_list_files`` diverges from ``ducklake_data_file``
    at that snapshot.
    """
    meta = target.metadata_catalog

    # Outside the export, only the current snapshot is allowed.
    current = _current_snapshot_id(con, meta)
    if target.snapshot_id > current or (
        target.snapshot_id != current and not allow_historic
    ):
        raise FileEnumerationBlocked(
            [
                _blocker(
                    "historic-snapshot",
                    f"export of a historic snapshot is not supported here "
                    f"(target={target.snapshot_id}, current={current})",
                    "use the current snapshot",
                )
            ]
        )

    layers = read_path_layers(con, meta, target.table_id, target.snapshot_id)
    list_rows = list_files(
        con,
        target.catalog,
        target.schema_name,
        target.table_name,
        target.snapshot_id,
    )
    df_rows = read_data_files(con, meta, target.table_id, target.snapshot_id)
    del_rows = read_delete_files(con, meta, target.table_id, target.snapshot_id)

    if not list_rows and not df_rows:
        reason = classify_empty(con, meta, target)
        if reason is EmptyReason.INLINED_PENDING:
            raise FileEnumerationBlocked(
                [
                    _blocker(
                        "inlined-data",
                        "table has inlined data; export would produce an empty table",
                        f"ducklake-iceberg sync --table {target.qualified_name}",
                    )
                ]
            )
        if reason is EmptyReason.UNKNOWN:
            raise FileEnumerationError(
                "ducklake_table_stats.record_count > 0 and no live files"
            )
        return FileSet(
            files=(),
            layers=layers,
            min_begin_snapshot=None,
            total_size_in_bytes=0,
            total_record_count=0,
            empty_reason=EmptyReason.TRULY_EMPTY,
        )

    files = reconcile_files(
        list_rows,
        df_rows,
        layers,
        table=target.qualified_name,
        snapshot_id=target.snapshot_id,
    )
    files = attach_delete_files(
        files,
        list_rows,
        del_rows,
        layers,
        table=target.qualified_name,
        snapshot_id=target.snapshot_id,
    )
    blockers = check_blockers(files, list_rows, df_rows, target.snapshot_id)
    if blockers:
        raise FileEnumerationBlocked(blockers)

    return FileSet(
        files=files,
        layers=layers,
        min_begin_snapshot=min(f.begin_snapshot for f in files),
        total_size_in_bytes=sum(f.file_size_in_bytes for f in files),
        total_record_count=sum(f.record_count for f in files),
        empty_reason=None,
    )
