"""SQL visibility fragment and shared helpers.

Every rule that reads a versioned table (`begin_snapshot`/`end_snapshot`) uses
the same predicate, exported here so it is not rewritten in 8 places.

Tables **without** a snapshot interval (`ducklake_metadata`,
`ducklake_inlined_data_tables`, `ducklake_file_partition_value`,
`ducklake_file_column_stats`, `ducklake_partition_column`,
`ducklake_column_mapping`) must not receive this predicate: there it is an SQL
error, not a harmless filter.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

#: Limit of individual findings per rule; above it, a single aggregate.
MAX_FINDINGS: int = 20

VISIBLE_AT_SNAPSHOT = (
    "$snapshot_id >= {alias}begin_snapshot\n"
    "  AND ({alias}end_snapshot IS NULL "
    "OR $snapshot_id < {alias}end_snapshot)"
)


def visible(alias: str = "") -> str:
    """Visibility predicate, optionally prefixed by an alias."""
    return VISIBLE_AT_SNAPSHOT.format(alias=f"{alias}." if alias else "")


def one_row(table: pa.Table, column: str, default: Any = None) -> Any:
    """First value of a one-row `pa.Table`; `default` if empty."""
    if table.num_rows == 0:
        return default
    return table.column(column)[0].as_py()


def as_int(value: Any) -> int:
    """Safely coerce an Arrow cell value (`object`) to int."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return int(str(value))
