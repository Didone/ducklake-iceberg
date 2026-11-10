"""Instrument catalog reads without recording SQL text or bound values."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from ducklake_iceberg import timing

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

Params = Sequence[Any] | Mapping[str, Any] | None


def query_arrow(
    con: duckdb.DuckDBPyConnection, sql: str, params: Params = None
) -> pa.Table:
    with timing.phase("catalog.query"):
        timing.count("catalog.queries")
        if params is None:
            return con.sql(sql).to_arrow_table()
        return con.sql(sql, params=params).to_arrow_table()
