"""Instrument catalog reads without recording SQL text or bound values."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from ducklake_iceberg import timing

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

Params = Sequence[Any] | Mapping[str, Any] | None

_READ_CACHE: ContextVar[tuple[int, dict[str, pa.Table]] | None] = ContextVar(
    "catalog_read_cache", default=None
)


@contextmanager
def cache_reads(con: duckdb.DuckDBPyConnection) -> Iterator[None]:
    """Cache materialized queries only for the owning read transaction."""
    token = _READ_CACHE.set((id(con), {}))
    try:
        yield
    finally:
        _READ_CACHE.reset(token)


def _key(sql: str, params: Params) -> str | None:
    # Parameters are kept in memory, never emitted. Unsupported values bypass
    # caching; type tags prevent bool/int and sequence/map collisions.
    def freeze(value: Any) -> Any:
        if value is None or type(value) in (str, int, bool, bytes, float):
            return (type(value).__name__, value)
        if isinstance(value, Mapping):
            return ("mapping", tuple(sorted((k, freeze(v)) for k, v in value.items())))
        if isinstance(value, list | tuple):
            return (type(value).__name__, tuple(freeze(v) for v in value))
        raise TypeError

    try:
        return repr((sql, freeze(params)))
    except TypeError:
        return None


def query_arrow(
    con: duckdb.DuckDBPyConnection, sql: str, params: Params = None
) -> pa.Table:
    scope = _READ_CACHE.get()
    key = _key(sql, params) if scope is not None and scope[0] == id(con) else None
    if key is not None and scope is not None and key in scope[1]:
        timing.count("catalog.query_cache_hits")
        return scope[1][key]
    with timing.phase("catalog.query"):
        timing.count("catalog.queries")
        if params is None:
            result = con.sql(sql).to_arrow_table()
        else:
            result = con.sql(sql, params=params).to_arrow_table()
    if key is not None and scope is not None:
        scope[1][key] = result
    return result


def prime_reads(
    con: duckdb.DuckDBPyConnection, requests: Sequence[tuple[str, Params]]
) -> None:
    """Materialize trusted internal SELECTs in one statement, preserving Arrow types.

    Only an active transaction cache may receive results. Each subquery has
    the same transaction and bindings as its later individual read. No SQL or
    parameter text is recorded by instrumentation.
    """
    import pyarrow as pa

    scope = _READ_CACHE.get()
    if scope is None or scope[0] != id(con) or not requests:
        return
    bindings: dict[str, Any] = {}
    expressions = []
    keys = []
    for index, (sql, params) in enumerate(requests):
        if not isinstance(params, Mapping):
            raise TypeError("batch reads require named parameters")
        for name, value in params.items():
            if name in bindings and bindings[name] != value:
                raise ValueError("batch reads require consistent parameter bindings")
            bindings[name] = value
        key = _key(sql, params)
        if key is None:
            return
        keys.append(key)
        # SQL comes only from internal rule builders, never user input. Values
        # remain bound; generated aliases are numeric and not externally supplied.
        expressions.append(f"(SELECT list(q) FROM ({sql}) q) AS r{index}")  # noqa: S608
    result = query_arrow(con, "SELECT " + ", ".join(expressions), bindings)
    for index, key in enumerate(keys):
        values = result.column(index).combine_chunks().values
        scope[1][key] = pa.Table.from_struct_array(values)
    timing.count("catalog.batched_reads", len(requests))


def snapshot_rows(con: duckdb.DuckDBPyConnection, meta: str) -> pa.Table:
    """One shared current-snapshot read for identity and enumeration checks."""
    quoted = '"' + meta.replace('"', '""') + '"'
    return query_arrow(
        con,
        "SELECT snapshot_id, CAST(snapshot_time AS TIMESTAMPTZ) AS snapshot_time, "  # noqa: S608 - quoted metadata identifier
        "schema_version, next_catalog_id, next_file_id "
        f"FROM {quoted}.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1",
    )
