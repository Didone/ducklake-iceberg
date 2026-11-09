"""Vocabulary, context, and rule protocol for `doctor`.

Defines the contract consumed by the rules and the execution plan: sortable
severities, the `Finding` object, the `DoctorContext` that carries the
connection and the table identity, and the structural `Rule` protocol. The
framework knows no concrete rule.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import TYPE_CHECKING, Any, Protocol, Union, cast

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

#: Values allowed in the `--json` document. Explicit alias (not `type ...`)
#: for Python 3.11 compatibility.
JsonValue = Union[  # noqa: UP007 - explicit alias, readable on 3.11
    str,
    int,
    float,
    bool,
    None,
    list["JsonValue"],
    dict[str, "JsonValue"],
]


class Severity(IntEnum):
    """Sortable: the exit code comes from ``max()`` over the findings."""

    INFO = 10
    DEGRADED = 20
    BLOCKER = 30

    @property
    def json(self) -> str:
        return _SEVERITY_JSON[self]


_SEVERITY_JSON: Mapping[Severity, str] = {
    Severity.INFO: "info",
    Severity.DEGRADED: "degraded",
    Severity.BLOCKER: "blocker",
}


class RuleStatus(StrEnum):
    PASSED = "passed"  # ran, no findings
    FAILED = "failed"  # ran, at least one finding
    SKIPPED = "skipped"  # did not run (a required gate failed)
    ERROR = "error"  # exception during check()


class Capability(StrEnum):
    """Pipeline capabilities that change the severity of a rule."""

    POSITION_DELETES = "position-deletes"  # delivered by the deletes module
    NAME_MAPPING = "name-mapping"  # To be verified (mapping-divergence rule)


@dataclass(frozen=True, slots=True)
class Finding:
    """A verdict about a specific object of the table.

    ``subject`` is always a stable identifier (column, file path, table),
    never a row index. ``data`` carries the same values as ``detail`` in
    structured form, for ``--json``.
    """

    rule_id: str
    severity: Severity
    subject: str
    detail: str
    remediation: str
    data: Mapping[str, JsonValue] = field(default_factory=dict)

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "severity": self.severity.json,
            "subject": self.subject,
            "detail": self.detail,
            "remediation": self.remediation,
            "data": dict(self.data),
        }


@dataclass(frozen=True, slots=True)
class DoctorTarget:
    catalog: str  # ATTACH alias:           'lake'
    metadata_catalog: str  # METADATA_CATALOG:       'lake_meta'
    schema_name: str  # 'sales'
    table_name: str  # 'orders'
    table_id: int  # ducklake_table.table_id
    snapshot_id: int  # export target snapshot (S)
    schema_version: int  # ducklake_snapshot.schema_version

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


@dataclass(frozen=True, slots=True)
class DoctorContext:
    con: duckdb.DuckDBPyConnection
    target: DoctorTarget
    format_version: int = 2
    capabilities: frozenset[Capability] = frozenset({Capability.POSITION_DELETES})

    def meta(self, table: str) -> str:
        """Qualified name of a table in the metadata catalog."""
        return f"{quote_ident(self.target.metadata_catalog)}.{table}"

    def query(
        self, sql: str, params: Mapping[str, JsonValue] | None = None
    ) -> pa.Table:
        """Run inside the transaction opened by the runner.

        `con.sql(params=...)` rejects extra parameters; since `ctx.bind`
        offers `table_id` and `snapshot_id` for convenience, we keep here only
        the ones the SQL actually references (`$name`).
        """
        used = {
            name: value for name, value in (params or {}).items() if f"${name}" in sql
        }
        # Uses `con.sql(...).to_arrow_table()` (no extra copy; it is what
        # DuckDB recommends). The duckdb stubs do not declare `.sql` with params.
        con = cast("Any", self.con)
        relation = con.sql(sql, params=used)
        return cast("pa.Table", relation.to_arrow_table())

    @property
    def bind(self) -> dict[str, JsonValue]:
        """Default parameters of every rule."""
        return {
            "table_id": self.target.table_id,
            "snapshot_id": self.target.snapshot_id,
        }


class Rule(Protocol):
    """Structural protocol; it is not ``runtime_checkable``.

    The attributes are declared as *read-only* properties so that a
    `@dataclass(frozen=True)` (whose fields are immutable) satisfies the
    protocol without a variance violation; a class with a read-write
    attribute still satisfies a read-only requirement.
    """

    @property
    def id(self) -> str:  # stable; key in --json
        ...

    @property
    def title(self) -> str:  # heading in the rich output
        ...

    @property
    def severity(self) -> Severity:  # maximum severity the rule can emit
        ...

    @property
    def remediation(self) -> str:  # template; the Finding carries the resolved text
        ...

    @property
    def requires(self) -> tuple[str, ...]:  # ids of gate rules
        ...

    def check(self, ctx: DoctorContext) -> Sequence[Finding]: ...


_IDENT = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")


def quote_ident(name: str) -> str:
    """Quote an SQL identifier; reject anything outside ``\\w``.

    The ``METADATA_CATALOG`` name comes from the configuration and must be
    interpolated (DuckDB has no identifier binding). Values always go through
    named parameters.
    """
    if not _IDENT.fullmatch(name):
        raise ValueError(f"invalid identifier for METADATA_CATALOG: {name!r}")
    return f'"{name}"'
