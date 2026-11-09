"""Post-checkpoint revalidation.

Reruns, against the export target snapshot, the `doctor` rules whose primary
source is files or inlining tables, and classifies each finding as having
appeared in the checkpoint window (`introduced`) or as already existing before
it (`pre_existing`).

Three decisions this module implements:

- **New connection.** The READ_ONLY connection from the start of the command
  saw the catalog before the checkpoint commit; revalidation opens another one
  (`open_catalog` calls `postgres_clear_cache()` on the Postgres backend).
- **Classification by object, not by report diff.** Before the checkpoint the
  file rules could be `SKIPPED` by the `inlined-data` gate; a diff would mark
  everything as new.
- **New = `file_id >= next_file_id` of the previous snapshot.** Verified at
  runtime: the file created by `merge_adjacent_files` inherits the **smallest**
  `begin_snapshot` of the merged files, so comparing `begin_snapshot` would
  classify a newly created file as old. File ids come from a monotonic counter
  (`ducklake_snapshot.next_file_id`), which does not have this problem.
  Without the counter, it falls back to `begin_snapshot > snapshot_before`.

The checkpoint is not reversible: the report describes the current state and
the action possible from it.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Final, Literal

from ducklake_iceberg.catalog.connection import DuckLakeConnection, open_catalog
from ducklake_iceberg.catalog.reader import snapshot_at, table_schema_version
from ducklake_iceberg.checkpoint.detect import clean_snapshot, detect_inlined
from ducklake_iceberg.checkpoint.model import TableRef
from ducklake_iceberg.config import DuckLakeIcebergSettings
from ducklake_iceberg.doctor import rules as doctor_rules
from ducklake_iceberg.doctor.model import (
    DoctorContext,
    DoctorTarget,
    Finding,
    RuleStatus,
    Severity,
)
from ducklake_iceberg.doctor.runner import run as run_rules
from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.output import worse_exit

LOG = logging.getLogger("ducklake_iceberg")

Origin = Literal["introduced", "pre_existing", "unknown"]

#: Rules rerun after the checkpoint: every rule whose primary source is
#: `ducklake_data_file`, `ducklake_delete_file`, `ducklake_list_files` or an
#: inlining table.
REVALIDATION_RULES: Final[frozenset[str]] = frozenset(
    {
        "inlined-data",
        "partial-files",
        "delete-parquet",
        "delete-puffin",
        "mapping-divergence",
        "encryption-key",
    }
)

#: Text per origin. The wording distinguishes "appeared" from "already existed".
ORIGIN_TEXT: Final[Mapping[Origin, str]] = {
    "introduced": "appeared in this window",
    "pre_existing": "already existed before the checkpoint",
    "unknown": "no associated file object",
}


@dataclass(frozen=True, slots=True)
class ClassifiedFinding:
    finding: Finding
    origin: Origin
    object_begin_snapshot: int | None
    introduced_objects: int = 0
    pre_existing_objects: int = 0

    def to_json(self) -> dict[str, Any]:
        return {
            "rule_id": self.finding.rule_id,
            "severity": self.finding.severity.json,
            "origin": self.origin,
            "origin_text": ORIGIN_TEXT[self.origin],
            "subject": self.finding.subject,
            "object_begin_snapshot": self.object_begin_snapshot,
            "introduced_objects": self.introduced_objects,
            "pre_existing_objects": self.pre_existing_objects,
            "detail": self.finding.detail,
            "remediation": self.finding.remediation,
            "data": dict(self.finding.data),
        }


@dataclass(frozen=True, slots=True)
class Revalidation:
    table: TableRef
    snapshot_before: int
    snapshot_after: int
    rules_run: tuple[str, ...]
    rules_skipped: tuple[str, ...]
    findings: tuple[ClassifiedFinding, ...]
    still_pending_rows: int
    rules_errored: tuple[str, ...] = ()
    warnings: tuple[str, ...] = field(default_factory=tuple)

    @property
    def blockers(self) -> tuple[ClassifiedFinding, ...]:
        """`BLOCKER` findings, any origin, except the pending-inlined one.

        Remaining inlined data is a divergence (exit 3), not a compatibility
        blocker (exit 2).
        """
        return tuple(
            c
            for c in self.findings
            if c.finding.severity is Severity.BLOCKER
            and c.finding.rule_id != "inlined-data"
        )

    @property
    def introduced_blockers(self) -> tuple[ClassifiedFinding, ...]:
        return tuple(c for c in self.blockers if c.origin == "introduced")

    @property
    def exit_code(self) -> ExitCode:
        """Rule error 1 · remaining inlined 3 · blocker 2 · ok 0."""
        if self.rules_errored:
            return ExitCode.ERROR
        if self.still_pending_rows > 0:
            return ExitCode.DIVERGED
        if self.blockers:
            return ExitCode.BLOCKED
        return ExitCode.SUCCESS

    def to_json(self) -> dict[str, Any]:
        return {
            "snapshot_before": self.snapshot_before,
            "snapshot_after": self.snapshot_after,
            "rules_run": list(self.rules_run),
            "rules_skipped": list(self.rules_skipped),
            "rules_errored": list(self.rules_errored),
            "still_pending_rows": self.still_pending_rows,
            "findings": [c.to_json() for c in self.findings],
            "warnings": list(self.warnings),
            "export_target_snapshot": self.snapshot_after,
            "exit_code": int(self.exit_code),
            "reversible": False,
        }


@dataclass(frozen=True, slots=True)
class RevalidationRequest:
    table: TableRef
    snapshot_before: int
    snapshot_after: int
    next_file_id_before: int | None = None


# -- classification ------------------------------------------------------------

_VISIBLE = (
    "AND $snapshot_id >= {a}.begin_snapshot "
    "AND ({a}.end_snapshot IS NULL OR $snapshot_id < {a}.end_snapshot)"
)


def _files_sql(meta: str, kind: Literal["data", "delete"], where: str) -> str:
    """Files live in the snapshot, with the extra predicate `where`."""
    table, id_col = (
        ("ducklake_data_file", "data_file_id")
        if kind == "data"
        else ("ducklake_delete_file", "delete_file_id")
    )
    return (
        f"SELECT {id_col} AS file_id, begin_snapshot "
        f'FROM "{meta}".{table} AS f '
        f"WHERE f.table_id = $table_id {_VISIBLE.format(a='f')} AND ({where})"
    )


def _object_queries(finding: Finding, meta: str) -> list[tuple[str, dict[str, Any]]]:
    """Queries for the file objects backing the finding.

    Empty list = finding without a file object (`origin = "unknown"`).
    """
    data = finding.data
    rid = finding.rule_id
    if rid == "partial-files":
        params: dict[str, Any] = {}
        where = "f.partial_max IS NOT NULL"
        if "path" in data:
            where += " AND f.path = $path"
            params["path"] = data["path"]
        kinds: tuple[Literal["data", "delete"], ...] = (
            (str(data["kind"]),)  # type: ignore[assignment]
            if data.get("kind") in ("data", "delete")
            else ("data", "delete")
        )
        return [(_files_sql(meta, k, where), params) for k in kinds]
    if rid == "delete-parquet" and "format" in data:
        return [
            (
                _files_sql(meta, "delete", "lower(f.format) = $fmt"),
                {"fmt": data["format"]},
            )
        ]
    if rid == "delete-puffin" and "puffin_files" in data:
        return [(_files_sql(meta, "delete", "lower(f.format) = 'puffin'"), {})]
    if rid == "mapping-divergence":
        if "mapping_id" in data:
            return [
                (
                    _files_sql(meta, "data", "f.mapping_id = $mid"),
                    {"mid": data["mapping_id"]},
                )
            ]
        if "mapping_ids" in data:
            return [(_files_sql(meta, "data", "f.mapping_id IS NOT NULL"), {})]
        return []
    if rid == "encryption-key" and "catalog_encrypted" not in data:
        where = "f.encryption_key IS NOT NULL"
        return [(_files_sql(meta, k, where), {}) for k in ("data", "delete")]
    return []


def _classify(
    con: DuckLakeConnection,
    finding: Finding,
    request: RevalidationRequest,
) -> ClassifiedFinding:
    queries = _object_queries(finding, con.meta)
    if not queries:
        return ClassifiedFinding(finding, "unknown", None)

    base = {"table_id": request.table.table_id, "snapshot_id": request.snapshot_after}
    rows: list[dict[str, Any]] = []
    for sql, extra in queries:
        rows.extend(con.query_arrow(sql, {**base, **extra}).to_pylist())
    if not rows:
        return ClassifiedFinding(finding, "unknown", None)

    def is_new(row: dict[str, Any]) -> bool:
        if request.next_file_id_before is not None:
            return int(row["file_id"]) >= request.next_file_id_before
        return int(row["begin_snapshot"]) > request.snapshot_before

    new = sum(1 for r in rows if is_new(r))
    old = len(rows) - new
    return ClassifiedFinding(
        finding,
        "introduced" if new else "pre_existing",
        max(int(r["begin_snapshot"]) for r in rows),
        introduced_objects=new,
        pre_existing_objects=old,
    )


# -- execution -----------------------------------------------------------------


def _revalidate_one(
    con: DuckLakeConnection, request: RevalidationRequest
) -> Revalidation:
    with con.read_transaction():
        # the export target: under concurrent writes, before the inlined
        # commits that arrived after the flush (detect.clean_snapshot)
        request = dataclasses.replace(
            request,
            snapshot_after=clean_snapshot(
                con.raw, con.meta, request.table, request.snapshot_after
            ),
        )
        snapshot = snapshot_at(con, request.snapshot_after)
        target = DoctorTarget(
            catalog=con.alias,
            metadata_catalog=con.meta,
            schema_name=request.table.schema_name,
            table_name=request.table.table_name,
            table_id=request.table.table_id,
            snapshot_id=request.snapshot_after,
            schema_version=table_schema_version(con, request.table.table_id, snapshot),
        )
        selected = [r for r in doctor_rules.RULES if r.id in REVALIDATION_RULES]
        report = run_rules(
            DoctorContext(con=con.raw, target=target),
            selected,
            manage_transaction=False,
        )
        classified = tuple(
            _classify(con, f, request) for r in report.results for f in r.findings
        )
        status = detect_inlined(
            con.raw, con.meta, request.table, request.snapshot_after
        )

    not_selected = tuple(
        r.id for r in doctor_rules.RULES if r.id not in REVALIDATION_RULES
    )
    executed = (RuleStatus.PASSED, RuleStatus.FAILED, RuleStatus.ERROR)
    return Revalidation(
        table=request.table,
        snapshot_before=request.snapshot_before,
        snapshot_after=request.snapshot_after,
        rules_run=tuple(r.rule_id for r in report.results if r.status in executed),
        rules_skipped=(
            *(r.rule_id for r in report.results if r.status is RuleStatus.SKIPPED),
            *not_selected,
        ),
        rules_errored=tuple(
            r.rule_id for r in report.results if r.status is RuleStatus.ERROR
        ),
        findings=classified,
        still_pending_rows=status.pending_rows,
        warnings=status.warnings,
    )


def revalidate_many(
    cfg: DuckLakeIcebergSettings, requests: Iterable[RevalidationRequest]
) -> tuple[Revalidation, ...]:
    """One new READ_ONLY connection; one read transaction per table."""
    pending = tuple(requests)
    if not pending:
        return ()
    with open_catalog(cfg, read_only=True, s3=False) as con:
        return tuple(_revalidate_one(con, req) for req in pending)


def revalidate_after_checkpoint(
    cfg: DuckLakeIcebergSettings,
    table: TableRef,
    *,
    snapshot_before: int,
    snapshot_after: int,
    next_file_id_before: int | None = None,
) -> Revalidation:
    """Open a new READ_ONLY connection and run `REVALIDATION_RULES` against
    `snapshot_after`."""
    (result,) = revalidate_many(
        cfg,
        [
            RevalidationRequest(
                table, snapshot_before, snapshot_after, next_file_id_before
            )
        ],
    )
    return result


def aggregate_exit(revalidations: Iterable[Revalidation]) -> ExitCode:
    worst = ExitCode.SUCCESS
    for rev in revalidations:
        worst = worse_exit(worst, rev.exit_code)
    return worst
