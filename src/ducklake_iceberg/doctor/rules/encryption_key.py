"""Rule `encryption-key`.

DuckLake's encryption envelope (a per-file key stored in the catalog) does not
match Iceberg's; an Iceberg reader could not decrypt the files. Encrypted
tables are out of scope. The rule never exposes the value of `encryption_key`
in detail, data, or logs.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from ducklake_iceberg.doctor.model import DoctorContext, Finding, Severity
from ducklake_iceberg.doctor.registry import register
from ducklake_iceberg.doctor.sql import one_row


def _sql(ctx: DoctorContext) -> str:
    data_file = ctx.meta("ducklake_data_file")
    delete_file = ctx.meta("ducklake_delete_file")
    metadata = ctx.meta("ducklake_metadata")
    return f"""\
SELECT
    (SELECT count(*)
       FROM {data_file} df
      WHERE df.table_id = $table_id
        AND df.encryption_key IS NOT NULL
        AND $snapshot_id >= df.begin_snapshot
        AND (df.end_snapshot IS NULL
             OR $snapshot_id < df.end_snapshot))   AS encrypted_data_files,
    (SELECT count(*)
       FROM {delete_file} dl
      WHERE dl.table_id = $table_id
        AND dl.encryption_key IS NOT NULL
        AND $snapshot_id >= dl.begin_snapshot
        AND (dl.end_snapshot IS NULL
             OR $snapshot_id < dl.end_snapshot))   AS encrypted_delete_files,
    (SELECT lower(m.value)
       FROM {metadata} m
      WHERE m.key = 'encrypted' AND m.scope IS NULL
      LIMIT 1)                                     AS catalog_encrypted"""


def classify(table: pa.Table, ctx: DoctorContext, rule_id: str) -> list[Finding]:
    data_files = int(one_row(table, "encrypted_data_files", 0) or 0)
    delete_files = int(one_row(table, "encrypted_delete_files", 0) or 0)
    catalog_encrypted = one_row(table, "catalog_encrypted")

    if data_files > 0 or delete_files > 0:
        return [
            Finding(
                rule_id=rule_id,
                severity=Severity.BLOCKER,
                subject=ctx.target.qualified_name,
                detail=(
                    f"{data_files} data files and {delete_files} delete files "
                    f"have encryption_key set; DuckLake's encryption envelope "
                    f"does not match Iceberg's, and an Iceberg reader could not "
                    f"decrypt the files"
                ),
                remediation=(
                    "encrypted tables are out of scope; export an unencrypted "
                    "copy of the table, if policy allows"
                ),
                data={
                    "encrypted_data_files": data_files,
                    "encrypted_delete_files": delete_files,
                },
            )
        ]
    if catalog_encrypted == "true":
        return [
            Finding(
                rule_id=rule_id,
                severity=Severity.BLOCKER,
                subject=ctx.target.qualified_name,
                detail=(
                    "catalog configured as encrypted (encrypted=true); the next "
                    "write will produce encrypted files, unreadable by an "
                    "Iceberg reader"
                ),
                remediation=(
                    "encrypted tables are out of scope; export an unencrypted copy"
                ),
                data={"catalog_encrypted": True},
            )
        ]
    return []


@dataclass(frozen=True, slots=True)
class EncryptionKeyRule:
    id: str = "encryption-key"
    title: str = "encryption_key set"
    severity: Severity = Severity.BLOCKER
    remediation: str = "out of scope; export an unencrypted copy"
    requires: tuple[str, ...] = ("inlined-data",)

    def check(self, ctx: DoctorContext) -> Sequence[Finding]:
        table = ctx.query(_sql(ctx), ctx.bind)
        return classify(table, ctx, self.id)


RULE = register(EncryptionKeyRule())
