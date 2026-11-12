"""Batch independent compatibility reads without changing rule classifications."""

from __future__ import annotations

import duckdb

from ducklake_iceberg import timing
from ducklake_iceberg.catalog.query import Params, prime_reads
from ducklake_iceberg.doctor.model import DoctorContext
from ducklake_iceberg.doctor.rules import (
    calendar_transform,
    delete_parquet,
    delete_puffin,
    encryption_key,
    mapping_divergence,
    partial_files,
    unsupported_type,
)


def prefetch(ctx: DoctorContext) -> None:
    """Prime stable read results; failures still belong to individual rules."""
    statements = [
        partial_files._sql(ctx),
        delete_parquet._sql(ctx),
        delete_puffin._sql_option(ctx),
        mapping_divergence._sql_distribution(ctx),
        mapping_divergence._sql_types(ctx),
        encryption_key._sql(ctx),
        unsupported_type._sql(ctx),
        calendar_transform._sql(ctx),
    ]
    requests: list[tuple[str, Params]] = [
        (sql, {name: value for name, value in ctx.bind.items() if f"${name}" in sql})
        for sql in statements
    ]
    try:
        prime_reads(ctx.con, requests)
    except duckdb.Error:
        # Do not mask a rule error or attribute a shared query failure to all
        # rules. The runner executes the original reads and produces its report.
        timing.count("catalog.batch_failures")
