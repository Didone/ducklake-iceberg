"""Package of the concrete `doctor` rules.

Importing this package populates the registry (`ducklake_iceberg.doctor.registry`) as a
side effect of each module's `register(...)` call. The import is **explicit**
(not `pkgutil.walk_packages`): the list is short, fixed, and checkable by
`mypy` and `ruff`. The import order follows `PLAN_ORDER`.
"""

from __future__ import annotations

from ducklake_iceberg.doctor.model import Rule
from ducklake_iceberg.doctor.rules import (
    calendar_transform,
    delete_parquet,
    delete_puffin,
    encryption_key,
    inlined_data,
    mapping_divergence,
    parquet_compression,
    partial_files,
    unsupported_type,
)

#: Rules in the declared execution order.
RULES: tuple[Rule, ...] = (
    inlined_data.RULE,
    partial_files.RULE,
    delete_parquet.RULE,
    delete_puffin.RULE,
    mapping_divergence.RULE,
    encryption_key.RULE,
    unsupported_type.RULE,
    calendar_transform.RULE,
    parquet_compression.RULE,
)

__all__ = ["RULES"]
