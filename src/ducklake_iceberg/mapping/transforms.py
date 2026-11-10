"""Pure partition-transform policy shared by validation and export."""

from __future__ import annotations

import re
from typing import Any, Final

from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    YearTransform,
)

CALENDAR_TRANSFORMS: Final = frozenset({"year", "month", "day", "hour"})
_EXACT: Final[dict[str, Transform[Any, Any]]] = {
    "identity": IdentityTransform(),
    "epoch_year": YearTransform(),
    "epoch_month": MonthTransform(),
    "epoch_day": DayTransform(),
    "epoch_hour": HourTransform(),
}
_BUCKET = re.compile(r"\Abucket\s*\(\s*(\d+)\s*(?:,[^)]*)?\)\Z")


class PartitionSpecError(Exception):
    """Inconsistent partition catalog (exit 1)."""

    exit_code = 1


def map_transform(text: str) -> Transform[Any, Any] | None:
    """Map equivalent transforms; return None for calendar/unknown transforms."""
    normalized = text.strip().lower()
    if normalized in _EXACT:
        return _EXACT[normalized]
    match = _BUCKET.fullmatch(normalized)
    if match is None:
        return None
    buckets = int(match.group(1))
    if buckets <= 0:
        raise PartitionSpecError(f"invalid bucket transform: {text!r}")
    return BucketTransform(buckets)
