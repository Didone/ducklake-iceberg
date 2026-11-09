"""Read-only check of the DuckLake retention and of the published files.

The check never runs maintenance: the three DuckLake functions
(``ducklake_expire_snapshots``, ``ducklake_cleanup_old_files`` and
``ducklake_delete_orphaned_files``) are only called with ``dry_run => true``,
against the catalog attached as ``READ_ONLY``.
"""

from __future__ import annotations

import hashlib
import math
import re
import weakref
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum, StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Literal

from botocore.exceptions import BotoCoreError, ClientError
from rich.console import Console
from rich.table import Table as RichTable

from ducklake_iceberg import aws
from ducklake_iceberg.catalog.files import compose_path, read_path_layers
from ducklake_iceberg.concurrency import ContextThreadPoolExecutor
from ducklake_iceberg.config import RetentionSettings
from ducklake_iceberg.doctor.model import quote_ident
from ducklake_iceberg.errors import AwsError, DuckLakeIcebergError, ExitCode
from ducklake_iceberg.s3 import canonical_uri

if TYPE_CHECKING:
    from pyiceberg.table import Table as IcebergTable

    from ducklake_iceberg.catalog.connection import DuckLakeConnection


_DURATION_RE = re.compile(
    r"(?:(\d+)\s*d(?:ays?)?)?\s*(?:(\d+)\s*h(?:ours?)?)?\s*(?:(\d+)\s*m(?:in(?:utes?)?)?)?\s*(?:(\d+)\s*s(?:ec(?:onds?)?)?)?",
    re.I,
)
_OPTION_PATTERNS = ("%expir%", "%retention%", "%cleanup%", "%delete%")
_MAX_DISPLAY_FILES = 50
_HOUR: Final = timedelta(hours=1)
_MINUTE: Final = timedelta(minutes=1)


class RetentionSource(StrEnum):
    CONFIG = "config"
    CATALOG = "catalog"
    INFERRED = "inferred"


class Severity(StrEnum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"


_RANK: Final[dict[Severity, int]] = {
    Severity.OK: 0,
    Severity.WARNING: 1,
    Severity.CRITICAL: 2,
}


class _Unset(Enum):
    """Sentinel: the caller did not provide the Iceberg snapshot retention."""

    TOKEN = 0


#: ``validate_retention(iceberg_retention_hours=UNSET)``: without the
#: ``snapshot_retention_hours`` config, the Iceberg retention is observed from
#: the history.
UNSET: Final = _Unset.TOKEN


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Effective policy used in the computation.

    `expire_window`  : older_than passed to ducklake_expire_snapshots (R).
    `cleanup_window` : older_than passed to ducklake_cleanup_old_files (C),
                       i.e., the safety period between scheduling and the
                       physical removal of the object. ``0`` = unknown.
    """

    expire_window: timedelta
    cleanup_window: timedelta
    source: RetentionSource
    confidence: Literal["declared", "observed", "unknown"]
    evidence: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SyncProfile:
    interval: timedelta
    p95_duration: timedelta = timedelta(minutes=10)

    @property
    def cycle(self) -> timedelta:
        """``I + D``: longest gap between two successful ``sync`` runs."""
        return self.interval + self.p95_duration


@dataclass(frozen=True, slots=True)
class RiskWindow:
    published_snapshot_id: int
    published_at: datetime
    published_age: timedelta
    margin: timedelta
    severity: Severity
    recommended_expire_window: timedelta


@dataclass(frozen=True, slots=True)
class DoomedFile:
    path: str
    reason: Literal["expire", "cleanup", "scheduled", "missing_s3"]
    scheduled_at: datetime | None = None
    #: ``current``: cited by the current Iceberg snapshot (reads break);
    #: ``history``: only by earlier retained snapshots (time travel breaks).
    referenced_by: Literal["current", "history"] = "current"


@dataclass(frozen=True, slots=True)
class RetentionMessage:
    """Message: condition, text, and suggested action."""

    severity: Severity
    message: str
    hint: str | None = None


@dataclass(frozen=True, slots=True)
class IcebergFiles:
    """Files cited by the Iceberg side (data files and delete files)."""

    current: set[str]
    #: union of all snapshots retained in the published ``metadata.json``
    retained: set[str]
    retained_snapshots: int
    #: ``timestamp_ms`` of the oldest retained snapshot that is not the current one
    oldest_history_ms: int | None
    #: some retained snapshot has an already-expired snapshot as its parent
    expired_evidence: bool

    @property
    def history_only(self) -> set[str]:
        return self.retained - self.current


@dataclass(slots=True)
class RetentionReport:
    policy: RetentionPolicy
    sync: SyncProfile
    risk: RiskWindow
    published_snapshot_present: bool
    doomed_snapshots: list[int]
    referenced_files: int
    doomed_files: list[DoomedFile]
    s3_checked: int
    s3_missing: list[str]
    recommendation: str
    table: str = ""
    catalog_options: list[dict[str, Any]] = field(default_factory=list)
    messages: list[RetentionMessage] = field(default_factory=list)
    iceberg: dict[str, Any] = field(default_factory=dict)
    strict: bool = False

    @property
    def warnings(self) -> list[str]:
        return [m.message for m in self.messages if m.severity is Severity.WARNING]

    @property
    def warning(self) -> str | None:
        """Concatenated warnings (the envelope ``warning`` key)."""
        return "; ".join(self.warnings) or None

    @property
    def exit_code(self) -> ExitCode:
        """0 ok · 3 concrete risk · 1 error · 4 nothing to check.

        A warning only becomes 3 with ``--strict``.
        """
        if not self.published_snapshot_present:
            return ExitCode.DIVERGED
        if self.referenced_files == 0:
            return ExitCode.NOTHING_TO_DO
        if self.risk.severity is Severity.CRITICAL:
            return ExitCode.DIVERGED
        if self.strict and (self.risk.severity is Severity.WARNING or self.warnings):
            return ExitCode.DIVERGED
        return ExitCode.SUCCESS

    def as_dict(self, *, strict: bool | None = None) -> dict[str, Any]:
        if strict is not None:
            self.strict = strict
        return {
            "table": self.table,
            "policy": {
                "expire_window_seconds": _seconds(self.policy.expire_window),
                "cleanup_window_seconds": _seconds(self.policy.cleanup_window),
                "source": self.policy.source.value,
                "confidence": self.policy.confidence,
                "evidence": self.policy.evidence,
                "catalog_options": self.catalog_options,
            },
            "sync": {
                "interval_seconds": _seconds(self.sync.interval),
                "p95_duration_seconds": _seconds(self.sync.p95_duration),
            },
            "risk": {
                "published_snapshot_id": self.risk.published_snapshot_id,
                "published_at": self.risk.published_at.isoformat(),
                "published_age_seconds": _seconds(self.risk.published_age),
                "margin_seconds": _seconds(self.risk.margin),
                "severity": self.risk.severity.value,
                "recommended_expire_window_seconds": _seconds(
                    self.risk.recommended_expire_window
                ),
            },
            "published_snapshot_present": self.published_snapshot_present,
            "doomed_snapshots": self.doomed_snapshots,
            "referenced_files": self.referenced_files,
            "doomed_files": [
                {
                    "path": file.path,
                    "reason": file.reason,
                    "scheduled_at": file.scheduled_at.isoformat()
                    if file.scheduled_at
                    else None,
                    "referenced_by": file.referenced_by,
                }
                for file in self.doomed_files
            ],
            "s3_checked": self.s3_checked,
            "s3_missing": self.s3_missing,
            "recommendation": self.recommendation,
            "warning": self.warning,
            "warnings": self.warnings,
            "messages": [
                {
                    "severity": m.severity.value,
                    "message": m.message,
                    "hint": m.hint,
                }
                for m in self.messages
            ],
            "iceberg": self.iceberg,
            "exit_code": int(self.exit_code),
        }


class RetentionError(DuckLakeIcebergError):
    """Failure to query the policy, simulate maintenance, or check S3."""


def _seconds(value: timedelta) -> int:
    return int(value.total_seconds())


def parse_duration(value: str) -> timedelta:
    text = value.strip().lower()
    match = _DURATION_RE.fullmatch(text)
    if match is None or not any(match.groups()):
        raise RetentionError(f"invalid retention duration: {value!r}")
    days, hours, minutes, seconds = (int(part or 0) for part in match.groups())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def format_duration(value: timedelta) -> str:
    """Readable duration with the two largest units: ``6d4h``, ``40min``, ``-12h``."""
    total = int(value.total_seconds())
    sign = "-" if total < 0 else ""
    total = abs(total)
    amounts: list[tuple[int, str]] = []
    for unit, size in (("d", 86_400), ("h", 3_600), ("min", 60), ("s", 1)):
        amount, total = divmod(total, size)
        amounts.append((amount, unit))
    first = next((i for i, (amount, _) in enumerate(amounts) if amount), None)
    if first is None:
        return "0s"
    parts = [f"{amounts[first][0]}{amounts[first][1]}"]
    if first + 1 < len(amounts) and amounts[first + 1][0]:
        parts.append(f"{amounts[first + 1][0]}{amounts[first + 1][1]}")
    return sign + "".join(parts)


def _ceil(value: timedelta) -> timedelta:
    """Round up to the hour (>= 1 h) or the minute, for the recommendation."""
    step = _HOUR if value >= _HOUR else _MINUTE
    return step * math.ceil(value / step) if value > timedelta() else timedelta()


def _utc_from_us(value: object) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(int(str(value)) / 1_000_000, tz=UTC)


# -- policy discovery -----------------------------------------------------------


def _catalog_options(con: DuckLakeConnection) -> list[dict[str, Any]]:
    """Scan ``ducklake_options`` by name pattern rather than by a fixed list."""
    clauses = " OR ".join("lower(option_name) LIKE ?" for _ in _OPTION_PATTERNS)
    try:
        rows = con.raw.execute(
            "SELECT option_name, value, scope, scope_entry "
            f"FROM ducklake_options(?) WHERE {clauses} "
            "ORDER BY option_name, scope, scope_entry",
            [con.alias, *_OPTION_PATTERNS],
        ).fetchall()
    except Exception as exc:
        raise RetentionError(
            f"failed to discover DuckLake retention options: {exc}"
        ) from exc
    return [
        {
            "option_name": str(row[0]),
            "value": None if row[1] is None else str(row[1]),
            "scope": None if row[2] is None else str(row[2]),
            "scope_entry": None if row[3] is None else str(row[3]),
        }
        for row in rows
    ]


def _option_duration(con: DuckLakeConnection, value: object) -> timedelta | None:
    """Option value as a DuckDB ``INTERVAL`` (``2 days``, ``1 week``...)."""
    if value is None:
        return None
    try:
        rows = con.raw.execute("SELECT epoch(CAST(? AS INTERVAL))", [str(value)])
        seconds = rows.fetchall()[0][0]
    except Exception:
        try:
            return parse_duration(str(value))
        except RetentionError:
            return None
    duration = timedelta(seconds=float(seconds))
    return duration if duration > timedelta() else None


def _catalog_windows(
    con: DuckLakeConnection, options: list[dict[str, Any]]
) -> dict[str, timedelta]:
    """Windows declared in the catalog, global scope only.

    ``expire_older_than`` (used by the checkpoint's argument-less expire)
    becomes R; ``delete_older_than`` (used by ``cleanup_old_files``) becomes C.
    Numeric options such as ``rewrite_delete_threshold`` are ignored.
    """
    windows: dict[str, timedelta] = {}
    for option in options:
        name = str(option["option_name"]).lower()
        if "threshold" in name or option.get("scope") not in (None, "GLOBAL"):
            continue
        if "expir" in name or "retention" in name:
            key = "expire"
        elif "cleanup" in name or "delete" in name:
            key = "cleanup"
        else:  # pragma: no cover - the SQL filter already restricts the names
            continue
        duration = _option_duration(con, option["value"])
        if duration is not None:
            windows[key] = duration
    return windows


def infer_policy_from_history(
    con: DuckLakeConnection, *, now: datetime | None = None
) -> RetentionPolicy:
    """Observed retention: a sequential ``snapshot_id`` with gaps proves expiration."""
    current = now or datetime.now(UTC)
    try:
        rows = con.raw.execute(
            "SELECT min(snapshot_id), max(snapshot_id), count(*), "
            "max(snapshot_id) - min(snapshot_id) + 1 - count(*), "
            "epoch_us(min(CAST(snapshot_time AS TIMESTAMPTZ))) "
            f"FROM {quote_ident(con.meta)}.ducklake_snapshot"
        ).fetchall()
    except Exception as exc:
        raise RetentionError(f"failed to read the snapshot history: {exc}") from exc
    row = rows[0] if rows else None
    if row is None or row[0] is None:
        return RetentionPolicy(
            timedelta(), timedelta(), RetentionSource.INFERRED, "unknown"
        )
    oldest, newest, count, missing_ids, oldest_us = row
    oldest_time = _utc_from_us(oldest_us) or current
    observed = max(current - oldest_time, timedelta())
    expired = int(oldest) > 0 or int(missing_ids) > 0
    return RetentionPolicy(
        observed if expired else timedelta(),
        timedelta(),
        RetentionSource.INFERRED,
        "observed" if expired else "unknown",
        {
            "oldest_retained_snapshot": int(oldest),
            "newest_snapshot": int(newest),
            "oldest_retained_snapshot_time": oldest_time.isoformat(),
            "retained_count": int(count),
            "missing_snapshot_ids": int(missing_ids),
            "expiration_observed": expired,
            "observed_retention_seconds": _seconds(observed),
        },
    )


def resolve_policy(
    con: DuckLakeConnection,
    *,
    configured: RetentionSettings | None,
    now: datetime | None = None,
) -> tuple[RetentionPolicy, list[dict[str, Any]], timedelta, timedelta, float]:
    """Precedence: config > catalog > inference.

    The inference never overrides a declaration: it stays in ``evidence`` for
    the inconsistency check.
    """
    options = _catalog_options(con)
    inferred = infer_policy_from_history(con, now=now)
    catalog_values = _catalog_windows(con, options)

    declared_expire = (
        parse_duration(configured.expire_window)
        if configured and configured.expire_window
        else None
    )
    declared_cleanup = (
        parse_duration(configured.cleanup_window)
        if configured and configured.cleanup_window
        else None
    )
    expire = declared_expire or catalog_values.get("expire") or inferred.expire_window
    cleanup = declared_cleanup or catalog_values.get("cleanup") or timedelta()
    confidence: Literal["declared", "observed", "unknown"]
    if declared_expire is not None or declared_cleanup is not None:
        source, confidence = RetentionSource.CONFIG, "declared"
    elif catalog_values:
        source, confidence = RetentionSource.CATALOG, "declared"
    else:
        source, confidence = inferred.source, inferred.confidence
    if confidence == "declared" and expire <= timedelta():
        # only C declared: R comes from nowhere
        confidence = "unknown"

    evidence = dict(inferred.evidence)
    evidence["catalog_options"] = options
    for key, value in catalog_values.items():
        evidence[f"catalog_{key}_window_seconds"] = _seconds(value)
    if configured:
        interval = parse_duration(configured.sync_interval)
        p95 = parse_duration(configured.sync_p95)
        factor = configured.safety_factor
    else:
        interval, p95, factor = timedelta(hours=1), timedelta(minutes=10), 2.0
    policy = RetentionPolicy(expire, cleanup, source, confidence, evidence)
    return policy, options, interval, p95, factor


# -- risk window ----------------------------------------------------------------


def compute_risk_window(
    policy: RetentionPolicy,
    sync: SyncProfile,
    published_at: datetime,
    *,
    safety_factor: float = 2.0,
    now: datetime | None = None,
    published_snapshot_id: int = 0,
) -> RiskWindow:
    """``margin = (R + C) - (A + I + D)`` and the ``older_than`` recommendation.

    The recommendation uses ``A_max = max(A, I + D)``: the published snapshot
    may be older than one cycle (idle table), and a recommendation below the
    current age would not be actionable. With the recommended ``R`` the margin
    is ``k * (I + D)``, above the warning threshold ``k * I``.
    """
    current = now or datetime.now(UTC)
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=UTC)
    age = max(current - published_at, timedelta())
    safe_window = policy.expire_window + policy.cleanup_window
    margin = safe_window - (age + sync.cycle)
    impossible = safe_window <= sync.cycle
    if policy.confidence == "unknown":
        severity = Severity.WARNING
    elif impossible or margin <= timedelta():
        severity = Severity.CRITICAL
    elif margin < safety_factor * sync.interval:
        severity = Severity.WARNING
    else:
        severity = Severity.OK
    age_max = max(age, sync.cycle)
    recommended = _ceil(
        max(
            age_max + sync.cycle + safety_factor * sync.cycle - policy.cleanup_window,
            timedelta(),
        )
    )
    return RiskWindow(
        published_snapshot_id,
        published_at,
        age,
        margin,
        severity,
        recommended,
    )


# -- maintenance simulation -----------------------------------------------------

#: simulation results per connection: they are global to the catalog and the
#: ``validate`` connection reads in a single transaction, so ``--all-tables``
#: does not need to repeat the orphan listing (a LIST over the whole
#: ``data_path``).
_SIMULATIONS: weakref.WeakKeyDictionary[
    DuckLakeConnection, dict[tuple[int, int], tuple[list[int], list[str]]]
] = weakref.WeakKeyDictionary()


def _call_rows(
    con: DuckLakeConnection, function: str, column: str, args: str, params: list[Any]
) -> list[tuple[Any, ...]]:
    # only the used column: `snapshot_time` (TIMESTAMPTZ) would require pytz on fetch
    try:
        return con.raw.execute(
            f"SELECT {column} FROM {function}(?, {args}, dry_run => true)",
            [con.alias, *params],
        ).fetchall()
    except Exception as exc:
        raise RetentionError(
            f"{function} failed: {exc}",
            hint="check the ducklake extension version",
        ) from exc


def simulate_maintenance(
    con: DuckLakeConnection,
    *,
    policy: RetentionPolicy,
    now: datetime | None = None,
) -> tuple[list[int], list[str]]:
    """(snapshots that would expire, paths that would be removed).

    The three functions always run with ``dry_run => true``:

    - ``ducklake_expire_snapshots`` with ``older_than = now - R``, only when
      ``R`` is known (with an unknown ``R`` every non-current snapshot would
      "expire", which tells nothing);
    - ``ducklake_cleanup_old_files`` and ``ducklake_delete_orphaned_files`` with
      ``older_than = now - C``; with an unknown ``C``, ``cleanup_all => true``
      returns the worst case.
    """
    current = now or datetime.now(UTC)
    expire_known = policy.confidence != "unknown" and policy.expire_window > timedelta()
    key = (
        _seconds(policy.expire_window) if expire_known else -1,
        _seconds(policy.cleanup_window),
    )
    try:
        cached = _SIMULATIONS.setdefault(con, {})
    except TypeError:  # connection without weakref support (test doubles)
        cached = {}
    if key in cached:
        return cached[key]
    doomed_snapshots: list[int] = []
    if expire_known:
        rows = _call_rows(
            con,
            "ducklake_expire_snapshots",
            "snapshot_id",
            "older_than => CAST(? AS TIMESTAMPTZ)",
            [(current - policy.expire_window).isoformat()],
        )
        doomed_snapshots = sorted(int(str(row[0])) for row in rows)
    if policy.cleanup_window > timedelta():
        args = "older_than => CAST(? AS TIMESTAMPTZ)"
        params: list[Any] = [(current - policy.cleanup_window).isoformat()]
    else:
        args, params = "cleanup_all => true", []
    doomed_paths: set[str] = set()
    for function in ("ducklake_cleanup_old_files", "ducklake_delete_orphaned_files"):
        rows = _call_rows(con, function, "path", args, params)
        doomed_paths.update(canonical_uri(str(row[0])) for row in rows if row[0])
    result = (doomed_snapshots, sorted(doomed_paths))
    cached[key] = result
    return result


def iceberg_files(table: IcebergTable, *, max_workers: int = 8) -> IcebergFiles:
    """Files cited by the current snapshot and by all retained snapshots.

    Reads each manifest once (manifests are shared across snapshots) and only
    the ``file_path`` of live entries; covers data files and delete files.
    """
    try:
        metadata = table.metadata
        io = table.io
        current_id = metadata.current_snapshot_id
        manifests: dict[str, Any] = {}
        current_manifests: set[str] = set()
        snapshot_ids = {snapshot.snapshot_id for snapshot in metadata.snapshots}
        history_ms: list[int] = []
        expired = False
        for snapshot in metadata.snapshots:
            if snapshot.snapshot_id != current_id:
                history_ms.append(snapshot.timestamp_ms)
            parent = snapshot.parent_snapshot_id
            expired = expired or (parent is not None and parent not in snapshot_ids)
            for manifest in snapshot.manifests(io):
                manifests.setdefault(manifest.manifest_path, manifest)
                if snapshot.snapshot_id == current_id:
                    current_manifests.add(manifest.manifest_path)

        def paths(manifest: Any) -> set[str]:
            return {
                canonical_uri(str(entry.data_file.file_path))
                for entry in manifest.fetch_manifest_entry(io, discard_deleted=True)
            }

        with ContextThreadPoolExecutor(max_workers=max_workers) as pool:
            by_manifest = dict(
                zip(manifests, pool.map(paths, manifests.values()), strict=True)
            )
    except Exception as exc:
        raise RetentionError(f"failed to list referenced Iceberg files: {exc}") from exc
    current: set[str] = set()
    retained: set[str] = set()
    for path, files in by_manifest.items():
        retained |= files
        if path in current_manifests:
            current |= files
    return IcebergFiles(
        current=current,
        retained=retained,
        retained_snapshots=len(snapshot_ids),
        oldest_history_ms=min(history_ms) if history_ms else None,
        expired_evidence=expired,
    )


def referenced_data_files(table: IcebergTable) -> set[str]:
    """Files of the current snapshot (data and delete files), normalized."""
    return iceberg_files(table).current


def _scheduled_files(
    con: DuckLakeConnection,
    *,
    table_id: int,
    snapshot_id: int,
) -> list[tuple[set[str], datetime | None]]:
    """The ``ducklake_files_scheduled_for_deletion`` queue with candidate paths.

    The queue's relative path is relative to the global ``data_path``
    (``main/t/ducklake-….parquet``), unlike ``ducklake_data_file``, which is
    relative to the table path; both compositions are included as candidates so
    as not to depend on the extension version.
    """
    try:
        rows = con.raw.execute(
            "SELECT path, path_is_relative, "
            "epoch_us(CAST(schedule_start AS TIMESTAMPTZ)) "
            f"FROM {quote_ident(con.meta)}.ducklake_files_scheduled_for_deletion"
        ).fetchall()
        layers = read_path_layers(con.raw, con.meta, table_id, snapshot_id)
    except Exception as exc:
        raise RetentionError(
            f"failed to read the DuckLake removal queue: {exc}"
        ) from exc
    scheduled: list[tuple[set[str], datetime | None]] = []
    for path, relative, timestamp in rows:
        text = str(path)
        if bool(relative):
            candidates = {
                canonical_uri(compose_path(layers, text, True)),
                canonical_uri(layers.data_path.rstrip("/") + "/" + text.lstrip("/")),
            }
        else:
            candidates = {canonical_uri(text)}
        scheduled.append((candidates, _utc_from_us(timestamp)))
    return scheduled


def check_s3_existence(
    paths: list[str],
    *,
    mode: Literal["off", "sample", "full"],
    sample_size: int = 100,
    max_workers: int = 8,
    region: str = "us-east-1",
    profile: str | None = None,
) -> tuple[list[str], int]:
    """(missing URIs, HEADs made). Sample = smallest ``md5(uri)`` values."""
    if mode == "off" or not paths:
        return [], 0
    ordered = sorted(
        set(paths),
        # MD5 is the deterministic (non-cryptographic) ordering of the sample.
        key=lambda path: (hashlib.md5(path.encode()).hexdigest(), path),  # noqa: S324
    )
    selected = ordered if mode == "full" else ordered[:sample_size]
    client = aws.client("s3", region, profile)

    def exists(path: str) -> str | None:
        if not path.startswith("s3://"):
            return None if Path(path).exists() else path
        bucket, _, key = path.removeprefix("s3://").partition("/")
        try:
            client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
                return path
            if code in {"403", "AccessDenied"} or status == 403:
                raise AwsError(
                    f"AccessDenied while checking {path}",
                    hint="review the IAM read policy (s3:GetObject)",
                ) from exc
            raise AwsError(f"failed to check {path}: {code or exc}") from exc
        except BotoCoreError as exc:
            raise AwsError(f"failed to check {path}: {type(exc).__name__}") from exc
        return None

    with ContextThreadPoolExecutor(max_workers=max_workers) as pool:
        missing = [result for result in pool.map(exists, selected) if result]
    return sorted(missing), len(selected)


# -- evaluation -----------------------------------------------------------------


def policy_inconsistencies(
    policy: RetentionPolicy, sync: SyncProfile
) -> list[RetentionMessage]:
    """Material divergence between the policy in use and the other sources.

    Warns in both directions: declared larger than observed (the real window is
    shorter and the computed margin is optimistic - the dangerous case) or
    smaller (maintenance is not running as declared). Tolerance:
    ``max(I + D, R/10)``, because expiration runs in batches and the observed
    age fluctuates by one cycle.
    """
    if policy.source is RetentionSource.INFERRED or policy.confidence == "unknown":
        return []
    tolerance = max(sync.cycle, policy.expire_window / 10)
    messages: list[RetentionMessage] = []
    evidence = policy.evidence
    observed = evidence.get("observed_retention_seconds")
    if evidence.get("expiration_observed") and isinstance(observed, int):
        window = timedelta(seconds=observed)
        if abs(policy.expire_window - window) > tolerance:
            messages.append(
                RetentionMessage(
                    Severity.WARNING,
                    f"declared retention {format_duration(policy.expire_window)} "
                    f"differs from the one observed in the history "
                    f"{format_duration(window)}",
                    "check the actual older_than of the DuckLake maintenance; "
                    "the computation uses the declared one",
                )
            )
    if policy.source is RetentionSource.CONFIG:
        for key, label, value in (
            ("expire", "expire_window", policy.expire_window),
            ("cleanup", "cleanup_window", policy.cleanup_window),
        ):
            seconds = evidence.get(f"catalog_{key}_window_seconds")
            if not isinstance(seconds, int) or value <= timedelta():
                continue
            option = timedelta(seconds=seconds)
            if abs(value - option) > tolerance:
                messages.append(
                    RetentionMessage(
                        Severity.WARNING,
                        f"retention.{label} {format_duration(value)} differs from "
                        f"the catalog option {format_duration(option)}",
                        "align ducklake-iceberg.yaml with ducklake_set_option",
                    )
                )
    return messages


def iceberg_retention_check(
    policy: RetentionPolicy,
    sync: SyncProfile,
    files: IcebergFiles,
    *,
    iceberg_retention_hours: int | _Unset | None = UNSET,
    now: datetime | None = None,
) -> tuple[dict[str, Any], RetentionMessage | None, timedelta | None]:
    """``delete_older_than`` (C) against the Iceberg snapshot retention (H).

    The checkpoint compaction schedules the merged files immediately, while
    earlier Iceberg snapshots keep citing them until they expire; and Iceberg
    expiration only runs on the next ``export``. Safe if ``C >= H + I + D``.
    ``H`` comes from ``snapshot_retention_hours`` when the caller provides it
    (``None`` = keeps everything); otherwise it is the age of the oldest
    retained snapshot (a lower bound of what Iceberg keeps).

    Returns (evidence, warning or ``None``, recommended ``cleanup_window``).
    """
    current = now or datetime.now(UTC)
    observed = (
        max(
            current - datetime.fromtimestamp(files.oldest_history_ms / 1000, tz=UTC),
            timedelta(),
        )
        if files.oldest_history_ms is not None
        else timedelta()
    )
    evidence: dict[str, Any] = {
        "retained_snapshots": files.retained_snapshots,
        "retained_files": len(files.retained),
        "history_only_files": len(files.history_only),
        "oldest_history_age_seconds": _seconds(observed),
        "expiration_observed": files.expired_evidence,
    }
    cleanup = policy.cleanup_window
    if isinstance(iceberg_retention_hours, _Unset):
        evidence["snapshot_retention_source"] = "observed"
        evidence["snapshot_retention_seconds"] = _seconds(observed)
        required = observed
        label = f"retained Iceberg history {format_duration(observed)}"
    elif iceberg_retention_hours is None:
        evidence["snapshot_retention_source"] = "config"
        evidence["snapshot_retention_seconds"] = None
        if cleanup <= timedelta():
            return evidence, None, None
        return (
            evidence,
            RetentionMessage(
                Severity.WARNING,
                "Iceberg keeps the whole history (snapshot_retention_hours unset); "
                f"the DuckLake cleanup ({format_duration(cleanup)}) will delete "
                "files cited by old Iceberg snapshots",
                "set snapshot_retention_hours below cleanup_window "
                "(delete_older_than) minus one sync cycle",
            ),
            None,
        )
    else:
        retention = timedelta(hours=iceberg_retention_hours)
        evidence["snapshot_retention_source"] = "config"
        evidence["snapshot_retention_seconds"] = _seconds(retention)
        required = retention + sync.cycle
        label = (
            f"Iceberg snapshot retention {format_duration(retention)} "
            f"+ sync cycle {format_duration(sync.cycle)}"
        )
    if cleanup <= timedelta():
        evidence["check"] = "skipped: cleanup_window unknown"
        return evidence, None, None
    if required <= cleanup:
        return evidence, None, None
    recommended = _ceil(required)
    return (
        evidence,
        RetentionMessage(
            Severity.WARNING,
            f"cleanup_window {format_duration(cleanup)} < {label}",
            f"cleanup_old_files/delete_older_than older_than >= "
            f"{format_duration(recommended)}, or reduce snapshot_retention_hours",
        ),
        recommended,
    )


def _recommendation(
    policy: RetentionPolicy,
    sync: SyncProfile,
    risk: RiskWindow,
    cleanup_recommended: timedelta | None,
) -> str:
    """Actionable sentence with the context that justifies it."""
    current = (
        format_duration(policy.expire_window)
        if policy.confidence != "unknown" and policy.expire_window > timedelta()
        else "unknown"
    )
    text = (
        f"expire_snapshots older_than >= "
        f"{format_duration(risk.recommended_expire_window)} "
        f"(current {current}; sync every {format_duration(sync.interval)}, "
        f"p95 {format_duration(sync.p95_duration)}, published snapshot aged "
        f"{format_duration(risk.published_age)})"
    )
    if cleanup_recommended is not None:
        text += (
            f"; cleanup_old_files older_than >= "
            f"{format_duration(cleanup_recommended)} "
            f"(current {format_duration(policy.cleanup_window)})"
        )
    return text


def _margin_message(
    policy: RetentionPolicy, sync: SyncProfile, risk: RiskWindow, factor: float
) -> RetentionMessage:
    if policy.confidence == "unknown":
        return RetentionMessage(
            Severity.WARNING,
            "unknown retention policy",
            "declare retention.expire_window in the config; the absence of "
            "observed expiration is not a guarantee of policy",
        )
    safe = policy.expire_window + policy.cleanup_window
    used = risk.published_age + sync.cycle
    margin = format_duration(risk.margin)
    raise_hint = (
        f"raise older_than to >= "
        f"{format_duration(risk.recommended_expire_window)} before the next sync"
    )
    if safe <= sync.cycle:
        return RetentionMessage(
            Severity.CRITICAL,
            f"impossible policy: R+C={format_duration(safe)} <= "
            f"I+D={format_duration(sync.cycle)}",
            raise_hint,
        )
    if risk.margin <= timedelta():
        return RetentionMessage(
            Severity.CRITICAL, f"insufficient retention: margin {margin}", raise_hint
        )
    threshold = factor * sync.interval
    if risk.margin < threshold:
        factor_text = f"{factor:g}"
        return RetentionMessage(
            Severity.WARNING,
            f"margin {margin} < {format_duration(threshold)} "
            f"({factor_text}× sync interval)",  # noqa: RUF001 (multiplication sign)
            "raise older_than or reduce the sync interval",
        )
    return RetentionMessage(
        Severity.OK,
        f"retention ok: margin {margin} (R+C={format_duration(safe)}, "
        f"A+I+D={format_duration(used)})",
    )


def _doomed_messages(
    doomed: list[DoomedFile], policy: RetentionPolicy
) -> list[RetentionMessage]:
    messages: list[RetentionMessage] = []
    current = [f for f in doomed if f.referenced_by == "current"]
    removal = [f for f in current if f.reason in {"cleanup", "scheduled"}]
    if removal:
        messages.append(
            RetentionMessage(
                Severity.CRITICAL,
                f"{len(removal)} referenced files scheduled for removal",
                "suspend the cleanup; run ducklake-iceberg sync; follow the recovery "
                "runbook",
            )
        )
    for file in [f for f in current if f.reason == "missing_s3"][:_MAX_DISPLAY_FILES]:
        detail = "NoSuchKey" if file.path.startswith("s3://") else "missing"
        messages.append(
            RetentionMessage(
                Severity.CRITICAL,
                f"{file.path}: {detail}",
                "follow the recovery runbook",
            )
        )
    history = [f for f in doomed if f.referenced_by == "history"]
    # with a known C, "cleanup" = eligible in the next cleanup: concrete risk
    eligible = [f for f in history if f.reason == "cleanup" and policy.cleanup_window]
    pending = [f for f in history if f not in eligible]
    if eligible:
        messages.append(
            RetentionMessage(
                Severity.CRITICAL,
                f"{len(eligible)} files cited by retained Iceberg snapshots "
                "would be removed by the next cleanup (time travel)",
                "suspend the cleanup or reduce snapshot_retention_hours and "
                "run ducklake-iceberg sync",
            )
        )
    if pending:
        messages.append(
            RetentionMessage(
                Severity.WARNING,
                f"{len(pending)} files cited by retained Iceberg snapshots "
                "are scheduled for removal (time travel)",
                "cleanup_window must cover the Iceberg snapshot retention",
            )
        )
    return messages


def validate_retention(
    con: DuckLakeConnection,
    table: IcebergTable,
    *,
    table_id: int,
    snapshot_id: int,
    table_name: str,
    configured: RetentionSettings | None,
    strict: bool = False,
    check_s3: Literal["off", "sample", "full"] = "off",
    s3_sample: int = 100,
    max_io_workers: int = 8,
    region: str = "us-east-1",
    profile: str | None = None,
    layers_snapshot_id: int | None = None,
    iceberg_retention_hours: int | _Unset | None = UNSET,
    now: datetime | None = None,
) -> RetentionReport:
    """Full retention check for a published table."""
    current_time = now or datetime.now(UTC)
    policy, options, interval, p95, factor = resolve_policy(
        con, configured=configured, now=current_time
    )
    sync = SyncProfile(interval, p95)
    try:
        snapshot_rows = con.raw.execute(
            "SELECT epoch_us(CAST(snapshot_time AS TIMESTAMPTZ)) "
            f"FROM {quote_ident(con.meta)}.ducklake_snapshot WHERE snapshot_id = ?",
            [snapshot_id],
        ).fetchall()
    except Exception as exc:
        raise RetentionError(f"failed to query the published snapshot: {exc}") from exc
    present = bool(snapshot_rows)
    published_at = (
        _utc_from_us(snapshot_rows[0][0]) if present else None
    ) or current_time
    risk = compute_risk_window(
        policy,
        sync,
        published_at,
        safety_factor=factor,
        now=current_time,
        published_snapshot_id=snapshot_id,
    )
    messages: list[RetentionMessage] = [_margin_message(policy, sync, risk, factor)]
    if not present:
        messages = [
            RetentionMessage(
                Severity.CRITICAL,
                f"published snapshot {snapshot_id} no longer exists in DuckLake",
                "run ducklake-iceberg sync right away; review the retention policy",
            )
        ]
    doomed_snapshots, simulated = simulate_maintenance(
        con, policy=policy, now=current_time
    )
    if snapshot_id in doomed_snapshots:
        messages.append(
            RetentionMessage(
                Severity.CRITICAL,
                f"published snapshot {snapshot_id} expires in the next maintenance",
                "postpone the maintenance or run ducklake-iceberg sync first",
            )
        )
    files = iceberg_files(table, max_workers=max_io_workers)
    referenced = files.current
    history_only = files.history_only
    scheduled = _scheduled_files(
        con,
        table_id=table_id,
        snapshot_id=layers_snapshot_id or snapshot_id,
    )
    doomed: dict[str, DoomedFile] = {}
    removable = set(simulated)
    for path in sorted(files.retained & removable):
        scope: Literal["current", "history"] = (
            "current" if path in referenced else "history"
        )
        doomed[path] = DoomedFile(path, "cleanup", referenced_by=scope)
    for candidates, scheduled_at in scheduled:
        for path in candidates & files.retained:
            if path in doomed and doomed[path].reason == "cleanup":
                previous = doomed[path]
                if previous.referenced_by == "history":
                    # eligible now: keep the more severe reason, note the date
                    doomed[path] = DoomedFile(
                        path, "cleanup", scheduled_at, referenced_by="history"
                    )
                    continue
            scope = "current" if path in referenced else "history"
            doomed[path] = DoomedFile(path, "scheduled", scheduled_at, scope)
    s3_missing, s3_checked = check_s3_existence(
        sorted(referenced),
        mode=check_s3,
        sample_size=s3_sample,
        max_workers=max_io_workers,
        region=region,
        profile=profile,
    )
    for path in s3_missing:
        doomed[path] = DoomedFile(path, "missing_s3")
    doomed_files = sorted(doomed.values(), key=lambda item: (item.path, item.reason))
    messages.extend(_doomed_messages(doomed_files, policy))
    if not history_only and not referenced:
        messages.append(
            RetentionMessage(
                Severity.WARNING,
                "table has no referenced files (empty or with still-inlined data)",
                "run ducklake-iceberg checkpoint to flush the data",
            )
        )
    messages.extend(policy_inconsistencies(policy, sync))
    iceberg_evidence, iceberg_message, cleanup_recommended = iceberg_retention_check(
        policy,
        sync,
        files,
        iceberg_retention_hours=iceberg_retention_hours,
        now=current_time,
    )
    if iceberg_message is not None:
        messages.append(iceberg_message)

    severity = risk.severity
    for message in messages:
        if message.severity is Severity.CRITICAL:
            severity = Severity.CRITICAL
    if not present or severity is not risk.severity:
        risk = RiskWindow(
            risk.published_snapshot_id,
            risk.published_at,
            risk.published_age,
            risk.margin,
            Severity.CRITICAL if not present else severity,
            risk.recommended_expire_window,
        )
    return RetentionReport(
        policy=policy,
        sync=sync,
        risk=risk,
        published_snapshot_present=present,
        doomed_snapshots=doomed_snapshots,
        referenced_files=len(referenced),
        doomed_files=doomed_files,
        s3_checked=s3_checked,
        s3_missing=s3_missing,
        recommendation=_recommendation(policy, sync, risk, cleanup_recommended),
        table=table_name,
        catalog_options=options,
        messages=sorted(messages, key=lambda m: -_RANK[m.severity]),
        iceberg=iceberg_evidence,
        strict=strict,
    )


# -- output ---------------------------------------------------------------------

_STYLE: Final[dict[str, str]] = {
    "ok": "green",
    "warning": "yellow",
    "critical": "red",
}


def _fmt_seconds(value: object) -> str:
    if not isinstance(value, int | float):
        return "-"
    return format_duration(timedelta(seconds=float(value)))


def render_retention(results: list[dict[str, Any]]) -> None:
    """Terminal output; also accepts the ``no_snapshot_result`` result."""
    console = Console()
    for result in results:
        title = f"ducklake-iceberg validate · retention · {result.get('table', '')}"
        policy = result.get("policy")
        risk = result.get("risk")
        sync = result.get("sync")
        if not isinstance(policy, dict) or not isinstance(risk, dict):
            console.print(f"[bold]{title}[/bold]")
            if result.get("warning"):
                console.print(f"[yellow]{result['warning']}[/yellow]")
            if result.get("recommendation"):
                console.print(f"action: {result['recommendation']}")
            continue
        sync = sync if isinstance(sync, dict) else {}
        table = RichTable(title=title, title_justify="left")
        table.add_column("field")
        table.add_column("value")
        table.add_row("policy", f"{policy.get('source')} ({policy.get('confidence')})")
        table.add_row(
            "windows",
            f"R={_fmt_seconds(policy.get('expire_window_seconds'))} "
            f"C={_fmt_seconds(policy.get('cleanup_window_seconds'))}",
        )
        table.add_row(
            "sync",
            f"every {_fmt_seconds(sync.get('interval_seconds'))}, "
            f"p95 {_fmt_seconds(sync.get('p95_duration_seconds'))}",
        )
        table.add_row(
            "published snapshot",
            f"{risk.get('published_snapshot_id')} "
            f"(age {_fmt_seconds(risk.get('published_age_seconds'))}"
            + ("" if result.get("published_snapshot_present") else ", expired")
            + ")",
        )
        table.add_row("margin", _fmt_seconds(risk.get("margin_seconds")))
        severity = str(risk.get("severity", "-"))
        table.add_row("risk", f"[{_STYLE.get(severity, 'white')}]{severity}[/]")
        table.add_row("referenced files", str(result.get("referenced_files")))
        iceberg = result.get("iceberg") or {}
        if iceberg:
            table.add_row(
                "retained Iceberg snapshots",
                f"{iceberg.get('retained_snapshots')} "
                f"({iceberg.get('retained_files')} files)",
            )
        table.add_row("files at risk", str(len(result.get("doomed_files", []))))
        table.add_row("S3 checked (HEAD)", str(result.get("s3_checked")))
        console.print(table)
        for message in result.get("messages", []):
            level = str(message.get("severity"))
            prefix = "warning: " if level == "warning" else ""
            line = f"[{_STYLE.get(level, 'white')}]{prefix}{message['message']}[/]"
            console.print(line)
            if message.get("hint"):
                console.print(f"  action: {message['hint']}")
        console.print(f"recommendation: {result.get('recommendation')}")
        doomed = result.get("doomed_files", [])
        for file in doomed[:_MAX_DISPLAY_FILES]:
            console.print(
                f"[red]{file['reason']} ({file.get('referenced_by', 'current')}): "
                f"{file['path']}[/red]"
            )
        if len(doomed) > _MAX_DISPLAY_FILES:
            console.print(
                f"… {len(doomed) - _MAX_DISPLAY_FILES} more files (full list in --json)"
            )


def no_snapshot_result(table_name: str) -> dict[str, Any]:
    """Empty report when no Iceberg snapshot exists yet (exit 4)."""
    return {
        "table": table_name,
        "policy": None,
        "sync": None,
        "risk": None,
        "published_snapshot_present": False,
        "doomed_snapshots": [],
        "referenced_files": 0,
        "doomed_files": [],
        "s3_checked": 0,
        "s3_missing": [],
        "recommendation": "publish the table with 'ducklake-iceberg sync'",
        "warning": "nothing to check: table has no published snapshot",
        "warnings": ["nothing to check: table has no published snapshot"],
        "messages": [
            {
                "severity": Severity.WARNING.value,
                "message": "nothing to check: table has no published snapshot",
                "hint": "ducklake-iceberg sync",
            }
        ],
        "exit_code": int(ExitCode.NOTHING_TO_DO),
    }
