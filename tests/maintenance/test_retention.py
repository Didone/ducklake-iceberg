from __future__ import annotations

import hashlib
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Literal
from unittest import mock

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from botocore.exceptions import ClientError
from moto import mock_aws
from pydantic import ValidationError

from ducklake_iceberg import aws
from ducklake_iceberg.config import RetentionSettings
from ducklake_iceberg.errors import AwsError, ExitCode
from ducklake_iceberg.retention import (
    DoomedFile,
    IcebergFiles,
    RetentionMessage,
    RetentionPolicy,
    RetentionReport,
    RetentionSource,
    Severity,
    SyncProfile,
    _margin_message,
    _recommendation,
    check_s3_existence,
    compute_risk_window,
    format_duration,
    iceberg_retention_check,
    no_snapshot_result,
    parse_duration,
    policy_inconsistencies,
    render_retention,
)


def test_parse_duration_supports_compound_and_word_units() -> None:
    assert parse_duration("2d 3h 4m 5s") == timedelta(
        days=2, hours=3, minutes=4, seconds=5
    )
    assert parse_duration("7 days") == timedelta(days=7)


@pytest.mark.parametrize("value", ["", "0s", "tomorrow", "2h invalid"])
def test_retention_settings_reject_invalid_durations(value: str) -> None:
    with pytest.raises(ValidationError, match="duration"):
        RetentionSettings(expire_window=value)


def test_compute_risk_window_reports_safe_margin_and_recommendation() -> None:
    now = datetime(2026, 11, 5, tzinfo=UTC)
    policy = RetentionPolicy(
        timedelta(days=14),
        timedelta(),
        RetentionSource.CONFIG,
        "declared",
    )
    risk = compute_risk_window(
        policy,
        SyncProfile(timedelta(hours=1), timedelta(minutes=4)),
        now - timedelta(days=2),
        now=now,
        safety_factor=2,
        published_snapshot_id=42,
    )
    assert risk.severity is Severity.OK
    assert risk.margin == timedelta(days=11, hours=22, minutes=56)
    assert risk.published_snapshot_id == 42
    assert risk.recommended_expire_window > timedelta()


def test_compute_risk_window_detects_impossible_policy() -> None:
    risk = compute_risk_window(
        RetentionPolicy(
            timedelta(hours=1),
            timedelta(),
            RetentionSource.CONFIG,
            "declared",
        ),
        SyncProfile(timedelta(hours=24)),
        datetime.now(UTC),
    )
    assert risk.severity is Severity.CRITICAL


def test_check_s3_is_missing_and_permission_errors_are_distinct() -> None:
    class FakeS3:
        def head_object(self, **kwargs: str) -> None:
            if kwargs["Key"] == "missing":
                raise ClientError(
                    {
                        "Error": {"Code": "404"},
                        "ResponseMetadata": {"HTTPStatusCode": 404},
                    },
                    "HeadObject",
                )
            raise ClientError(
                {"Error": {"Code": "403"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
                "HeadObject",
            )

    paths = ["s3://bucket/missing", "s3://bucket/forbidden"]
    with (
        mock.patch("ducklake_iceberg.retention.aws.client", return_value=FakeS3()),
        pytest.raises(AwsError, match="AccessDenied"),
    ):
        check_s3_existence(
            paths,
            mode="full",
            region="us-east-1",
        )

    with mock.patch(
        "ducklake_iceberg.retention.aws.client",
        return_value=mock.Mock(
            head_object=mock.Mock(
                side_effect=ClientError(
                    {
                        "Error": {"Code": "404"},
                        "ResponseMetadata": {"HTTPStatusCode": 404},
                    },
                    "HeadObject",
                )
            )
        ),
    ):
        missing, checked = check_s3_existence(
            ["s3://bucket/a", "s3://bucket/b"],
            mode="sample",
            sample_size=1,
        )
    assert checked == 1
    assert len(missing) == 1


# -- readable units, recommendation, and messages -------------------------------


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (timedelta(days=6, hours=4, minutes=3), "6d4h"),
        (timedelta(days=15), "15d"),
        (timedelta(days=8, hours=20), "8d20h"),
        (timedelta(minutes=40), "40min"),
        (timedelta(hours=-12), "-12h"),
        (timedelta(hours=1, minutes=4), "1h4min"),
        (timedelta(days=2, minutes=5), "2d"),
        (timedelta(seconds=42), "42s"),
        (timedelta(), "0s"),
    ],
)
def test_format_duration_is_human_readable(value: timedelta, text: str) -> None:
    assert format_duration(value) == text


def _policy(
    expire: timedelta,
    cleanup: timedelta = timedelta(),
    *,
    source: RetentionSource = RetentionSource.CONFIG,
    confidence: Literal["declared", "observed", "unknown"] = "declared",
    evidence: dict[str, object] | None = None,
) -> RetentionPolicy:
    return RetentionPolicy(expire, cleanup, source, confidence, evidence or {})


def test_recommended_window_clears_warning_threshold() -> None:
    """The applied recommendation leaves the margin above ``k * I``."""
    now = datetime(2026, 11, 5, tzinfo=UTC)
    sync = SyncProfile(timedelta(hours=1), timedelta(minutes=4))
    published = now - timedelta(days=6, hours=21, minutes=56)
    risk = compute_risk_window(
        _policy(timedelta(days=7)), sync, published, now=now, safety_factor=2
    )
    assert risk.severity is Severity.WARNING  # margem 1h < 2h
    applied = compute_risk_window(
        _policy(risk.recommended_expire_window),
        sync,
        published,
        now=now,
        safety_factor=2,
    )
    assert applied.severity is Severity.OK
    assert risk.recommended_expire_window >= timedelta(days=6, hours=21)


def test_margin_messages_follow_section_6() -> None:
    now = datetime(2026, 11, 5, tzinfo=UTC)
    sync = SyncProfile(timedelta(hours=1), timedelta(minutes=4))
    ok = _policy(timedelta(days=14), timedelta(days=1))
    risk = compute_risk_window(ok, sync, now - timedelta(days=8), now=now)
    message = _margin_message(ok, sync, risk, 2.0)
    assert message.severity is Severity.OK
    assert message.message == ("retention ok: margin 6d22h (R+C=15d, A+I+D=8d1h)")

    tight = _policy(timedelta(days=2), timedelta(hours=1))
    risk = compute_risk_window(
        tight, sync, now - timedelta(days=1, hours=23, minutes=52), now=now
    )
    message = _margin_message(tight, sync, risk, 2.0)
    assert message.severity is Severity.WARNING
    assert message.message == "margin 4min < 2h (2× sync interval)"  # noqa: RUF001

    short = _policy(timedelta(days=7))
    risk = compute_risk_window(
        short, sync, now - timedelta(days=7, hours=10, minutes=56), now=now
    )
    message = _margin_message(short, sync, risk, 2.0)
    assert message.severity is Severity.CRITICAL
    assert message.message == "insufficient retention: margin -12h"
    assert message.hint is not None
    assert "raise older_than to >=" in message.hint

    unknown = _policy(timedelta(), confidence="unknown")
    risk = compute_risk_window(unknown, sync, now, now=now)
    assert _margin_message(unknown, sync, risk, 2.0).message == (
        "unknown retention policy"
    )


def test_recommendation_sentence_has_context() -> None:
    now = datetime(2026, 11, 5, tzinfo=UTC)
    sync = SyncProfile(timedelta(hours=1), timedelta(minutes=4))
    policy = _policy(timedelta(days=7))
    risk = compute_risk_window(policy, sync, now - timedelta(days=6, hours=8), now=now)
    text = _recommendation(policy, sync, risk, None)
    assert text.startswith("expire_snapshots older_than >= 6d")
    assert "(current 7d; sync every 1h, p95 4min, published snapshot aged 6d8h)" in text
    assert "cleanup_old_files" not in text
    text = _recommendation(policy, sync, risk, timedelta(days=2))
    assert text.endswith("; cleanup_old_files older_than >= 2d (current 0s)")


# -- inconsistency between declared and observed -------------------------------


@pytest.mark.parametrize(
    ("declared", "observed", "warns"),
    [
        (timedelta(days=7), timedelta(days=3), True),  # real window is shorter
        (timedelta(days=3), timedelta(days=7), True),  # expiration does not run
        (timedelta(days=7), timedelta(days=7, hours=5), False),  # daily batch
    ],
)
def test_declared_vs_observed_inconsistency_both_directions(
    declared: timedelta, observed: timedelta, warns: bool
) -> None:
    policy = _policy(
        declared,
        evidence={
            "expiration_observed": True,
            "observed_retention_seconds": int(observed.total_seconds()),
        },
    )
    messages = policy_inconsistencies(policy, SyncProfile(timedelta(hours=1)))
    assert bool(messages) is warns
    if warns:
        assert messages[0].severity is Severity.WARNING
        assert "differs from the one observed" in messages[0].message


def test_inconsistency_ignored_without_observed_expiration() -> None:
    policy = _policy(
        timedelta(days=7),
        evidence={"expiration_observed": False, "observed_retention_seconds": 60},
    )
    assert policy_inconsistencies(policy, SyncProfile(timedelta(hours=1))) == []


def test_config_vs_catalog_option_inconsistency() -> None:
    policy = _policy(
        timedelta(days=7),
        timedelta(days=1),
        evidence={
            "catalog_expire_window_seconds": 2 * 86400,
            "catalog_cleanup_window_seconds": 86400,
        },
    )
    messages = policy_inconsistencies(policy, SyncProfile(timedelta(hours=1)))
    assert [m.message for m in messages] == [
        "retention.expire_window 7d differs from the catalog option 2d"
    ]


# -- delete_older_than versus Iceberg snapshot retention --------------------------


def _files(history_age: timedelta | None, now: datetime) -> IcebergFiles:
    oldest = (
        int((now - history_age).timestamp() * 1000) if history_age is not None else None
    )
    return IcebergFiles(
        current={"s3://b/cur.parquet"},
        retained={"s3://b/cur.parquet", "s3://b/old.parquet"},
        retained_snapshots=2,
        oldest_history_ms=oldest,
        expired_evidence=False,
    )


def test_cleanup_shorter_than_iceberg_retention_warns() -> None:
    now = datetime(2026, 11, 5, tzinfo=UTC)
    sync = SyncProfile(timedelta(hours=1), timedelta(minutes=10))
    policy = _policy(timedelta(days=7), timedelta(days=1))
    evidence, message, recommended = iceberg_retention_check(
        policy, sync, _files(None, now), iceberg_retention_hours=48, now=now
    )
    assert message is not None
    assert message.severity is Severity.WARNING
    assert message.message == (
        "cleanup_window 1d < Iceberg snapshot retention 2d + sync cycle 1h10min"
    )
    assert recommended == timedelta(days=2, hours=2)
    assert evidence["snapshot_retention_seconds"] == 48 * 3600
    assert evidence["history_only_files"] == 1

    _, message, recommended = iceberg_retention_check(
        policy, sync, _files(None, now), iceberg_retention_hours=12, now=now
    )
    assert message is None
    assert recommended is None


def test_iceberg_retaining_everything_warns_when_cleanup_is_finite() -> None:
    now = datetime(2026, 11, 5, tzinfo=UTC)
    sync = SyncProfile(timedelta(hours=1))
    _, message, _ = iceberg_retention_check(
        _policy(timedelta(days=7), timedelta(days=1)),
        sync,
        _files(None, now),
        iceberg_retention_hours=None,
        now=now,
    )
    assert message is not None
    assert "Iceberg keeps the whole history" in message.message
    # unknown C: nothing to compare
    _, message, _ = iceberg_retention_check(
        _policy(timedelta(days=7)),
        sync,
        _files(None, now),
        iceberg_retention_hours=None,
        now=now,
    )
    assert message is None


def test_iceberg_retention_observed_when_not_informed() -> None:
    now = datetime(2026, 11, 5, tzinfo=UTC)
    sync = SyncProfile(timedelta(hours=1))
    policy = _policy(timedelta(days=7), timedelta(days=1))
    evidence, message, _ = iceberg_retention_check(
        policy, sync, _files(timedelta(days=3), now), now=now
    )
    assert evidence["snapshot_retention_source"] == "observed"
    assert message is not None
    assert message.message == "cleanup_window 1d < retained Iceberg history 3d"
    _, message, _ = iceberg_retention_check(
        policy, sync, _files(timedelta(hours=5), now), now=now
    )
    assert message is None


# -- S3 via moto ------------------------------------------------------------------


@pytest.fixture
def bucket() -> Iterator[Any]:
    with mock_aws():
        client = aws.client("s3", "us-east-1", None)
        client.create_bucket(Bucket="lake")
        for index in range(20):
            client.put_object(Bucket="lake", Key=f"data/f{index:02}.parquet", Body=b"x")
        yield client


def _heads(client: Any) -> list[str]:
    keys: list[str] = []

    def record(params: dict[str, Any], **_: Any) -> None:
        keys.append(params["Key"])

    client.meta.events.register("provide-client-params.s3.HeadObject", record)
    return keys


def test_check_s3_full_reports_missing_object(bucket: Any) -> None:
    """A file deleted from the bucket -> 1 item in s3_missing."""
    bucket.delete_object(Bucket="lake", Key="data/f07.parquet")
    paths = [f"s3://lake/data/f{index:02}.parquet" for index in range(20)]
    missing, checked = check_s3_existence(paths, mode="full")
    assert missing == ["s3://lake/data/f07.parquet"]
    assert checked == 20


def test_check_s3_sample_is_deterministic(bucket: Any) -> None:
    """Same sample across consecutive runs, regardless of order."""
    paths = [f"s3://lake/data/f{index:02}.parquet" for index in range(20)]
    keys = _heads(bucket)
    _, first_count = check_s3_existence(paths, mode="sample", sample_size=5)
    first = sorted(keys)
    keys.clear()
    _, second_count = check_s3_existence(
        list(reversed(paths)), mode="sample", sample_size=5
    )
    assert first_count == second_count == 5
    assert sorted(keys) == first
    expected = sorted(
        paths,
        key=lambda p: hashlib.md5(p.encode()).hexdigest(),  # noqa: S324
    )[:5]
    assert first == sorted(p.removeprefix("s3://lake/") for p in expected)


def test_check_s3_access_denied_is_error_not_missing(bucket: Any) -> None:
    """A 403 on head_object is a permission error (exit 1), not an absence."""
    _deny_head_object(bucket)
    with pytest.raises(AwsError, match="AccessDenied while checking") as info:
        check_s3_existence(["s3://lake/data/f00.parquet"], mode="full")
    assert info.value.exit_code is ExitCode.ERROR


def _deny_head_object(client: Any) -> None:
    def deny(**_: Any) -> tuple[Any, dict[str, Any]]:
        response = {
            "Error": {"Code": "403", "Message": "Forbidden"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        }
        return SimpleNamespace(status_code=403, headers={}), response

    client.meta.events.register("before-call.s3.HeadObject", deny)


# -- result without a snapshot, and rendering --------------------------------------


def test_render_handles_no_snapshot_result(capsys: pytest.CaptureFixture[str]) -> None:
    """``policy=None`` must not break the terminal output."""
    result = no_snapshot_result("main.t")
    assert result["exit_code"] == int(ExitCode.NOTHING_TO_DO)
    render_retention([result])
    out = capsys.readouterr().out
    assert "nothing to check: table has no published snapshot" in out
    assert "ducklake-iceberg sync" in out


def test_render_prints_margin_in_readable_units(
    capsys: pytest.CaptureFixture[str],
) -> None:
    now = datetime(2026, 11, 5, tzinfo=UTC)
    sync = SyncProfile(timedelta(hours=1), timedelta(minutes=4))
    policy = _policy(timedelta(days=14), timedelta(days=1))
    risk = compute_risk_window(policy, sync, now - timedelta(days=8), now=now)
    report = RetentionReport(
        policy=policy,
        sync=sync,
        risk=risk,
        published_snapshot_present=True,
        doomed_snapshots=[],
        referenced_files=3,
        doomed_files=[
            DoomedFile(f"s3://b/{i}.parquet", "scheduled") for i in range(60)
        ],
        s3_checked=0,
        s3_missing=[],
        recommendation=_recommendation(policy, sync, risk, None),
        table="main.t",
        messages=[_margin_message(policy, sync, risk, 2.0)],
    )
    render_retention([report.as_dict()])
    out = capsys.readouterr().out
    assert "6d22h" in out
    assert "retention ok: margin 6d22h" in out
    assert "recommendation: expire_snapshots older_than >=" in out
    assert "10 more files" in out
    assert " s\n" not in out


def test_warning_escalates_only_with_strict() -> None:
    now = datetime(2026, 11, 5, tzinfo=UTC)
    sync = SyncProfile(timedelta(hours=1))
    policy = _policy(timedelta(days=30), timedelta(days=7))
    risk = compute_risk_window(policy, sync, now, now=now)
    report = RetentionReport(
        policy=policy,
        sync=sync,
        risk=risk,
        published_snapshot_present=True,
        doomed_snapshots=[],
        referenced_files=1,
        doomed_files=[],
        s3_checked=0,
        s3_missing=[],
        recommendation="",
        messages=[RetentionMessage(Severity.WARNING, "inconsistente")],
    )
    assert risk.severity is Severity.OK
    assert report.exit_code is ExitCode.SUCCESS
    assert report.as_dict(strict=True)["exit_code"] == int(ExitCode.DIVERGED)
    assert report.as_dict()["warning"] == "inconsistente"
