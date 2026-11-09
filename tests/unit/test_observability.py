"""Structured logging, per-phase metrics, redaction, and the envelope contract.

Covered here (the end-to-end checks with a real catalog live in
``tests/integration/test_envelope_contract.py``): one JSON line per event,
botocore never at DEBUG, sum of phases <= total, lint of the event catalog,
v1 schema compatibility, ``--json-version 1``, closed stderr, and the
fail-closed redaction policy.
"""

from __future__ import annotations

import ast
import json
import logging
import sys
import threading
import time
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from typer.testing import CliRunner

from iceduck import cli, timing
from iceduck.concurrency import ContextThreadPoolExecutor, map_parallel
from iceduck.errors import AwsError, ExitCode, NothingToDo
from iceduck.obs import redaction
from iceduck.obs.logging import (
    EVENTS,
    bind,
    configure_logging,
    log_event,
    restore_library_levels,
)
from iceduck.obs.metrics import (
    PhaseMetric,
    RunMetrics,
    add_work,
    collect,
    current,
)
from iceduck.output import ENVELOPE_VERSION, Envelope, Problem

ROOT = Path(__file__).parents[2]
SCHEMA_PATH = ROOT / "src/iceduck/schemas/envelope-v1.json"
runner = CliRunner()


def _schema() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return loaded


def _lines(stream: StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def _json_logging(level: str = "INFO", command: str = "sync") -> StringIO:
    stream = StringIO()
    configure_logging(
        level, fmt="json", stream=stream, run_id="a" * 32, command=command
    )
    stream.seek(0)
    stream.truncate()
    return stream


# -- structured logging -----------------------------------------------------


def test_json_logging_emits_single_redacted_line() -> None:
    stream = _json_logging()
    log_event(
        "sync.end",
        table="main.orders",
        snapshot_id=3,
        message="host=db.internal",
        data_file_encryption_key="not-for-output",
    )

    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["run_id"] == "a" * 32
    assert event["command"] == "sync"
    assert event["table"] == "main.orders"
    assert event["snapshot_id"] == 3
    assert event["log_schema_version"] == 1
    assert event["level"] == "INFO"
    assert "db.internal" not in lines[0]
    assert "not-for-output" not in lines[0]
    assert "data_file_encryption_key" not in lines[0]


def test_free_text_lines_become_log_events_with_scrubbed_message() -> None:
    """Free-form ``LOG.*`` is also single-line JSON, with the text in
    ``message``; a non-``str`` argument (an exception with the DSN) is redacted."""
    stream = _json_logging()
    error = RuntimeError("failed: host=db.internal password=hunter2\nline 2")
    logging.getLogger("iceduck.export").warning("export failed: %s", error)

    (event,) = _lines(stream)
    assert event["event"] == "log"
    assert event["logger"] == "iceduck.export"
    assert event["run_id"] == "a" * 32
    assert "host=***" in event["message"]
    assert "hunter2" not in stream.getvalue()
    assert "db.internal" not in stream.getvalue()


def test_third_party_records_go_through_the_same_handler() -> None:
    """The ``SecretFilter`` also runs for third-party libraries."""
    stream = _json_logging()
    logging.getLogger("botocore.credentials").warning("key AKIAABCDEFGHIJKLMNOP seen")

    (event,) = _lines(stream)
    assert event["logger"] == "botocore.credentials"
    assert "AKIAABCDEFGHIJKLMNOP" not in stream.getvalue()
    restore_library_levels()
    assert logging.getLogger("botocore").propagate is True


def test_traceback_only_in_debug_as_single_line() -> None:
    def emit() -> str:
        try:
            raise ValueError("password=hunter2\nsecond line")
        except ValueError as exc:
            logging.getLogger("iceduck").error("x", exc_info=exc)
        return stream.getvalue()

    stream = _json_logging("INFO")
    assert "exc" not in json.loads(emit())

    stream = _json_logging("DEBUG")
    output = emit()
    assert len(output.splitlines()) == 1
    event = json.loads(output)
    assert "ValueError" in event["exc"]
    assert "hunter2" not in output


def test_debug_logging_does_not_enable_botocore_debug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--log-level DEBUG`` raises botocore to INFO at most."""
    botocore = logging.getLogger("botocore")
    previous_level = botocore.level
    monkeypatch.setattr(botocore, "level", previous_level)
    configure_logging("DEBUG", fmt="text", stream=StringIO(), command="sync")
    assert botocore.level == logging.INFO
    assert not botocore.isEnabledFor(logging.DEBUG)
    restore_library_levels()
    assert botocore.level == previous_level


def test_closed_stderr_does_not_break_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    """``2>&-`` leaves ``sys.stderr`` as ``None``; nothing raises."""
    monkeypatch.setattr(sys, "stderr", None)
    configure_logging("INFO", fmt="auto", command="doctor")
    log_event("command.end", exit_code=0)

    closed = StringIO()
    closed.close()
    configure_logging("INFO", fmt="json", stream=closed, command="doctor")
    log_event("command.end", exit_code=0)  # the write fails silently


# -- context in threads ------------------------------------------------------


def test_map_parallel_workers_inherit_log_context() -> None:
    """Without ``copy_context``, worker events were emitted without ``run_id``."""
    stream = _json_logging()

    def work(table: str) -> int:
        log_event("table.start", table=table)
        return 0

    with bind(phase_hint="x"):
        map_parallel(work, ["a.t1", "a.t2", "a.t3"], workers=3)

    events = _lines(stream)
    assert len(events) == 3
    assert {event["run_id"] for event in events} == {"a" * 32}
    assert {event["command"] for event in events} == {"sync"}
    assert {event["phase_hint"] for event in events} == {"x"}


def test_inner_pools_propagate_metrics_and_table_binding() -> None:
    stream = _json_logging()
    metrics = RunMetrics(run_id="a" * 32, command="export", table="main.t")
    seen: list[RunMetrics | None] = []
    lock = threading.Lock()

    def work(_: int) -> None:
        with lock:
            seen.append(current())
        add_work("deletes", items=1, byte_count=10)
        logging.getLogger("iceduck.deletes").info("file written")

    with (
        bind(table="main.t"),
        collect(metrics),
        timing.phase("export.deletes"),
        ContextThreadPoolExecutor(max_workers=4) as pool,
    ):
        list(pool.map(work, range(8)))

    assert seen == [metrics] * 8
    (phase,) = metrics.phases
    assert (phase.phase, phase.items, phase.bytes) == ("deletes", 8, 80)
    worker_lines = [e for e in _lines(stream) if e["event"] == "log"]
    assert len(worker_lines) == 8
    assert all(e["table"] == "main.t" and e["run_id"] for e in worker_lines)


# -- per-phase metrics ------------------------------------------------------


def test_run_metrics_serializes_phase_and_totals() -> None:
    metrics = RunMetrics(run_id="a" * 32, command="sync", table="main.orders")
    metrics.phases.append(
        PhaseMetric(
            phase="glue",
            started_at=datetime(2026, 11, 5, tzinfo=UTC),
            duration_ms=25,
            calls=2,
            retries=1,
        )
    )
    time.sleep(0.005)

    payload = metrics.to_json()
    assert payload["duration_ms"] > 0
    assert payload["totals"] == {"api_calls": 2, "retries": 1}
    assert payload["phases"][0]["phase"] == "glue"
    assert payload["phases"][0]["started_at"].endswith("+00:00")


def test_footers_and_glue_stats_record_work_counts() -> None:
    metrics = RunMetrics(run_id="a" * 32, command="sync")

    with collect(metrics):
        with timing.phase("footers"):
            add_work("footers", items=3, byte_count=120)
        with timing.phase("glue_stats"):
            add_work("glue_stats", items=2)
            metrics.add_call("glue_stats")

    phases = {metric.phase: metric for metric in metrics.phases}
    assert (phases["footers"].items, phases["footers"].bytes) == (3, 120)
    assert (phases["glue_stats"].items, phases["glue_stats"].calls) == (2, 1)


def test_nested_phases_are_exclusive_and_sum_below_total() -> None:
    """``stats`` contains ``footers``; the sum does not exceed the total."""
    metrics = RunMetrics(run_id="a" * 32, command="export")
    with collect(metrics):
        with timing.phase("export.stats"):
            time.sleep(0.02)
            with timing.phase("footers"):
                time.sleep(0.04)
        # ``export.write`` is not a phase: only its children count
        with timing.phase("export.write"), timing.phase("manifest"):
            time.sleep(0.01)

    phases = {m.phase: m for m in metrics.phases}
    assert set(phases) == {"stats", "footers", "manifest"}
    assert phases["footers"].duration_ms >= 40
    assert 15 <= phases["stats"].duration_ms < 40
    total = sum(m.duration_ms for m in metrics.phases)
    assert total <= metrics.duration_ms


def test_repeated_phase_is_merged_and_late_counters_apply() -> None:
    metrics = RunMetrics(run_id="a" * 32, command="sync")
    with collect(metrics):
        with timing.phase("doctor"):
            add_work("doctor", items=8)
        with timing.phase("doctor"):
            add_work("doctor", items=6)
        add_work("doctor", items=1)  # after the phase: still included in to_json

    payload = metrics.to_json()
    (doctor,) = payload["phases"]
    assert doctor["items"] == 15


def test_failed_phase_records_error_and_nothing_to_do_does_not() -> None:
    """A failed phase ends with ``ok=false`` and an ``error_code``."""
    stream = _json_logging()
    metrics = RunMetrics(run_id="a" * 32, command="sync")
    with collect(metrics):
        with pytest.raises(AwsError), timing.phase("register"):
            raise AwsError("glue down")
        with pytest.raises(NothingToDo), timing.phase("checkpoint"):
            raise NothingToDo("nothing")

    phases = {m.phase: m for m in metrics.phases}
    assert (phases["glue"].ok, phases["glue"].error_code) == (False, "AwsError")
    assert (phases["checkpoint"].ok, phases["checkpoint"].error_code) == (True, None)
    ends = [e for e in _lines(stream) if e["event"] == "phase.end"]
    assert [(e["phase"], e["ok"]) for e in ends] == [
        ("glue", False),
        ("checkpoint", True),
    ]


@pytest.mark.aws
def test_aws_calls_are_attributed_to_the_open_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moto import mock_aws

    from iceduck import aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    metrics = RunMetrics(run_id="a" * 32, command="register")
    with mock_aws(), collect(metrics):
        glue = aws.client("glue", "us-east-1", None)
        with timing.phase("register"):
            glue.get_databases()
            glue.get_databases()
        glue.get_databases()  # outside a phase: only in the total

    (phase,) = metrics.phases
    assert (phase.phase, phase.calls) == ("glue", 2)
    assert metrics.totals()["api_calls"] == 3


# -- event catalog ----------------------------------------------------------


def _logged_events() -> list[tuple[str, int, str | None]]:
    found: list[tuple[str, int, str | None]] = []
    for path in sorted((ROOT / "src/iceduck").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "log_event"
            ):
                first = node.args[0] if node.args else None
                name = (
                    first.value
                    if isinstance(first, ast.Constant) and isinstance(first.value, str)
                    else None
                )
                found.append((str(path.relative_to(ROOT)), node.lineno, name))
    return found


def test_every_logged_event_is_in_the_closed_catalog() -> None:
    calls = _logged_events()
    assert len(calls) > 10
    unknown = [call for call in calls if call[2] not in EVENTS]
    assert not unknown, f"events missing from the closed event catalog: {unknown}"


def test_unknown_event_never_raises_and_is_flagged() -> None:
    """An observability problem never changes the exit code."""
    stream = _json_logging()
    log_event("glue.stats", columns=3)

    (event,) = _lines(stream)
    assert event["event"] == "log"
    assert event["unregistered_event"] == "glue.stats"


# -- fail-closed redaction --------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "secret"),
    [
        ("dbname=lake host=db.internal port=5432", "db.internal"),
        ("password=hunter2 user=x", "hunter2"),
        ("pwd = hunter2", "hunter2"),
        ("aws_secret_access_key=abc123secret", "abc123secret"),
        ("token=tok-123", "tok-123"),
        ("postgresql://user:pw-123@db/lake", "pw-123"),
        ("key AKIAABCDEFGHIJKLMNOP", "AKIAABCDEFGHIJKLMNOP"),
        ("key ASIAABCDEFGHIJKLMNOP", "ASIAABCDEFGHIJKLMNOP"),
        ("https://b.s3.amazonaws.com/k?X-Amz-Signature=deadbeef&a=1", "deadbeef"),
        ("?X-Amz-Security-Token=FwoGZXIvYXdz&b=2", "FwoGZXIvYXdz"),
    ],
)
def test_scrub_covers_every_secret_pattern(raw: str, secret: str) -> None:
    assert secret not in redaction.scrub(raw)


def test_scrub_mapping_drops_forbidden_and_masks_host() -> None:
    data = {
        "catalog_dsn": "ducklake:postgres:dbname=lake host=db.internal",
        "host": "db.internal",
        "files": [{"path": "s3://b/f.parquet", "data_file_encryption_key": "k"}],
        "key_metadata": b"k",
        "Authorization": "AWS4-HMAC-SHA256 Credential=x",
    }
    out = redaction.scrub_mapping(data)
    assert out == {
        "catalog_dsn": "ducklake:postgres:dbname=lake host=***",
        "host": "***",
        "files": [{"path": "s3://b/f.parquet"}],
    }


def test_secret_filter_failure_drops_the_line(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = _json_logging()

    def boom(_: Any) -> Any:
        raise RuntimeError("redaction broke")

    monkeypatch.setattr(redaction, "scrub_mapping", boom)
    log_event("command.end", exit_code=0)

    assert stream.getvalue() == ""
    assert redaction.redaction_failures() == 1


def test_redaction_failure_becomes_problem_without_changing_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = runner.invoke(cli.app, ["doctor", "--json"])
    original = redaction.scrub_mapping
    calls = {"n": 0}

    def flaky(data: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:  # only the first log line fails
            raise RuntimeError("redaction broke")
        return original(data)

    monkeypatch.setattr(redaction, "scrub_mapping", flaky)
    result = runner.invoke(cli.app, ["doctor", "--json"])

    assert result.exit_code == base.exit_code == int(ExitCode.ERROR)
    codes = [p["code"] for p in json.loads(result.stdout)["problems"]]
    assert "redaction_failed" in codes
    assert "redaction_failed" not in [
        p["code"] for p in json.loads(base.stdout)["problems"]
    ]


# -- internal error ---------------------------------------------------------


def test_unhandled_exception_is_internal_error_with_scrubbed_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def explode(**_: Any) -> Any:
        raise RuntimeError("connection failed: host=db.internal password=hunter2")

    monkeypatch.setattr(cli, "load_settings", explode)
    result = runner.invoke(
        cli.app, ["sync", "--table", "main.t", "--json", "--log-format", "json"]
    )

    assert result.exit_code == int(ExitCode.ERROR)
    assert not isinstance(result.exception, RuntimeError)
    payload = json.loads(result.stdout)
    Draft202012Validator(_schema()).validate(payload)
    (problem,) = payload["problems"]
    assert problem["code"] == "internal_error"
    assert problem["message"].startswith("internal error: ")
    assert "host=***" in problem["message"]
    output = result.stdout + result.stderr
    assert "hunter2" not in output
    assert "db.internal" not in output
    assert "Traceback" not in output
    # the failure happened before the config: the log is still JSON, up to command.end
    events = [json.loads(line) for line in result.stderr.splitlines()]
    assert events[-1]["event"] == "command.end"
    assert events[-1]["exit_code"] == 1


# -- envelope version -------------------------------------------------------


def _comparable(stdout: str) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(stdout)
    for volatile in ("started_at", "duration_ms"):
        payload.pop(volatile)
    return payload


def test_explicit_json_version_matches_default() -> None:
    default = runner.invoke(cli.app, ["doctor", "--json"])
    explicit = runner.invoke(cli.app, ["doctor", "--json", "--json-version", "1"])
    assert explicit.exit_code == default.exit_code
    assert _comparable(explicit.stdout) == _comparable(default.stdout)


def test_unsupported_json_version_is_usage_error() -> None:
    result = runner.invoke(cli.app, ["doctor", "--json", "--json-version", "9"])
    assert result.exit_code == 2
    text = " ".join(result.output.replace("│", " ").split())
    assert "unsupported envelope version 9 (current 1)" in text


def test_deprecated_json_version_warns_without_changing_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "DEPRECATED_ENVELOPE_VERSIONS", frozenset({1}))
    result = runner.invoke(cli.app, ["doctor", "--json", "--json-version", "1"])
    assert result.exit_code == int(ExitCode.ERROR)  # same as without the flag
    problems = json.loads(result.stdout)["problems"]
    warning = next(p for p in problems if p["code"] == "envelope_version_deprecated")
    assert warning["level"] == "warning"


#: Minimum contract of envelope v1: required keys per ``$def`` and the type of
#: each. Removing, renaming, or changing the type of any entry requires
#: ``ENVELOPE_VERSION = 2`` and an ``envelope-v2.json``. Adding an optional key
#: does not change this table.
V1_CONTRACT: dict[str, dict[str, Any]] = {
    "": {
        "version": {"const": 1},
        "command": None,
        "status": None,
        "exit_code": {"type": "integer", "minimum": 0},
        "dry_run": {"type": "boolean"},
        "started_at": {"type": "string", "format": "date-time"},
        "duration_ms": {"type": "integer", "minimum": 0},
        "config": {"type": ["object", "null"]},
        "results": None,
        "problems": None,
    },
    "problem": {
        "code": {"type": "string"},
        "message": {"type": "string"},
        "hint": {"type": ["string", "null"]},
        "table": {"type": ["string", "null"]},
    },
    "metrics": {"run_id": None, "duration_ms": None, "phases": None, "totals": None},
    "doctor_result": dict.fromkeys(
        ("table", "identity", "snapshot", "environment", "aws", "inlined_rows")
    ),
    "checkpoint_table": dict.fromkeys(
        ("table", "inlined_rows_before", "inlined_rows_after")
    ),
    "export_publication": dict.fromkeys(
        (
            "table",
            "ducklake_snapshot_id",
            "metadata_location",
            "files_added",
            "bytes_referenced",
        )
    ),
    "register_result": dict.fromkeys(
        ("table", "metadata_location", "glue_table", "created")
    ),
    "sync_table": dict.fromkeys(
        (
            "table",
            "ducklake_snapshot_id",
            "iceberg_snapshot_id",
            "metadata_location",
            "files_added",
            "files_removed",
            "bytes_referenced",
        )
    ),
    "status_table": dict.fromkeys(
        (
            "table",
            "ducklake_snapshot_id",
            "iceberg_snapshot_id",
            "in_sync",
            "external_write_suspected",
        )
    ),
    "validate_data": dict.fromkeys(("table", "checks", "tolerances_applied")),
    "validate_retention": dict.fromkeys(("table", "policy", "risk", "doomed_files")),
}


def test_schema_v1_keeps_the_frozen_contract() -> None:
    """A key removed from the schema without a ``version`` bump fails here."""
    assert ENVELOPE_VERSION == 1, "new version: freeze the v2 contract too"
    schema = _schema()
    for name, keys in V1_CONTRACT.items():
        node = schema if name == "" else schema["$defs"][name]
        missing = set(keys) - set(node["required"])
        assert not missing, f"{name or 'envelope'}: removed keys {missing}"
        for key, declared in keys.items():
            if declared is not None:
                assert node["properties"][key] == declared, f"{name}.{key} changed"
    Draft202012Validator.check_schema(schema)


def test_envelope_redacts_fields_and_validates_schema() -> None:
    envelope = Envelope(command="sync", dry_run=False)
    envelope.config = {"host": "db.local"}
    envelope.results.append(
        {
            "table": "main.orders",
            "exit_code": 1,
            "data_file_encryption_key": "do-not-export",
            "error": "host=db.local",
        }
    )
    envelope.problems.append(Problem(code="db_error", message="host=db.local"))
    envelope.set_exit(ExitCode.ERROR)
    payload = json.loads(envelope.to_json())
    Draft202012Validator(_schema()).validate(payload)
    serialized = json.dumps(payload)
    assert "do-not-export" not in serialized
    assert "db.local" not in serialized
    assert "data_file_encryption_key" not in serialized
