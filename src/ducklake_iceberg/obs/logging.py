"""Structured logging for ``ducklake-iceberg``.

A single ``StreamHandler`` on stderr (stdout belongs to the envelope), with
``SecretFilter`` as the last gate. The context (``run_id``, ``command``,
``table``...) comes from ``contextvars``: the package's thread pools
(``concurrency.ContextThreadPoolExecutor``) copy the submitter's context so
that lines emitted in workers carry the same fields.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from typing import Any, Literal, TextIO

from ducklake_iceberg.obs.redaction import SecretFilter, scrub, scrub_mapping

LOG_SCHEMA_VERSION = 1
LogFormat = Literal["auto", "json", "text"]
_CONTEXT: ContextVar[dict[str, Any] | None] = ContextVar(
    "ducklake_iceberg_log_ctx", default=None
)
#: third-party libraries with a capped level and the same handler
_LIBRARIES: tuple[str, ...] = ("botocore", "urllib3", "pyiceberg")
#: (level, propagate) of each library before the first ``configure_logging``
_LIBRARY_STATE: dict[str, tuple[int, bool]] = {}
_HANDLER: logging.Handler | None = None

#: Event of free-text lines (``LOG.info(...)`` from internal modules and
#: records from third-party libraries): the message goes in ``message``.
#: Not a contract for alerting; alerting uses the other events.
FREE_TEXT_EVENT = "log"

#: Closed catalog: a new event must be added here and covered by a test.
EVENTS = frozenset(
    {
        "command.start",
        "command.end",
        "phase.start",
        "phase.end",
        "table.start",
        "table.end",
        "sync.end",
        "glue.register",
        "glue.conflict",
        "compat.blocked",
        "problem",
        FREE_TEXT_EVENT,
    }
)


def _payload(record: logging.LogRecord) -> dict[str, Any]:
    fields = scrub_mapping(getattr(record, "event_fields", {}))
    context = scrub_mapping(_CONTEXT.get() or {})
    event = getattr(record, "event", None)
    payload: dict[str, Any] = {**context, **fields}
    if event is None:
        # free text: the formatted message goes through ``scrub`` again because
        # non-``str`` arguments (e.g. an exception) only become text here
        event = FREE_TEXT_EVENT
        payload["message"] = scrub(record.getMessage())
        payload["logger"] = record.name
    payload.update(
        {
            "ts": datetime.now().astimezone().isoformat(),
            "level": record.levelname,
            "event": event,
            "log_schema_version": LOG_SCHEMA_VERSION,
        }
    )
    # traceback only at DEBUG, as a single, already redacted string
    if record.exc_text and logging.getLogger("ducklake_iceberg").isEnabledFor(
        logging.DEBUG
    ):
        payload["exc"] = scrub(record.exc_text)
    return payload


class JsonFormatter(logging.Formatter):
    """Serialize a record as a single, already redacted JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            _payload(record), ensure_ascii=False, separators=(",", ":"), default=str
        )


class _TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "event", None)
        message = event if event is not None else record.getMessage()
        return scrub(
            super().format(
                logging.makeLogRecord({**record.__dict__, "msg": message, "args": ()})
            )
        )


def _is_tty(stream: TextIO) -> bool:
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError, OSError):
        return False


def configure_logging(
    level: str,
    *,
    fmt: LogFormat = "auto",
    stream: TextIO | None = None,
    run_id: str | None = None,
    command: str = "unknown",
) -> str:
    """Install the single stderr handler and the process context.

    ``auto`` = ``json`` when stderr is not a TTY. With stderr closed
    (``2>&-``: ``sys.stderr is None``) the log is dropped and the command goes
    on with the same exit code. Returns the process ``run_id``.
    """
    global _HANDLER
    target = stream if stream is not None else sys.stderr
    logger = logging.getLogger("ducklake_iceberg")
    _remove_handler()
    handler: logging.Handler
    if target is None:
        handler = logging.NullHandler()
    else:
        if fmt == "auto":
            fmt = "text" if _is_tty(target) else "json"
        handler = logging.StreamHandler(target)
        handler.setFormatter(
            JsonFormatter()
            if fmt == "json"
            else _TextFormatter("%(levelname)s %(name)s: %(message)s")
        )
    handler.addFilter(SecretFilter())
    _HANDLER = handler
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    # stderr closed or disk full: silent failure, exit code unchanged
    logging.raiseExceptions = False
    resolved_run_id = run_id or uuid.uuid4().hex
    _CONTEXT.set({"run_id": resolved_run_id, "command": command})
    for name in _LIBRARIES:
        library_logger = logging.getLogger(name)
        _LIBRARY_STATE.setdefault(
            name, (library_logger.level, library_logger.propagate)
        )
        # never DEBUG: botocore DEBUG prints signed headers
        library_logger.setLevel(
            logging.INFO if level.upper() == "DEBUG" else logging.WARNING
        )
        library_logger.addHandler(handler)
        library_logger.propagate = False
    log_event("command.start", level=logging.INFO)
    return resolved_run_id


def _remove_handler() -> None:
    global _HANDLER
    if _HANDLER is None:
        return
    for name in ("ducklake_iceberg", *_LIBRARIES):
        logging.getLogger(name).removeHandler(_HANDLER)
    _HANDLER.close()
    _HANDLER = None


def is_configured() -> bool:
    """``True`` if ``configure_logging`` already installed the handler."""
    return _HANDLER is not None


def restore_library_levels() -> None:
    """Restore the libraries' level and ``propagate``; detach the handler."""
    for name, (level, propagate) in _LIBRARY_STATE.items():
        library_logger = logging.getLogger(name)
        library_logger.setLevel(level)
        library_logger.propagate = propagate
        if _HANDLER is not None:
            library_logger.removeHandler(_HANDLER)
    _LIBRARY_STATE.clear()


def reset_logging() -> None:
    """Fully undo ``configure_logging`` (tests)."""
    restore_library_levels()
    _remove_handler()
    _CONTEXT.set(None)


def current_run_id() -> str:
    """Process ``run_id`` (generates one if logging is not configured yet)."""
    context = _CONTEXT.get() or {}
    run_id = context.get("run_id")
    if isinstance(run_id, str):
        return run_id
    run_id = uuid.uuid4().hex
    _CONTEXT.set({**context, "run_id": run_id})
    return run_id


@contextmanager
def bind(**fields: Any) -> Iterator[None]:
    """Add fields to the context of the lines emitted in the block."""
    token = _CONTEXT.set({**(_CONTEXT.get() or {}), **fields})
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def log_event(event: str, *, level: int = logging.INFO, **fields: Any) -> None:
    """Emit an event from the closed catalog.

    An event outside the catalog never raises (an observability problem does
    not change the exit code): it is emitted as ``log`` with
    ``unregistered_event``. What keeps a new event from reaching the code is
    the lint test.
    """
    logger = logging.getLogger("ducklake_iceberg")
    if not logger.isEnabledFor(level):
        return
    if event not in EVENTS:
        fields = {**fields, "unregistered_event": event}
        event = FREE_TEXT_EVENT
    logger.log(level, event, extra={"event": event, "event_fields": fields})


__all__ = [
    "EVENTS",
    "FREE_TEXT_EVENT",
    "LOG_SCHEMA_VERSION",
    "JsonFormatter",
    "LogFormat",
    "bind",
    "configure_logging",
    "current_run_id",
    "is_configured",
    "log_event",
    "reset_logging",
    "restore_library_levels",
]
