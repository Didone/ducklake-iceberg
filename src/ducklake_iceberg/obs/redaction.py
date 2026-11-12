"""Secret redaction for logs, the envelope, and the terminal.

Defense in depth, in this order: ``scrub()`` at the boundary (every exception
converted into a ``Problem`` or log), ``scrub_mapping()`` in the envelope
serializer, and ``SecretFilter`` as the last gate of the log handler,
including records from third-party libraries.
"""

from __future__ import annotations

import logging
import re
import threading
from typing import Any

#: Applied in order; each one replaces the secret with ``***``.
SECRET_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    # the ``\w*`` prefix catches ``aws_secret_access_key=``, ``aws_session_token=``
    (
        re.compile(
            r"(?i)\b(\w*(?:password|passwd)|pwd)\s*=\s*"
            r"(?:'(?:\\.|[^'\\])*(?:'|$)|\"(?:\\.|[^\"\\])*(?:\"|$)|\S+)"
        ),
        r"\1=***",
    ),
    (
        re.compile(
            r"(?i)\b(\w*(?:secret|token|credential)\w*)\s*=\s*"
            r"(?:'(?:\\.|[^'\\])*(?:'|$)|\"(?:\\.|[^\"\\])*(?:\"|$)|\S+)"
        ),
        r"\1=***",
    ),
    (re.compile(r"(?i)://[^/\s:@]+:[^/\s@]+@"), "://***:***@"),
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "***"),
    (re.compile(r"\bASIA[0-9A-Z]{16}\b"), "***"),
    (
        re.compile(r"(?i)\bX-Amz-(Signature|Security-Token)=[^&\s]+"),
        r"X-Amz-\1=***",
    ),
    (re.compile(r"(?i)\bhost\s*=\s*\S+"), "host=***"),
)

#: Field names that are never serialized, whatever the value.
#: ``catalog_dsn`` is not here: the envelope carries the ``redacted()``
#: version, and the value still goes through ``scrub``.
FORBIDDEN_FIELDS = frozenset(
    {
        "catalog_dsn_raw",
        "password",
        "secret",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "data_file_encryption_key",
        "encryption_key",
        "key_metadata",
        "authorization",
    }
)

#: Fields kept with the value replaced by ``***`` (the key is informative).
MASKED_FIELDS = frozenset({"host"})

_MASK = "***"
_failures_lock = threading.Lock()
_failures = 0


def scrub(text: str) -> str:
    """Redact secrets in free text (exception messages included)."""
    for pattern, replacement in SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text.replace("\r", " ").replace("\n", " ")


def scrub_mapping(data: Any) -> Any:
    """Recursive: drop ``FORBIDDEN_FIELDS``, mask ``MASKED_FIELDS``, and
    apply ``scrub`` to ``str``."""
    if isinstance(data, dict):
        out: dict[Any, Any] = {}
        for key, value in data.items():
            name = key.lower() if isinstance(key, str) else None
            if name in FORBIDDEN_FIELDS:
                continue
            out[key] = (
                _MASK
                if name in MASKED_FIELDS and value is not None
                else scrub_mapping(value)
            )
        return out
    if isinstance(data, list):
        return [scrub_mapping(value) for value in data]
    if isinstance(data, tuple):
        return tuple(scrub_mapping(value) for value in data)
    if isinstance(data, str):
        return scrub(data)
    return data


def redaction_failures() -> int:
    """Lines dropped by ``SecretFilter`` since the last reset."""
    with _failures_lock:
        return _failures


def reset_redaction_failures() -> None:
    """Reset the failure counter (start of each command; tests)."""
    global _failures
    with _failures_lock:
        _failures = 0


def _note_failure() -> None:
    global _failures
    with _failures_lock:
        _failures += 1


class SecretFilter(logging.Filter):
    """Last gate: apply ``scrub_mapping`` to the already assembled record.

    Fail-closed: if redaction raises, the line is dropped and the failure is
    counted; the command attaches ``problem code=redaction_failed`` to the
    envelope without changing the exit code.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            # the message is formatted before redaction: redacting the template
            # (``"host=%s"``) would break ``%`` with the arguments, and a
            # non-``str`` argument (an exception with the DSN) only becomes
            # text on formatting
            record.args = scrub_mapping(record.args)
            if isinstance(record.msg, dict | list | tuple):
                record.msg = scrub_mapping(record.msg)
            if record.args or not isinstance(record.msg, str):
                record.msg = scrub(record.getMessage())
                record.args = None
            else:
                record.msg = scrub(record.msg)
            for key, value in list(record.__dict__.items()):
                if key not in {"msg", "args"}:
                    record.__dict__[key] = scrub_mapping(value)
            if record.exc_info:
                record.exc_text = scrub(
                    logging.Formatter().formatException(record.exc_info)
                )
                record.exc_info = None
            return True
        except Exception:
            _note_failure()
            return False


__all__ = [
    "FORBIDDEN_FIELDS",
    "MASKED_FIELDS",
    "SECRET_PATTERNS",
    "SecretFilter",
    "redaction_failures",
    "reset_redaction_failures",
    "scrub",
    "scrub_mapping",
]
