"""Fixtures shared across the test suites."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from ducklake_iceberg import aws, concurrency, storage, timing
from ducklake_iceberg.obs.logging import reset_logging
from ducklake_iceberg.obs.redaction import reset_redaction_failures


def reset_process_state() -> None:
    """Discard per-process clients/caches/metrics between isolated scenarios."""
    aws.clear_cache()
    storage.clear_file_io_cache()
    timing.reset()
    concurrency.reset_limits()
    reset_logging()
    reset_redaction_failures()


@pytest.fixture(autouse=True)
def _fresh_aws_process_state() -> Iterator[None]:
    """Never reuse a previous test's credentials, FileIO, logs or counters."""
    reset_process_state()
    yield
    reset_process_state()
