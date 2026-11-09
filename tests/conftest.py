"""Fixtures shared across the test suites."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from iceduck import aws, concurrency, register, retention, timing
from iceduck.obs.logging import reset_logging
from iceduck.obs.redaction import reset_redaction_failures


@pytest.fixture(autouse=True)
def _fresh_aws_process_state() -> Iterator[None]:
    """Sessions, clients and FileIO are per process; each test starts clean.

    Tests swap ``AWS_*``/moto settings within the same process; without the
    cleanup, a client created under one test's mock would leak into the next.
    """
    aws.clear_cache()
    register.clear_file_io_cache()
    timing.reset()
    concurrency.reset_limits()
    retention._SIMULATIONS.clear()
    # log handler, library log levels and the redaction failure counter
    reset_logging()
    reset_redaction_failures()
    yield
    aws.clear_cache()
    register.clear_file_io_cache()
    concurrency.reset_limits()
    retention._SIMULATIONS.clear()
    reset_logging()
    reset_redaction_failures()
