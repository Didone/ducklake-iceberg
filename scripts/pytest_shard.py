"""Partition an existing pytest selection across independent CI jobs."""

from __future__ import annotations

from collections.abc import Iterator

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--ci-shard",
        type=_parse_shard,
        metavar="INDEX/TOTAL",
        help="Run one 1-based shard of the selected tests.",
    )


def _parse_shard(value: str) -> tuple[int, int]:
    try:
        index, total = map(int, value.split("/"))
        if 1 <= index <= total:
            return index, total
    except ValueError:
        pass
    raise pytest.UsageError("--ci-shard requires 1 <= INDEX <= TOTAL")


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> Iterator[None]:
    yield  # Apply marker and keyword selection before assigning shards.
    shard = config.getoption("--ci-shard")
    if shard is None:
        return

    index, total = shard
    assigned = {
        item.nodeid
        for item in sorted(items, key=lambda item: item.nodeid)[index - 1 :: total]
    }
    deselected = [item for item in items if item.nodeid not in assigned]
    items[:] = [item for item in items if item.nodeid in assigned]
    config.hook.pytest_deselected(items=deselected)
