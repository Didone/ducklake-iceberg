"""Exercise the CI shard selector's collection and argument contracts."""

from __future__ import annotations

from typing import cast
from unittest.mock import Mock

import pytest
from scripts import pytest_shard


@pytest.mark.parametrize("value", ["0/3", "4/3", "1/0", "1", "a/3", "1/2/3"])
def test_invalid_shard_is_a_usage_error(value: str) -> None:
    with pytest.raises(pytest.UsageError, match="1 <= INDEX <= TOTAL"):
        pytest_shard._parse_shard(value)


def test_shards_partition_filtered_items_and_preserve_collection_order() -> None:
    candidates = [Mock(nodeid=name) for name in ("g", "b", "d", "f", "c", "a", "e")]
    marked = Mock(nodeid="ignored-by-marker-selection")
    expected = [("g", "d", "a"), ("b", "e"), ("f", "c")]

    for index, nodeids in enumerate(expected, start=1):
        config = Mock()
        config.getoption.return_value = (index, 3)
        items = [marked, *candidates]
        hook = pytest_shard.pytest_collection_modifyitems(
            config, cast("list[pytest.Item]", items)
        )
        next(hook)
        items.remove(marked)
        with pytest.raises(StopIteration):
            next(hook)

        assert tuple(item.nodeid for item in items) == nodeids
        config.hook.pytest_deselected.assert_called_once_with(
            items=[item for item in candidates if item.nodeid not in nodeids]
        )
