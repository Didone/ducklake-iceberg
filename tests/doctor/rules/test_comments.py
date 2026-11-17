"""Lossless documentation boundaries and diagnostics, independent of AWS."""

from __future__ import annotations

import pytest

from ducklake_iceberg.mapping.schema import comment_problem


@pytest.mark.parametrize(
    ("text", "table", "valid"),
    [
        (None, False, True),
        ("", False, True),
        (" \t ", False, True),
        ("a" * 255, False, True),
        ("a" * 256, False, False),
        ("é" * 127 + "a", False, True),
        ("é" * 128, False, False),
        ("🦆" * 63 + "abc", False, True),
        ("🦆" * 64, False, False),
        ("a" * 2048, True, True),
        ("a" * 2049, True, False),
        ("é" * 1024, True, True),
        ("é" * 1024 + "a", True, False),
        ("line\r\nnext\tvalue", True, True),
        ("line\nnext", False, False),
        ("line\rnext", False, False),
        ("secret\x00phrase", True, False),
        ("\ud800", False, False),
        ("\ufffe", True, False),
        ("quoted 'text' with tab\t", False, True),
    ],
)
def test_comment_limits(text: str | None, table: bool, valid: bool) -> None:
    problem = comment_problem(text, table=table)
    assert (problem is None) == valid
    if not valid and text:
        assert text not in str(problem)


def test_nested_comment_uses_iceberg_constraints() -> None:
    assert comment_problem("long\n" * 200, nested=True) is None
    assert comment_problem("\ud800", nested=True) is not None
