"""parse_table_ref, without catalog access."""

from __future__ import annotations

import pytest

from iceduck.catalog.reader import DEFAULT_SCHEMA, TableRef, parse_table_ref
from iceduck.errors import ConfigError


@pytest.mark.parametrize(
    ("ref", "expected"),
    [
        ("pedidos", TableRef(DEFAULT_SCHEMA, "pedidos")),
        ("vendas.pedidos", TableRef("vendas", "pedidos")),
        ('"meu.schema".pedidos', TableRef("meu.schema", "pedidos")),
        ('"tbl.ponto"', TableRef(DEFAULT_SCHEMA, "tbl.ponto")),
    ],
)
def test_parse_valid(ref: str, expected: TableRef) -> None:
    assert parse_table_ref(ref) == expected


@pytest.mark.parametrize(
    "ref",
    ["a.b.c", "", ".pedidos", "pedidos.", "vendas..pedidos", '"aberta'],
)
def test_parse_invalid(ref: str) -> None:
    with pytest.raises(ConfigError):
        parse_table_ref(ref)


def test_default_schema_override() -> None:
    assert parse_table_ref("pedidos", default_schema="vendas") == TableRef(
        "vendas", "pedidos"
    )


def test_str_is_qualified() -> None:
    assert str(TableRef("vendas", "pedidos")) == "vendas.pedidos"


def test_name_comparison_is_case_sensitive() -> None:
    # There is no case normalization: 'Pedidos' != 'pedidos'.
    assert parse_table_ref("Pedidos").table_name == "Pedidos"


def test_int_coercion() -> None:
    from iceduck.catalog.reader import _int

    assert _int(5) == 5
    assert _int(True) == 1
    assert _int("42") == 42


def test_int_coercion_rejects_non_numeric() -> None:
    from iceduck.catalog.reader import _int
    from iceduck.errors import CatalogError

    with pytest.raises(CatalogError, match="numeric"):
        _int(object())


def test_as_datetime_rejects_non_datetime() -> None:
    from iceduck.catalog.reader import _as_datetime
    from iceduck.errors import CatalogError

    with pytest.raises(CatalogError, match="snapshot_time"):
        _as_datetime("not a datetime")
