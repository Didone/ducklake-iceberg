"""``stats_source: catalog`` vs ``footer``: catalog metrics never diverge.

The same DuckLake snapshot is exported twice, once with each source, and the
manifests' ``DataFile`` entries are compared file by file. Every metric the
catalog path produces must equal the footer's (the reference); the catalog
path may omit a metric (type without an exact conversion) but never differ.
The bounds are also checked against the real min/max of the rows.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pyiceberg.conversions import from_bytes
from pyiceberg.manifest import DataFile, ManifestContent

from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.export import run_export
from ducklake_iceberg.register import build_file_io, read_pointer
from ducklake_iceberg.writer.commit import load_base_snapshot
from ducklake_iceberg.writer.metadata import read_metadata_json

from .conftest import CatalogHarness

pytestmark = pytest.mark.integration

_LONG = "z" * 300 + "\u00e7"  # > 256: the catalog truncates the max


def _seed(catalog: CatalogHarness) -> None:
    catalog.write("CALL lake.set_option('data_inlining_row_limit', 0)")
    catalog.write(
        "CREATE TABLE lake.main.t ("
        " i8 TINYINT, i32 INTEGER, i64 BIGINT, b BOOLEAN, d DATE,"
        " dec DECIMAL(38,10), dec2 DECIMAL(9,2), s VARCHAR, f FLOAT, g DOUBLE,"
        " ts TIMESTAMP, tz TIMESTAMPTZ, tm TIME, u UUID, bl BLOB,"
        " st STRUCT(x INTEGER, y VARCHAR), l INTEGER[])"
    )
    catalog.write(
        "INSERT INTO lake.main.t VALUES"
        " (-128, -5, 9223372036854775807, true, DATE '1970-01-01',"
        "  12345678901234567890.0123456789, -1.25,"
        f"  '{_LONG}', 'nan'::FLOAT, -0.0,"
        "  TIMESTAMP '2024-01-01 00:00:00.123456',"
        "  TIMESTAMPTZ '2024-03-01 10:00:00.5-03', TIME '12:34:56.789012',"
        "  '00000000-0000-0000-0000-000000000001', '\\x00\\xff'::BLOB,"
        "  {'x': 1, 'y': 'a'}, [1, 2]),"
        " (127, 7, -1, false, DATE '2099-12-31', -0.0000000001, 99.99,"
        "  'caf\u00e9', 1.5, 'inf'::DOUBLE, TIMESTAMP '1999-12-31 23:59:59',"
        "  TIMESTAMPTZ '2024-03-01 13:00:00+00', TIME '00:00:00',"
        "  'ffffffff-ffff-ffff-ffff-ffffffffffff', '\\x01'::BLOB,"
        "  {'x': NULL, 'y': 'b'}, NULL),"
        " (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,"
        "  NULL, NULL, NULL, NULL, NULL, [3])"
    )
    # second file: only nulls in some columns, repeated values in others
    catalog.write(
        "INSERT INTO lake.main.t (i32, s, d) VALUES (0, 'a', DATE '2000-02-29'),"
        " (0, 'a', DATE '2000-02-29')"
    )


def _export(
    catalog: CatalogHarness, tmp_path: Path, source: str
) -> dict[str, DataFile]:
    prefix = tmp_path / f"iceberg-{source}"
    prefix.mkdir()
    settings = catalog.settings(
        iceberg_metadata_prefix=str(prefix), stats_source=source
    )
    result = run_export(settings, "main", "t")
    assert result.exit_code is ExitCode.SUCCESS
    io = build_file_io(settings)
    pointer = read_pointer(io, result.plan.location)
    metadata = read_metadata_json(io, pointer.metadata_location)
    base = load_base_snapshot(io, metadata)
    live = base.live(ManifestContent.DATA)
    return {e.data_file.file_path: e.data_file for e in live.values()}


def test_catalog_stats_never_diverge_from_footer(
    catalog: CatalogHarness, tmp_path: Path
) -> None:
    _seed(catalog)
    from_catalog = _export(catalog, tmp_path, "catalog")
    from_footer = _export(catalog, tmp_path, "footer")
    assert from_catalog.keys() == from_footer.keys()
    assert len(from_catalog) == 2

    for path, cat in from_catalog.items():
        ref = from_footer[path]
        assert cat.record_count == ref.record_count
        assert cat.file_size_in_bytes == ref.file_size_in_bytes
        for metric in (
            "column_sizes",
            "value_counts",
            "null_value_counts",
            "lower_bounds",
            "upper_bounds",
        ):
            got: dict[int, Any] = getattr(cat, metric) or {}
            want: dict[int, Any] = getattr(ref, metric) or {}
            for fid, value in got.items():
                assert fid in want, f"{metric}[{fid}] only in the catalog ({path})"
                assert value == want[fid], f"{metric}[{fid}] differs ({path})"


def test_catalog_bounds_cover_supported_types(
    catalog: CatalogHarness, tmp_path: Path
) -> None:
    """Types in the conversion table get bounds, and the bounds enclose the data."""
    _seed(catalog)
    files = _export(catalog, tmp_path, "catalog")
    first = max(files.values(), key=lambda df: df.record_count)

    from pyiceberg.types import (
        BooleanType,
        DateType,
        DecimalType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        TimestamptzType,
        TimeType,
    )

    # field id = DuckLake column_id, in CREATE TABLE order
    expected = {
        1: IntegerType(),
        2: IntegerType(),
        3: LongType(),
        4: BooleanType(),
        5: DateType(),
        6: DecimalType(38, 10),
        7: DecimalType(9, 2),
        8: StringType(),
        11: TimestampType(),
        12: TimestamptzType(),
        13: TimeType(),
    }
    lower, upper = first.lower_bounds or {}, first.upper_bounds or {}
    for fid, itype in expected.items():
        assert fid in lower, f"no lower bound for field {fid}"
        assert fid in upper, f"no upper bound for field {fid}"
        assert from_bytes(itype, lower[fid]) <= from_bytes(itype, upper[fid])
    # float, double, uuid, blob and nested types: no bound
    for fid in (9, 10, 14, 15, 16, 17, 18, 19, 20):
        assert fid not in lower
        assert fid not in upper

    # string: the real max (301 characters) is below the upper truncated to 16
    s_upper = from_bytes(StringType(), upper[8])
    s_lower = from_bytes(StringType(), lower[8])
    assert s_lower <= "caf\u00e9"
    assert s_upper > _LONG
    assert len(s_upper) == 16
