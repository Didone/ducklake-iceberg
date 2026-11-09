from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from pyiceberg.catalog.glue import GlueCatalog

from iceduck.cli import _iceberg_references


def test_iceberg_references_include_historical_snapshot_data_and_deletes() -> None:
    table = SimpleNamespace(
        metadata=SimpleNamespace(
            snapshots=[
                SimpleNamespace(snapshot_id=1),
                SimpleNamespace(snapshot_id=2),
            ]
        ),
        scan=lambda *, snapshot_id: SimpleNamespace(
            plan_files=lambda: (
                [
                    SimpleNamespace(
                        file=SimpleNamespace(
                            file_path=(
                                "s3://bucket/data/old.parquet"
                                if snapshot_id == 1
                                else "s3://bucket/data/new.parquet"
                            )
                        ),
                        delete_files=(
                            SimpleNamespace(
                                file_path="s3://bucket/data/retained-delete.parquet"
                            ),
                        ),
                    )
                ]
            )
        ),
    )
    glue = SimpleNamespace(load_table=lambda _identifier: table)

    referenced = _iceberg_references(
        cast(GlueCatalog, glue),
        [("analytics", "orders")],
        {"s3://bucket/data/old.parquet"},
    )

    assert referenced == {"s3://bucket/data/old.parquet"}


def test_iceberg_references_include_retained_delete_files() -> None:
    table = SimpleNamespace(
        metadata=SimpleNamespace(snapshots=[SimpleNamespace(snapshot_id=1)]),
        scan=lambda **_kwargs: SimpleNamespace(
            plan_files=lambda: [
                SimpleNamespace(
                    file=SimpleNamespace(file_path="s3://bucket/data/current.parquet"),
                    delete_files=(
                        SimpleNamespace(
                            file_path="s3://bucket/data/position-delete.parquet"
                        ),
                    ),
                )
            ]
        ),
    )
    glue = SimpleNamespace(load_table=lambda _identifier: table)

    referenced = _iceberg_references(
        cast(GlueCatalog, glue),
        [("analytics", "orders")],
        {"s3://bucket/data/position-delete.parquet"},
    )

    assert referenced == {"s3://bucket/data/position-delete.parquet"}
