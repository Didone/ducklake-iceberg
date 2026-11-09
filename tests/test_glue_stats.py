from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)

from ducklake_iceberg.glue_stats import (
    GLUE_STATS_BATCH,
    ColumnStat,
    plan_column_statistics,
    publish_column_statistics,
)
from ducklake_iceberg.obs.metrics import RunMetrics, collect


def test_plan_supported_and_unsupported_types() -> None:
    schema = Schema(
        NestedField(1, "active", BooleanType(), required=False),
        NestedField(2, "created", DateType(), required=False),
        NestedField(3, "amount", DecimalType(12, 2), required=False),
        NestedField(4, "value", DoubleType(), required=False),
        NestedField(5, "count", IntegerType(), required=False),
        NestedField(6, "name", StringType(), required=False),
        NestedField(7, "payload", BinaryType(), required=False),
        NestedField(8, "at", TimestampType(), required=False),
        NestedField(9, "nested", StructType(), required=False),
    )

    plans = plan_column_statistics(schema)

    assert [plan.glue_type for plan in plans] == [
        "BOOLEAN",
        "DATE",
        "DECIMAL",
        "DOUBLE",
        "LONG",
        "STRING",
        "BINARY",
        None,
        None,
    ]
    assert plans[7].skip_reason == "glue_type_unsupported"
    assert plans[8].skip_reason == "nested_type"
    assert plans[2].column_type == "decimal(12,2)"
    assert plans[4].column_type == "int"


def test_ndv_off_skips_types_with_required_distinct_count() -> None:
    schema = Schema(
        NestedField(1, "active", BooleanType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "payload", BinaryType(), required=False),
        NestedField(4, "at", TimestampType(), required=False),
    )

    plans = plan_column_statistics(schema, ndv="off")

    assert [plan.glue_type for plan in plans] == ["BOOLEAN", None, "BINARY", None]
    assert plans[1].skip_reason == "ndv_disabled"
    assert plans[3].skip_reason == "glue_type_unsupported"


def test_boolean_payload_uses_glue_union_shape() -> None:
    payload = ColumnStat(
        column_name="active",
        glue_type="BOOLEAN",
        number_of_nulls=2,
        number_of_trues=3,
        number_of_falses=5,
    ).to_glue(datetime(2024, 1, 1, tzinfo=UTC))

    assert payload["ColumnType"] == "boolean"
    assert payload["StatisticsData"] == {
        "Type": "BOOLEAN",
        "BooleanColumnStatisticsData": {
            "NumberOfTrues": 3,
            "NumberOfFalses": 5,
            "NumberOfNulls": 2,
        },
    }


def test_publish_batches_by_25_and_aggregates_column_errors() -> None:
    class GlueStub:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def update_column_statistics_for_table(
            self, **kwargs: object
        ) -> dict[str, object]:
            self.calls.append(kwargs)
            errors: list[dict[str, object]] = []
            if len(self.calls) == 1:
                # botocore shape: the name comes in ColumnStatistics.ColumnName
                errors.append(
                    {
                        "ColumnStatistics": {"ColumnName": "column_0"},
                        "Error": {
                            "ErrorCode": "InvalidInputException",
                            "ErrorMessage": "invalid statistic",
                        },
                    }
                )
            return {"Errors": errors}

    glue = GlueStub()
    stats = [
        ColumnStat(
            column_name=f"column_{index}",
            glue_type="STRING",
            number_of_nulls=0,
            number_of_distinct_values=1,
        )
        for index in range(GLUE_STATS_BATCH * 2 + 10)
    ]

    report = publish_column_statistics(
        glue,
        database="db",
        table="table",
        stats=stats,
        analyzed_at=datetime(2024, 1, 1, tzinfo=UTC),
    )

    sizes: list[int] = []
    for call in glue.calls:
        columns = call["ColumnStatisticsList"]
        assert isinstance(columns, list)
        sizes.append(len(columns))
    assert sizes == [25, 25, 10]
    assert report.batches == report.api_calls == 3
    assert report.columns_published == len(stats) - 1
    assert report.errors == [
        {
            "column": "column_0",
            "code": "InvalidInputException",
            "message": "invalid statistic",
        }
    ]


_AT = datetime(2024, 1, 1, tzinfo=UTC)


def _string_stats(count: int) -> list[ColumnStat]:
    return [
        ColumnStat(
            column_name=f"column_{index}",
            glue_type="STRING",
            number_of_nulls=0,
            number_of_distinct_values=1,
            maximum_length=3,
            average_length=2.5,
            column_type="string",
        )
        for index in range(count)
    ]


@pytest.fixture
def glue_client(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    return boto3.client("glue", region_name="us-east-1")


def test_errors_shape_validated_against_botocore_model(glue_client: Any) -> None:
    """``Errors[]`` with 1 column; shape validated by the Stubber.

    The ``Stubber`` validates the response against the service model (the
    column name only exists in ``ColumnStatistics.ColumnName``) and the request
    against the real ``ColumnStatisticsList``.
    """
    stats = _string_stats(60)
    failed = stats[7].to_glue(_AT)
    stubber = Stubber(glue_client)
    stubber.add_response(
        "update_column_statistics_for_table",
        {
            "Errors": [
                {
                    "ColumnStatistics": failed,
                    "Error": {
                        "ErrorCode": "InvalidInputException",
                        "ErrorMessage": "column not in table",
                    },
                }
            ]
        },
    )
    stubber.add_response("update_column_statistics_for_table", {"Errors": []})
    stubber.add_response("update_column_statistics_for_table", {})
    with stubber:
        report = publish_column_statistics(
            glue_client, database="db", table="t", stats=stats, analyzed_at=_AT
        )
    stubber.assert_no_pending_responses()

    assert report.columns_published == 59
    assert report.batches == report.api_calls == 3
    assert report.errors == [
        {
            "column": "column_7",
            "code": "InvalidInputException",
            "message": "column not in table",
        }
    ]


def test_publish_sends_analyzed_time_given(glue_client: Any) -> None:
    stubber = Stubber(glue_client)
    expected = {
        "DatabaseName": "db",
        "TableName": "t",
        "ColumnStatisticsList": [stat.to_glue(_AT) for stat in _string_stats(2)],
    }
    stubber.add_response("update_column_statistics_for_table", {}, expected)
    with stubber:
        publish_column_statistics(
            glue_client,
            database="db",
            table="t",
            stats=_string_stats(2),
            analyzed_at=_AT,
        )
    stubber.assert_no_pending_responses()


def test_publish_retries_throttling_and_counts_calls(
    glue_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient Glue error uses backoff; retries enter the metrics."""
    sleeps: list[float] = []
    monkeypatch.setattr("ducklake_iceberg.retry.time.sleep", sleeps.append)
    stubber = Stubber(glue_client)
    stubber.add_client_error(
        "update_column_statistics_for_table",
        service_error_code="ThrottlingException",
        http_status_code=400,
    )
    stubber.add_client_error(
        "update_column_statistics_for_table",
        service_error_code="InternalServiceException",
        http_status_code=500,
    )
    stubber.add_response("update_column_statistics_for_table", {})
    metrics = RunMetrics(run_id="r", command="sync")
    with stubber, collect(metrics):
        report = publish_column_statistics(
            glue_client,
            database="db",
            table="t",
            stats=_string_stats(3),
            analyzed_at=_AT,
        )
        metrics.record("glue_stats", started_at=_AT, duration_ms=1)

    assert report.columns_published == 3
    assert report.batches == 1
    assert report.api_calls == 3
    assert len(sleeps) == 2
    phase = metrics.phases[0]
    assert (phase.calls, phase.retries) == (3, 2)


def test_publish_fatal_error_is_not_retried(
    glue_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ducklake_iceberg.retry.time.sleep", lambda _: None)
    stubber = Stubber(glue_client)
    stubber.add_client_error(
        "update_column_statistics_for_table",
        service_error_code="AccessDeniedException",
        http_status_code=400,
    )
    with stubber, pytest.raises(ClientError, match="AccessDenied"):
        publish_column_statistics(
            glue_client,
            database="db",
            table="t",
            stats=_string_stats(1),
            analyzed_at=_AT,
        )
    stubber.assert_no_pending_responses()


def test_publish_without_publishable_columns_makes_no_call() -> None:
    """A timestamp-only table => 0 published, no call."""

    class NoCalls:
        def update_column_statistics_for_table(self, **_: object) -> dict[str, object]:
            raise AssertionError("should not call Glue")

    report = publish_column_statistics(NoCalls(), database="db", table="t", stats=[])
    assert (report.columns_published, report.batches, report.api_calls) == (0, 0, 0)
