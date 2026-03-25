"""Unit tests for Dead Letter Queue (DLQ) routing — Req §5.

Tests cover:
  - Producer: malformed SSE frames are routed to the DLQ topic, not silently dropped.
  - Producer: DLQ send failures are logged but do not crash the main stream.
  - Spark consumer: records that fail from_json parsing are split into a DLQ DataFrame.
"""
import json
import os
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Producer DLQ tests
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _set_pii_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure PII_HASH_KEY is set for all tests in this module."""
    monkeypatch.setenv("PII_HASH_KEY", "test-key-for-dlq-tests")


def _make_kafka_producer() -> MagicMock:
    """Return a mock KafkaProducer with a callable send method."""
    mock = MagicMock()
    mock.send = MagicMock()
    return mock


class TestProducerDlq:
    """_send_to_dlq routes malformed SSE frames to KAFKA_DLQ_TOPIC."""

    def test_send_to_dlq_calls_producer_send(self) -> None:
        """A malformed payload is published to the DLQ topic with metadata."""
        from producer.main import KAFKA_DLQ_TOPIC, _send_to_dlq

        kafka_producer = _make_kafka_producer()
        raw = "data: {not valid json"

        _send_to_dlq(
            kafka_producer,
            raw_payload=raw,
            error_type="JSONDecodeError",
            error_message="Expecting property name",
        )

        kafka_producer.send.assert_called_once()
        topic_arg, payload_arg = kafka_producer.send.call_args.args
        assert topic_arg == KAFKA_DLQ_TOPIC
        assert payload_arg["raw_payload"] == raw
        assert payload_arg["error_type"] == "JSONDecodeError"
        assert payload_arg["source"] == "producer"
        assert "timestamp_utc" in payload_arg

    def test_send_to_dlq_truncates_large_payload(self) -> None:
        """Payloads larger than 4096 bytes are truncated to avoid oversized messages."""
        from producer.main import _send_to_dlq

        kafka_producer = _make_kafka_producer()
        huge_payload = "x" * 10_000

        _send_to_dlq(
            kafka_producer,
            raw_payload=huge_payload,
            error_type="JSONDecodeError",
            error_message="unexpected end of data",
        )

        _, payload_arg = kafka_producer.send.call_args.args
        assert len(payload_arg["raw_payload"]) == 4096

    def test_dlq_send_failure_does_not_raise(self) -> None:
        """If the DLQ topic itself is unavailable, _send_to_dlq must not raise."""
        from producer.main import _send_to_dlq

        kafka_producer = _make_kafka_producer()
        kafka_producer.send.side_effect = RuntimeError("broker unavailable")

        # Must not propagate the exception.
        _send_to_dlq(
            kafka_producer,
            raw_payload="data: bad",
            error_type="JSONDecodeError",
            error_message="parse error",
        )

    def test_malformed_sse_frame_routed_to_dlq(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """In consume_stream, a JSON decode error routes to DLQ, not exception."""
        import httpx
        from producer.main import consume_stream

        kafka_producer = _make_kafka_producer()
        dlq_calls: list[Any] = []

        # Patch _send_to_dlq to capture calls without touching Kafka.
        with patch("producer.main._send_to_dlq", side_effect=lambda *a, **kw: dlq_calls.append(kw or a)):
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            mock_response.__enter__ = MagicMock(return_value=mock_response)
            mock_response.__exit__ = MagicMock(return_value=False)
            # One bad SSE frame then stop iteration.
            mock_response.iter_lines = MagicMock(return_value=iter(["data: {bad json"]))

            mock_client = MagicMock(spec=httpx.Client)
            mock_client.stream = MagicMock(return_value=mock_response)

            consume_stream(mock_client, kafka_producer)

        assert len(dlq_calls) == 1, "Expected exactly one DLQ routing call"
        # Verify the normal topic was NOT called for the malformed frame.
        for send_call in kafka_producer.send.call_args_list:
            topic = send_call.args[0] if send_call.args else send_call[0][0]
            assert topic != "log-topic", "Malformed frame must not reach main topic"


# ---------------------------------------------------------------------------
# Spark consumer DLQ split tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():  # type: ignore[return]
    """Minimal SparkSession for DataFrame unit tests.

    Importing pyspark at module scope would fail in environments without Spark.
    Use ``importorskip`` so the test is skipped gracefully, not errored.
    """
    pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("pytest-dlq")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestSparkDlqSplit:
    """from_json PERMISSIVE-mode NULL struct is identified and split into DLQ."""

    def test_valid_records_not_in_dlq(self, spark) -> None:  # type: ignore[no-untyped-def]
        """Well-formed JSON records must not appear in the DLQ split."""
        from pyspark.sql import functions as fn
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("log_id", StringType(), True),
                StructField("log_region", StringType(), True),
                StructField("log_size", IntegerType(), True),
            ]
        )
        valid_json = json.dumps({"log_id": "abc", "log_region": "us-east", "log_size": 100})
        df = spark.createDataFrame([(valid_json,)], ["json_value"])

        with_parsed = df.select(
            fn.col("json_value"),
            fn.from_json(fn.col("json_value"), schema).alias("data"),
        )

        dlq_df = with_parsed.filter(fn.col("data").isNull())
        assert dlq_df.count() == 0

    def test_malformed_json_lands_in_dlq(self, spark) -> None:  # type: ignore[no-untyped-def]
        """Unparseable JSON must produce a NULL data struct, captured by DLQ filter."""
        from pyspark.sql import functions as fn
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("log_id", StringType(), True),
                StructField("log_region", StringType(), True),
                StructField("log_size", IntegerType(), True),
            ]
        )
        bad_json = "this is not json at all"
        df = spark.createDataFrame([(bad_json,)], ["json_value"])

        with_parsed = df.select(
            fn.col("json_value"),
            fn.from_json(fn.col("json_value"), schema).alias("data"),
        )

        dlq_df = with_parsed.filter(fn.col("data").isNull()).select(
            fn.col("json_value"),
            fn.lit("json_schema_parse_failure").alias("dlq_reason"),
        )

        rows = dlq_df.collect()
        assert len(rows) == 1
        assert rows[0]["json_value"] == bad_json
        assert rows[0]["dlq_reason"] == "json_schema_parse_failure"

    def test_mixed_batch_splits_correctly(self, spark) -> None:  # type: ignore[no-untyped-def]
        """A batch with mixed valid/invalid records is split cleanly."""
        from pyspark.sql import functions as fn
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("log_id", StringType(), True),
                StructField("log_region", StringType(), True),
                StructField("log_size", IntegerType(), True),
            ]
        )
        good = json.dumps({"log_id": "id-1", "log_region": "eu-west", "log_size": 50})
        bad = "{incomplete json"
        df = spark.createDataFrame([(good,), (bad,)], ["json_value"])

        with_parsed = df.select(
            fn.col("json_value"),
            fn.from_json(fn.col("json_value"), schema).alias("data"),
        )

        valid_count = with_parsed.filter(fn.col("data").isNotNull()).count()
        dlq_count = with_parsed.filter(fn.col("data").isNull()).count()

        assert valid_count == 1
        assert dlq_count == 1
