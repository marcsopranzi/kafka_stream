"""Kafka producer: subscribes to the API SSE stream and forwards masked events.

Architecture change from polling to streaming:
    Previously the producer called GET /events once per POLL_INTERVAL_SECONDS.
    Now it opens a single long-lived SSE connection and processes events as they
    arrive — matching the behaviour of the real upstream source system.

Responsibilities:
    - Subscribe to ``GET /events`` as a persistent SSE stream.
    - Hash all PHI fields via :func:`shared.pii.mask_record` before the event
      is serialised and sent to Kafka — plaintext PHI never touches the broker.
    - Reconnect automatically on stream drop, with exponential backoff.
    - Three-state circuit breaker prevents hammering a degraded API.

Environment variables:
    API_BASE_URL:                   Base URL of the events API (default: http://localhost:8000)
    KAFKA_BOOTSTRAP_SERVERS:        Comma-separated broker list (default: localhost:9092)
    KAFKA_TOPIC_NAME:               Target topic (default: log-topic)
    STREAM_MODE:                    Activity mode forwarded to /events (default: normal)
    STREAM_READ_TIMEOUT_SECONDS:    Seconds of silence before treating stream as stale (default: 30)
    CB_FAILURE_THRESHOLD:           Consecutive connection failures to open circuit (default: 5)
    CB_RECOVERY_TIMEOUT_SECONDS:    Seconds in OPEN before allowing a probe (default: 30)
    PII_HASH_KEY:                   HMAC secret for PHI masking (required)
"""
import enum
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any
from datetime import datetime, timezone

import httpx
from kafka import KafkaProducer
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from shared.logging import get_logger
from shared.pii import mask_record

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Config (all from environment — no hardcoded values per CLAUDE.md §5)
# ---------------------------------------------------------------------------

API_BASE_URL: str = os.environ.get("API_BASE_URL", "http://localhost:8000")
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC_NAME", "log-topic")
# Dead Letter Queue topic — malformed SSE frames are routed here instead of discarded.
KAFKA_DLQ_TOPIC: str = os.environ.get("KAFKA_DLQ_TOPIC", "log-topic-dlq")
# Activity mode forwarded to the SSE stream — matches ACTIVITY_MODES in api/main.py.
STREAM_MODE: str = os.environ.get("STREAM_MODE", "normal")
# How long the SSE connection may be silent before we treat it as stale.
# Must be above the longest quiet-mode gap (8 s) with a safety margin.
STREAM_READ_TIMEOUT_SECONDS: float = float(
    os.environ.get("STREAM_READ_TIMEOUT_SECONDS", "30.0")
)
CB_FAILURE_THRESHOLD: int = int(os.environ.get("CB_FAILURE_THRESHOLD", "5"))
CB_RECOVERY_TIMEOUT_SECONDS: float = float(
    os.environ.get("CB_RECOVERY_TIMEOUT_SECONDS", "30.0")
)


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------


class _CBState(enum.Enum):
    CLOSED = "closed"        # Normal operation — connections pass through.
    OPEN = "open"            # API is degraded — connections refused immediately.
    HALF_OPEN = "half_open"  # One probe connection allowed to test recovery.


class CircuitBreakerOpenError(Exception):
    """Raised when a connection is attempted while the circuit is OPEN."""


@dataclass
class CircuitBreaker:
    """Three-state circuit breaker protecting the upstream events API.

    Prevents hammering a degraded API by failing fast once a failure
    threshold is crossed, then allowing a single probe after a recovery
    timeout.

    State transitions::

        CLOSED    --[N consecutive failures]--> OPEN
        OPEN      --[recovery_timeout elapsed]--> HALF_OPEN
        HALF_OPEN --[probe succeeds]-----------> CLOSED
        HALF_OPEN --[probe fails]--------------> OPEN

    Attributes:
        failure_threshold: Consecutive failures required to open the circuit.
        recovery_timeout: Seconds to wait in OPEN before allowing a probe.
    """

    failure_threshold: int = CB_FAILURE_THRESHOLD
    recovery_timeout: float = CB_RECOVERY_TIMEOUT_SECONDS
    _state: _CBState = field(default=_CBState.CLOSED, init=False, repr=False)
    _failure_count: int = field(default=0, init=False, repr=False)
    _opened_at: float = field(default=0.0, init=False, repr=False)

    @property
    def state(self) -> _CBState:
        """Current state, auto-transitioning OPEN → HALF_OPEN on timeout."""
        if (
            self._state is _CBState.OPEN
            and time.monotonic() - self._opened_at >= self.recovery_timeout
        ):
            self._state = _CBState.HALF_OPEN
            logger.info(
                "circuit_breaker_transition",
                extra={"from": "open", "to": "half_open"},
            )
        return self._state

    def _on_success(self) -> None:
        if self._state is not _CBState.CLOSED:
            logger.info(
                "circuit_breaker_transition",
                extra={"from": self._state.value, "to": "closed"},
            )
        self._state = _CBState.CLOSED
        self._failure_count = 0

    def _on_failure(self) -> None:
        self._failure_count += 1
        if (
            self._state is _CBState.HALF_OPEN
            or self._failure_count >= self.failure_threshold
        ):
            self._state = _CBState.OPEN
            self._opened_at = time.monotonic()
            logger.warning(
                "circuit_breaker_transition",
                extra={
                    "from": "closed",
                    "to": "open",
                    "consecutive_failures": self._failure_count,
                },
            )


# Module-level circuit breaker shared across all reconnect attempts.
_circuit_breaker = CircuitBreaker()


# ---------------------------------------------------------------------------
# Dead Letter Queue
# ---------------------------------------------------------------------------


def _send_to_dlq(
    kafka_producer: KafkaProducer,
    raw_payload: str,
    error_type: str,
    error_message: str,
) -> None:
    """Route a malformed record to the Dead Letter Queue Kafka topic.

    The DLQ record includes the original raw payload (truncated to 4 KiB to
    avoid oversized messages) plus enough metadata for an ops team to triage
    and replay the record.  DLQ send failures are logged but never re-raised
    — losing a DLQ record is preferable to crashing the main stream.

    Args:
        kafka_producer: Shared :class:`kafka.KafkaProducer` instance.
        raw_payload: The raw string that failed processing.
        error_type: Exception class name (e.g. ``JSONDecodeError``).
        error_message: Human-readable description of the failure.
    """
    dlq_record: dict[str, Any] = {
        "raw_payload": raw_payload[:4096],  # guard against oversized frames
        "error_type": error_type,
        "error_message": error_message,
        "source": "producer",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    try:
        kafka_producer.send(KAFKA_DLQ_TOPIC, dlq_record)
        logger.warning(
            "dlq_record_routed",
            extra={
                "dlq_topic": KAFKA_DLQ_TOPIC,
                "error_type": error_type,
                "payload_preview": raw_payload[:120],
            },
        )
    except Exception as dlq_exc:
        # DLQ itself failed — log only; do not raise to avoid masking the
        # primary stream error or creating an infinite error loop.
        logger.error(
            "dlq_send_failed",
            extra={"dlq_topic": KAFKA_DLQ_TOPIC, "error": str(dlq_exc)},
        )


# ---------------------------------------------------------------------------
# SSE stream consumer
# ---------------------------------------------------------------------------


@retry(
    retry=retry_if_exception_type(
        (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError)
    ),
    wait=wait_exponential_jitter(initial=1, max=60, jitter=2),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, log_level=logging.WARNING),
    reraise=True,
)
def consume_stream(client: httpx.Client, kafka_producer: KafkaProducer) -> None:
    """Subscribe to the API SSE stream and forward PHI-masked events to Kafka.

    Opens a single long-lived HTTP connection and processes ``data:`` frames
    as they arrive.  On connection drop, tenacity retries with exponential
    backoff.  The circuit breaker opens after repeated failures to avoid
    hammering a degraded API.

    Args:
        client: Shared :class:`httpx.Client` instance.
        kafka_producer: Shared :class:`kafka.KafkaProducer` instance.

    Raises:
        CircuitBreakerOpenError: Circuit is OPEN — caller should back off.
        httpx.HTTPStatusError: Non-2xx after all retries exhausted.
        httpx.TimeoutException: Stream read timeout after all retries exhausted.
    """
    if _circuit_breaker.state is _CBState.OPEN:
        raise CircuitBreakerOpenError(
            "Circuit is OPEN; refusing SSE connection to protect the upstream API."
        )

    url = f"{API_BASE_URL}/events"
    # 5 s default for connect/write/pool; long read timeout for the persistent stream.
    timeout = httpx.Timeout(5.0, read=STREAM_READ_TIMEOUT_SECONDS)
    records_sent: int = 0

    try:
        with client.stream(
            "GET", url, params={"mode": STREAM_MODE}, timeout=timeout
        ) as response:
            response.raise_for_status()
            # Connection established — reset failure counter.
            _circuit_breaker._on_success()
            logger.info(
                "sse_stream_connected",
                extra={"url": url, "mode": STREAM_MODE},
            )

            for line in response.iter_lines():
                # SSE format: lines starting with "data: " carry event payloads.
                # Lines starting with ":" are keep-alive comments — skip them.
                if not line.startswith("data: "):
                    continue

                try:
                    raw_event: dict[str, Any] = json.loads(line[len("data: "):])
                except json.JSONDecodeError as exc:
                    # Malformed SSE frame — route to DLQ so the record is not
                    # silently lost and can be inspected or replayed by ops.
                    _send_to_dlq(
                        kafka_producer,
                        raw_payload=line,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    continue

                masked_event: dict[str, Any] = mask_record(raw_event)
                kafka_producer.send(KAFKA_TOPIC, masked_event)
                records_sent += 1
                logger.info(
                    "event_forwarded",
                    extra={
                        "log_id": masked_event.get("log_id"),
                        "kafka_topic": KAFKA_TOPIC,
                        "records_sent_total": records_sent,
                    },
                )

    except Exception:
        _circuit_breaker._on_failure()
        raise


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------


def _build_kafka_producer() -> KafkaProducer:
    """Construct and return a configured KafkaProducer.

    Returns:
        A :class:`kafka.KafkaProducer` with JSON serialisation, gzip
        compression, and ``acks='all'`` for durability.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        compression_type="gzip",
        # Batching — critical for 1M+ message throughput.
        # linger_ms: wait up to 10ms to fill a batch before sending.
        # batch_size: max bytes per batch per partition (64KB).
        # Without these, each message incurs a separate TCP round-trip.
        linger_ms=10,
        batch_size=65536,
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def run() -> None:
    """Subscribe to the SSE stream and forward PHI-masked events to Kafka.

    The outer loop handles circuit-breaker recovery and post-exhaustion backoff.
    The inner ``consume_stream`` call handles per-connection retries via tenacity.
    """
    logger.info(
        "producer_starting",
        extra={
            "api_base_url": API_BASE_URL,
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "kafka_topic": KAFKA_TOPIC,
            "stream_mode": STREAM_MODE,
            "stream_read_timeout_s": STREAM_READ_TIMEOUT_SECONDS,
            "cb_failure_threshold": CB_FAILURE_THRESHOLD,
            "cb_recovery_timeout_s": CB_RECOVERY_TIMEOUT_SECONDS,
        },
    )

    kafka_producer = _build_kafka_producer()

    with httpx.Client() as http_client:
        while True:
            try:
                # Blocks here, processing events until the stream drops.
                consume_stream(http_client, kafka_producer)

            except CircuitBreakerOpenError:
                logger.warning(
                    "circuit_open_backing_off",
                    extra={"sleep_s": CB_RECOVERY_TIMEOUT_SECONDS},
                )
                time.sleep(CB_RECOVERY_TIMEOUT_SECONDS)

            except Exception as exc:
                # Tenacity exhausted all retries — wait before reconnecting.
                logger.error(
                    "stream_all_retries_exhausted",
                    extra={
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                        "sleep_s": CB_RECOVERY_TIMEOUT_SECONDS,
                    },
                )
                time.sleep(CB_RECOVERY_TIMEOUT_SECONDS)


if __name__ == "__main__":
    run()
