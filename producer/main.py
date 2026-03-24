"""Kafka producer: polls the events API and forwards masked events to Kafka.

Responsibilities (single-responsibility per service):
    - Poll ``GET /events`` on a configurable interval.
    - Hash all PHI fields via :func:`shared.pii.mask_record` **before** the
      event is serialised and sent to Kafka — PHI never touches the broker.
    - Handle API throttling and transient failures with exponential backoff
      and a three-state circuit breaker.

All configuration is via environment variables — no hardcoded values.

Environment variables:
    API_BASE_URL:               Base URL of the events API (default: http://localhost:8000)
    KAFKA_BOOTSTRAP_SERVERS:    Comma-separated broker list (default: localhost:9092)
    KAFKA_TOPIC_NAME:           Target topic (default: log-topic)
    POLL_INTERVAL_SECONDS:      Sleep between successful polls (default: 1.0)
    REQUEST_TIMEOUT_SECONDS:    Per-request HTTP timeout (default: 5.0)
    CB_FAILURE_THRESHOLD:       Consecutive failures before opening circuit (default: 5)
    CB_RECOVERY_TIMEOUT_SECONDS: Seconds before transitioning OPEN → HALF_OPEN (default: 30)
    PII_HASH_KEY:               HMAC secret for PHI masking (required)
"""
import enum
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

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
POLL_INTERVAL_SECONDS: float = float(os.environ.get("POLL_INTERVAL_SECONDS", "1.0"))
REQUEST_TIMEOUT_SECONDS: float = float(
    os.environ.get("REQUEST_TIMEOUT_SECONDS", "5.0")
)
CB_FAILURE_THRESHOLD: int = int(os.environ.get("CB_FAILURE_THRESHOLD", "5"))
CB_RECOVERY_TIMEOUT_SECONDS: float = float(
    os.environ.get("CB_RECOVERY_TIMEOUT_SECONDS", "30.0")
)


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------


class _CBState(enum.Enum):
    CLOSED = "closed"        # Normal operation — calls pass through.
    OPEN = "open"            # API is degraded — calls are rejected immediately.
    HALF_OPEN = "half_open"  # One probe request allowed to test recovery.


class CircuitBreakerOpenError(Exception):
    """Raised when a call is attempted while the circuit is OPEN."""


@dataclass
class CircuitBreaker:
    """Three-state circuit breaker protecting the upstream events API.

    Prevents hammering a degraded or throttled API by failing fast once
    a failure threshold is crossed, then allowing a single probe after a
    recovery timeout.

    State transitions::

        CLOSED  --[N consecutive failures]--> OPEN
        OPEN    --[recovery_timeout elapsed]--> HALF_OPEN
        HALF_OPEN --[probe succeeds]--> CLOSED
        HALF_OPEN --[probe fails]----> OPEN

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
        """Current circuit state, auto-transitioning OPEN → HALF_OPEN on timeout."""
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

    def __enter__(self) -> "CircuitBreaker":
        if self.state is _CBState.OPEN:
            raise CircuitBreakerOpenError(
                "Circuit is OPEN; refusing call to protect the upstream API."
            )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        if exc_type is None:
            self._on_success()
        else:
            self._on_failure()
        return False  # Never suppress exceptions.


# Module-level circuit breaker shared across all retries in this process.
_circuit_breaker = CircuitBreaker()


# ---------------------------------------------------------------------------
# API client with retry + circuit breaker
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
def fetch_event(client: httpx.Client) -> dict[str, Any]:
    """Fetch one event from the API with retry and circuit breaker protection.

    Tenacity handles transient HTTP failures with exponential backoff + jitter.
    The circuit breaker wraps the actual HTTP call to prevent retry storms
    against a fully degraded API.

    Args:
        client: A shared :class:`httpx.Client` instance (connection pooling).

    Returns:
        The raw event dict deserialised from the API JSON response.

    Raises:
        CircuitBreakerOpenError: When the circuit is OPEN (not retried).
        httpx.HTTPStatusError: On non-2xx responses after all retries exhausted.
        httpx.TimeoutException: On request timeout after all retries exhausted.
    """
    with _circuit_breaker:
        response = client.get(
            f"{API_BASE_URL}/events",
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return dict(response.json())


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
        acks="all",             # Wait for all in-sync replicas — durability first.
        retries=3,
        compression_type="gzip",
    )


# ---------------------------------------------------------------------------
# Main poll loop
# ---------------------------------------------------------------------------


def run() -> None:
    """Poll the events API indefinitely and forward PHI-masked events to Kafka.

    The loop is designed to be resilient:
    - Transient HTTP failures trigger exponential backoff via tenacity.
    - Sustained failures open the circuit breaker and sleep for the
      recovery timeout before retrying.
    - All events are masked via :func:`shared.pii.mask_record` before
      being sent to Kafka — PHI never leaves this process in plaintext.
    """
    logger.info(
        "producer_starting",
        extra={
            "api_base_url": API_BASE_URL,
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "kafka_topic": KAFKA_TOPIC,
            "poll_interval_s": POLL_INTERVAL_SECONDS,
            "cb_failure_threshold": CB_FAILURE_THRESHOLD,
            "cb_recovery_timeout_s": CB_RECOVERY_TIMEOUT_SECONDS,
        },
    )

    kafka_producer = _build_kafka_producer()
    records_sent: int = 0

    with httpx.Client() as http_client:
        while True:
            try:
                raw_event: dict[str, Any] = fetch_event(http_client)
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

            except CircuitBreakerOpenError:
                # Circuit is open — back off for the full recovery window before
                # attempting again. Do not retry immediately.
                logger.warning(
                    "circuit_open_backing_off",
                    extra={"sleep_s": CB_RECOVERY_TIMEOUT_SECONDS},
                )
                time.sleep(CB_RECOVERY_TIMEOUT_SECONDS)
                continue

            except Exception as exc:
                # Tenacity exhausted all retries — log and apply a penalised
                # sleep before starting the next poll cycle.
                logger.error(
                    "event_fetch_failed_all_retries_exhausted",
                    extra={
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                        "sleep_s": POLL_INTERVAL_SECONDS * 5,
                    },
                )
                time.sleep(POLL_INTERVAL_SECONDS * 5)
                continue

            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
