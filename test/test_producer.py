"""Unit tests for the Kafka producer — circuit breaker and fetch logic.

All external I/O (HTTP, Kafka) is mocked so these tests run without any
running infrastructure.
"""
import time
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from producer.main import (
    CB_FAILURE_THRESHOLD,
    CircuitBreaker,
    CircuitBreakerOpenError,
    _CBState,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def cb() -> CircuitBreaker:
    """Fresh circuit breaker for each test."""
    return CircuitBreaker(failure_threshold=3, recovery_timeout=30.0)


# ---------------------------------------------------------------------------
# CircuitBreaker — initial state
# ---------------------------------------------------------------------------


def test_circuit_breaker_starts_closed(cb: CircuitBreaker) -> None:
    assert cb.state is _CBState.CLOSED


# ---------------------------------------------------------------------------
# CircuitBreaker — CLOSED → OPEN transition
# ---------------------------------------------------------------------------


def test_circuit_opens_after_threshold_failures(cb: CircuitBreaker) -> None:
    for _ in range(cb.failure_threshold):
        cb._on_failure()
    assert cb.state is _CBState.OPEN


def test_circuit_does_not_open_before_threshold(cb: CircuitBreaker) -> None:
    for _ in range(cb.failure_threshold - 1):
        cb._on_failure()
    assert cb.state is _CBState.CLOSED


def test_open_circuit_state_after_threshold(cb: CircuitBreaker) -> None:
    """After threshold failures the circuit is OPEN (no context manager needed)."""
    for _ in range(cb.failure_threshold):
        cb._on_failure()
    assert cb.state is _CBState.OPEN


# ---------------------------------------------------------------------------
# CircuitBreaker — OPEN → HALF_OPEN transition
# ---------------------------------------------------------------------------


def test_circuit_transitions_to_half_open_after_timeout(
    cb: CircuitBreaker,
) -> None:
    for _ in range(cb.failure_threshold):
        cb._on_failure()

    # Simulate timeout elapsed by backdating _opened_at.
    cb._opened_at = time.monotonic() - cb.recovery_timeout - 1
    assert cb.state is _CBState.HALF_OPEN


# ---------------------------------------------------------------------------
# CircuitBreaker — HALF_OPEN → CLOSED / OPEN transitions
# ---------------------------------------------------------------------------


def test_successful_probe_closes_circuit(cb: CircuitBreaker) -> None:
    cb._state = _CBState.HALF_OPEN
    cb._on_success()
    assert cb.state is _CBState.CLOSED


def test_failed_probe_reopens_circuit(cb: CircuitBreaker) -> None:
    cb._state = _CBState.HALF_OPEN
    cb._on_failure()
    assert cb.state is _CBState.OPEN


# ---------------------------------------------------------------------------
# CircuitBreaker — success resets failure counter
# ---------------------------------------------------------------------------


def test_success_resets_failure_count(cb: CircuitBreaker) -> None:
    for _ in range(cb.failure_threshold - 1):
        cb._on_failure()
    cb._on_success()
    # After a success, a single failure should not open the circuit.
    cb._on_failure()
    assert cb.state is _CBState.CLOSED


# ---------------------------------------------------------------------------
# consume_stream — circuit breaker integration
# ---------------------------------------------------------------------------


def test_consume_stream_raises_circuit_open_without_retry() -> None:
    """CircuitBreakerOpenError must propagate immediately when circuit is OPEN."""
    from producer.main import _circuit_breaker, consume_stream

    # Force the circuit open.
    original_state = _circuit_breaker._state
    _circuit_breaker._state = _CBState.OPEN
    _circuit_breaker._opened_at = time.monotonic()

    mock_client = MagicMock(spec=httpx.Client)
    mock_kafka = MagicMock()

    try:
        with pytest.raises(CircuitBreakerOpenError):
            consume_stream(mock_client, mock_kafka)
    finally:
        # Restore state so other tests are unaffected.
        _circuit_breaker._state = original_state
        _circuit_breaker._failure_count = 0
