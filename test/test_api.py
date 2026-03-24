"""Unit tests for the mock healthcare events API."""
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


def test_health_returns_200() -> None:
    response = client.get("/health")
    assert response.status_code == 200


def test_health_returns_ok_status() -> None:
    response = client.get("/health")
    assert response.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# /events
# ---------------------------------------------------------------------------


def test_events_returns_200() -> None:
    response = client.get("/events")
    assert response.status_code == 200


def test_events_response_contains_required_fields() -> None:
    data = client.get("/events").json()
    assert "log_id" in data
    assert "log_region" in data
    assert "log_size" in data
    assert "log_datetime" in data


def test_events_includes_full_name_pii_field() -> None:
    """full_name must be present so the producer always exercises PII masking."""
    data = client.get("/events").json()
    assert "full_name" in data
    assert data["full_name"] is not None


def test_events_log_size_is_non_negative() -> None:
    data = client.get("/events").json()
    assert data["log_size"] >= 0


def test_events_log_region_is_valid() -> None:
    valid_regions = {"USA", "EMEA", "LATAM", "ASIA"}
    data = client.get("/events").json()
    assert data["log_region"] in valid_regions


def test_events_produces_unique_log_ids() -> None:
    ids = {client.get("/events").json()["log_id"] for _ in range(10)}
    assert len(ids) == 10


def test_events_log_datetime_is_iso8601() -> None:
    from datetime import datetime

    data = client.get("/events").json()
    # Should not raise — validates ISO-8601 format.
    datetime.fromisoformat(data["log_datetime"])
