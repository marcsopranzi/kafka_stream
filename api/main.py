"""Mock healthcare events API.

Simulates an external source system that emits PHI-containing log events.
Runs locally via ``uvicorn api.main:app --reload`` or inside Docker.

Endpoints:
    GET /events  — emit one randomly generated log event (includes full_name PHI)
    GET /health  — liveness probe
"""
import random
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI

from api.models import HealthResponse, LogEvent

app = FastAPI(
    title="Mock Healthcare Events API",
    description=(
        "Simulates an external source emitting log events that contain PHI. "
        "Intended for local development and integration testing only."
    ),
    version="0.1.0",
)

_REGIONS: list[str] = ["USA", "EMEA", "LATAM", "ASIA"]

# Synthetic patient names for local mock data only — never use real PHI here.
_MOCK_NAMES: list[str] = [
    "Alice Smith",
    "Bob Johnson",
    "Carol Williams",
    "David Brown",
    "Eve Davis",
    "Frank Miller",
    "Grace Wilson",
    "Henry Moore",
    "Isla Taylor",
    "James Anderson",
]


@app.get(
    "/events",
    response_model=LogEvent,
    summary="Emit a single log event",
    tags=["events"],
)
def get_event() -> LogEvent:
    """Return one randomly generated healthcare log event.

    The ``full_name`` field is always populated to ensure the downstream
    PII masking path is exercised on every message.

    Returns:
        A :class:`~api.models.LogEvent` with randomised field values.
    """
    return LogEvent(
        log_id=str(uuid.uuid4()),
        log_region=random.choice(_REGIONS),
        log_size=random.randint(1, 1024),
        log_datetime=datetime.now(tz=timezone.utc).isoformat(),
        full_name=random.choice(_MOCK_NAMES),
    )


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    tags=["ops"],
)
def health() -> HealthResponse:
    """Return service health status.

    Returns:
        :class:`~api.models.HealthResponse` with ``status='ok'``.
    """
    return HealthResponse(status="ok")
