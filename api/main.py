"""Mock healthcare events API — Server-Sent Events stream.

Simulates a real source system whose UI "updates constantly as new logs come
in, or stays mostly quiet if nothing is happening."

Endpoints:
    GET  /events           — SSE stream; push one event per data: frame
    POST /inject?count=N   — queue N events to burst through the stream NOW
    GET  /health           — liveness probe

Open http://localhost:8000/events in Chrome to watch the raw stream.
Open http://localhost:8000/docs to use the Swagger UI inject button.

Quick-start examples
--------------------
# watch the stream in your terminal:
    curl -N http://localhost:8000/events

# inject 50 000 events immediately:
    curl -X POST "http://localhost:8000/inject?count=50000"

# switch to flood mode for max throughput (no sleep between events):
    curl -N "http://localhost:8000/events?mode=flood"
"""
import asyncio
import random
import uuid
from collections import deque
from collections.abc import AsyncGenerator
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse

from api.models import HealthResponse, LogEvent

app = FastAPI(
    title="Mock Healthcare Events API",
    description=(
        "SSE stream simulating a live healthcare log source with PHI. "
        "Local development and integration testing only."
    ),
    version="0.2.0",
)

_REGIONS: list[str] = ["USA", "EMEA", "LATAM", "ASIA"]

# Synthetic names — never use real PHI here.
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

# ===========================================================================
# ACTIVITY CONTROL
# Adjust these ranges to change how the stream behaves.
# Each tuple is (min_sleep_seconds, max_sleep_seconds) between organic events.
# Sleep is skipped entirely when the /inject queue has pending events.
#
# To add your own mode, add an entry here and pass ?mode=<name> to /events.
# ===========================================================================
ACTIVITY_MODES: dict[str, tuple[float, float]] = {
    "quiet":  (2.0,  8.0),    # sparse — long quiet periods between events
    "normal": (0.3,  1.5),    # realistic — moderate, variable throughput
    "burst":  (0.02, 0.2),    # heavy load — rapid but still variable
    "flood":  (0.0,  0.0),    # maximum throughput — no sleep at all
}
DEFAULT_MODE: str = "normal"
# ===========================================================================
# END ACTIVITY CONTROL
# ===========================================================================

# Shared injection queue — POST /inject appends here; SSE consumers drain it.
# Each connected client drains from the *same* deque so injected events are
# spread across all consumers (fair-share). If you want every client to
# receive every injected event, switch this to a list of per-client queues.
_injected: deque[LogEvent] = deque()


def _make_event() -> LogEvent:
    """Generate one synthetic log event.

    Returns:
        A :class:`~api.models.LogEvent` with random field values.
    """
    return LogEvent(
        log_id=str(uuid.uuid4()),
        log_region=random.choice(_REGIONS),
        log_size=random.randint(1, 1_024),
        log_datetime=datetime.now(tz=timezone.utc).isoformat(),
        full_name=random.choice(_MOCK_NAMES),
    )


async def _sse_generator(mode: str) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted event frames indefinitely.

    Injected events (from POST /inject) are drained first with no delay.
    Once the injection queue is empty, organic events are generated at the
    pace defined by ``mode``.

    Args:
        mode: One of the keys in :data:`ACTIVITY_MODES`.

    Yields:
        SSE ``data:`` frames, each containing a JSON-serialised
        :class:`~api.models.LogEvent`.
    """
    lo, hi = ACTIVITY_MODES.get(mode, ACTIVITY_MODES[DEFAULT_MODE])
    while True:
        if _injected:
            event = _injected.popleft()
            # No sleep — flush injected events as fast as possible.
        else:
            event = _make_event()
            if lo or hi:
                await asyncio.sleep(random.uniform(lo, hi))
        # SSE wire format: "data: <json>\n\n"
        yield f"data: {event.model_dump_json()}\n\n"


@app.get(
    "/events",
    summary="SSE stream of live log events",
    tags=["events"],
    response_class=StreamingResponse,
)
async def stream_events(
    mode: str = Query(
        default=DEFAULT_MODE,
        enum=list(ACTIVITY_MODES),
        description="Controls the inter-event delay. See ACTIVITY_MODES in main.py.",
    ),
) -> StreamingResponse:
    """Server-Sent Events stream of healthcare log events.

    Each frame has the format::

        data: {"log_id": "...", "log_region": "...", ...}

    Open this endpoint directly in a browser or with ``curl -N`` to watch
    events arrive in real time.  The producer subscribes to this stream and
    forwards masked events to Kafka.

    Args:
        mode: Stream activity level — ``quiet``, ``normal``, ``burst``, or
            ``flood``.  Defaults to ``normal``.

    Returns:
        An infinite ``text/event-stream`` response.
    """
    return StreamingResponse(
        _sse_generator(mode),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # Prevent nginx / proxies from buffering the stream.
            "X-Accel-Buffering": "no",
        },
    )


@app.post(
    "/inject",
    summary="Bulk-inject events into the stream",
    tags=["ops"],
)
async def inject_events(
    count: int = Query(
        default=1_000,
        ge=1,
        le=100_000,
        description=(
            "Number of events to queue (1–100 000). "
            "They are emitted immediately with no inter-event delay. "
            "For volumes beyond 100 000, call this endpoint repeatedly or "
            "use ?mode=flood on /events."
        ),
    ),
) -> dict[str, int]:
    """Queue ``count`` synthetic events for immediate emission via the SSE stream.

    Use this to simulate a traffic spike or seed the pipeline for load testing.
    Events are generated here and drained by all active SSE consumers.

    Args:
        count: How many events to inject (max 100 000 per call).

    Returns:
        Dict with ``queued`` (events added this call) and
        ``total_pending`` (total in the queue right now).

    Examples:
        Inject 50 000 events::

            curl -X POST "http://localhost:8000/inject?count=50000"

        Watch them arrive::

            curl -N http://localhost:8000/events
    """
    for i in range(count):
        _injected.append(_make_event())
        # Yield to the event loop every 10 000 iterations so we don't block
        # other requests during large injections.
        if i % 10_000 == 0:
            await asyncio.sleep(0)

    return {"queued": count, "total_pending": len(_injected)}


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
