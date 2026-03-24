"""Pydantic data contracts for the mock healthcare events API.

These models are the canonical schema definition — both the API and the
producer import from here so the contract is defined in one place.

The ``full_name`` field is deliberately included to exercise the PII masking
requirement (REQUIREMENTS.md §3) in the producer before events reach Kafka.
"""
from typing import Annotated, Optional

from pydantic import BaseModel, Field


class LogEvent(BaseModel):
    """A single healthcare system log event emitted by the source API.

    Attributes:
        log_id: UUID identifying this specific log entry.
        log_region: Geographic region where the event originated.
        log_size: Payload size in bytes. Must be non-negative.
        log_datetime: ISO-8601 UTC timestamp of the event.
        full_name: Patient or user full name.
            **PHI — must be masked via :func:`shared.pii.mask_record`
            before writing to any downstream zone (Kafka, S3, Cassandra).**
    """

    log_id: str = Field(..., description="UUID event identifier")
    log_region: Annotated[str, Field(description="Geographic region of origin")]
    log_size: Annotated[int, Field(ge=0, description="Payload size in bytes")]
    log_datetime: str = Field(..., description="ISO-8601 UTC event timestamp")
    full_name: Optional[str] = Field(
        default=None,
        description="Patient full name. PHI — mask before any downstream write.",
    )


class HealthResponse(BaseModel):
    """Response body for the /health liveness endpoint."""

    status: str
