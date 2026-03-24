"""Structured JSON logging factory for the kafka-stream pipeline.

All pipeline services use this module so log output is consistent,
machine-parseable, and ready for ingestion into any log aggregation
platform (Datadog, CloudWatch, ELK, etc.).
"""
import json
import logging
import sys
from typing import Any

# Standard LogRecord attributes that should not be forwarded as extra fields.
_STANDARD_RECORD_KEYS: frozenset[str] = frozenset(
    {
        "name",
        "msg",
        "args",
        "created",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "exc_info",
        "exc_text",
        "taskName",
    }
)


class _JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize a LogRecord to a JSON string.

        Args:
            record: The log record to format.

        Returns:
            A single-line JSON string with standard and extra fields merged.
        """
        record.message = record.getMessage()
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        # Merge any caller-supplied extra= fields.
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_KEYS and not key.startswith("_"):
                payload[key] = value

        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    """Return a structured JSON logger writing to stdout.

    Idempotent — calling multiple times with the same name returns the same
    logger without duplicating handlers.

    Args:
        name: Logger name, typically ``__name__`` of the calling module.

    Returns:
        A configured :class:`logging.Logger` instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
