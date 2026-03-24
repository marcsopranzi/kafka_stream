"""PII/PHI field masking for healthcare data pipelines.

Uses HMAC-SHA256 with a secret key loaded from the ``PII_HASH_KEY``
environment variable.  The digest is deterministic — the same plaintext
always produces the same hash — so downstream systems can still join on
a masked identifier without ever seeing the raw PHI.

Usage::

    from shared.pii import mask_record

    raw = {"log_id": "abc", "full_name": "Alice Smith", "log_size": 42}
    safe = mask_record(raw)
    # safe["full_name"] == "sha256:<64-char hex>"
    # safe["log_id"]   == "abc"  (not a PII field)
"""
import hashlib
import hmac
import os
from typing import Any

# Extend this set as the upstream schema introduces new PHI fields.
PII_FIELDS: frozenset[str] = frozenset(
    {
        "full_name",
        "patient_id",
        "dob",
        "ssn",
        "email",
        "phone",
    }
)


def _get_key() -> bytes:
    """Load the HMAC secret from the environment.

    Returns:
        The secret key encoded as UTF-8 bytes.

    Raises:
        RuntimeError: If ``PII_HASH_KEY`` is not set, to prevent accidentally
            processing PHI without a key.
    """
    raw = os.environ.get("PII_HASH_KEY")
    if not raw:
        raise RuntimeError(
            "PII_HASH_KEY environment variable is not set. "
            "Configure it before processing any PHI data."
        )
    return raw.encode("utf-8")


def hash_pii(value: str) -> str:
    """Return an HMAC-SHA256 hex digest of a PII string.

    The ``sha256:`` prefix signals to downstream consumers that this field
    has been masked and should not be treated as a raw value.

    Args:
        value: The plaintext PII string to hash.

    Returns:
        A string of the form ``sha256:<64-char lowercase hex>``.

    Raises:
        RuntimeError: If ``PII_HASH_KEY`` is not set.
    """
    digest = hmac.new(
        _get_key(), value.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return f"sha256:{digest}"


def mask_record(record: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``record`` with all PII fields hashed.

    Fields absent from :data:`PII_FIELDS` are forwarded unchanged.
    ``None`` values are forwarded unchanged to preserve null semantics
    (a null is not the same as a hash of an empty string).

    Args:
        record: A dict representing a single pipeline event. Not mutated.

    Returns:
        A new dict with PII fields replaced by HMAC-SHA256 digests.

    Raises:
        RuntimeError: If ``PII_HASH_KEY`` is not set.
    """
    return {
        k: hash_pii(str(v)) if (k in PII_FIELDS and v is not None) else v
        for k, v in record.items()
    }
