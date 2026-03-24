"""Unit tests for shared.pii — PHI masking functions."""
import os
from typing import Any
from unittest.mock import patch

import pytest

from shared.pii import PII_FIELDS, hash_pii, mask_record

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def set_pii_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inject a deterministic test key for all tests in this module."""
    monkeypatch.setenv("PII_HASH_KEY", "test-secret-key-do-not-use-in-prod")


# ---------------------------------------------------------------------------
# hash_pii
# ---------------------------------------------------------------------------


def test_hash_pii_returns_sha256_prefix() -> None:
    result = hash_pii("Alice Smith")
    assert result.startswith("sha256:")


def test_hash_pii_is_deterministic() -> None:
    assert hash_pii("Alice Smith") == hash_pii("Alice Smith")


def test_hash_pii_different_inputs_produce_different_hashes() -> None:
    assert hash_pii("Alice Smith") != hash_pii("Bob Johnson")


def test_hash_pii_digest_is_64_chars() -> None:
    # sha256 hex digest is always 64 characters.
    _, digest = hash_pii("Alice Smith").split(":", 1)
    assert len(digest) == 64


def test_hash_pii_raises_without_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PII_HASH_KEY", raising=False)
    with pytest.raises(RuntimeError, match="PII_HASH_KEY"):
        hash_pii("Alice Smith")


def test_hash_pii_changes_with_different_key(monkeypatch: pytest.MonkeyPatch) -> None:
    hash_a = hash_pii("Alice Smith")
    monkeypatch.setenv("PII_HASH_KEY", "different-key")
    hash_b = hash_pii("Alice Smith")
    assert hash_a != hash_b


# ---------------------------------------------------------------------------
# mask_record
# ---------------------------------------------------------------------------


def test_mask_record_masks_known_pii_fields() -> None:
    record: dict[str, Any] = {"log_id": "abc", "full_name": "Alice Smith"}
    result = mask_record(record)
    assert result["full_name"].startswith("sha256:")


def test_mask_record_passes_through_non_pii_fields() -> None:
    record: dict[str, Any] = {"log_id": "abc", "log_size": 42, "full_name": "Alice"}
    result = mask_record(record)
    assert result["log_id"] == "abc"
    assert result["log_size"] == 42


def test_mask_record_preserves_none_values() -> None:
    """None PHI fields must not be hashed — null != hash(empty string)."""
    record: dict[str, Any] = {"full_name": None, "log_id": "abc"}
    result = mask_record(record)
    assert result["full_name"] is None


def test_mask_record_does_not_mutate_input() -> None:
    record: dict[str, Any] = {"full_name": "Alice Smith", "log_id": "abc"}
    original_name = record["full_name"]
    mask_record(record)
    assert record["full_name"] == original_name


def test_mask_record_empty_record() -> None:
    assert mask_record({}) == {}


def test_mask_record_masks_all_pii_fields() -> None:
    """All fields in PII_FIELDS should be masked when present."""
    record: dict[str, Any] = {f: f"value-{f}" for f in PII_FIELDS}
    result = mask_record(record)
    for field in PII_FIELDS:
        assert result[field].startswith("sha256:"), f"{field} was not masked"


def test_mask_record_unknown_fields_passed_through() -> None:
    record: dict[str, Any] = {"log_region": "USA", "log_datetime": "2026-01-01"}
    result = mask_record(record)
    assert result == record
