import hashlib
import hmac
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest

import worker


def test_body_contains_only_contract_fields():
    event = {
        "id": 1,
        "event_id": "evt-1",
        "type": "invoice.created",
        "payload": {"amount": 250},
        "attempts": 0,
    }
    assert worker.build_event_body(event) == {
        "event_id": "evt-1",
        "type": "invoice.created",
        "payload": {"amount": 250},
    }


def test_signature_matches_receiver_algorithm():
    body = {"event_id": "evt-1", "type": "x", "payload": {"a": 1}}
    raw = worker.encode_event_body(body)
    expected = (
        "sha256="
        + hmac.new(b"secret", raw, hashlib.sha256).hexdigest()
    )
    assert worker.sign_body("secret", raw) == expected


def test_signature_covers_exact_transmitted_bytes():
    body = {"event_id": "evt-1", "type": "x", "payload": {"a": 1}}
    raw = worker.encode_event_body(body)
    # json.dumps defaults insert spaces; the transmitted bytes must not.
    assert b", " not in raw
    assert b": " not in raw
    assert worker.sign_body("secret", raw) != worker.sign_body(
        "secret", body.__repr__().encode()
    )


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (200, "success"),
        (204, "success"),
        (299, "success"),
        (301, "permanent"),
        (400, "permanent"),
        (401, "permanent"),
        (404, "permanent"),
        (429, "retry"),
        (500, "retry"),
        (503, "retry"),
    ],
)
def test_classify_response(status_code, expected):
    assert worker.classify_response(status_code) == expected


def test_parse_retry_after_seconds():
    assert worker.parse_retry_after("3") == 3.0
    assert worker.parse_retry_after("0") == 0.0
    assert worker.parse_retry_after(None) is None
    assert worker.parse_retry_after("not-a-date") is None


def test_parse_retry_after_http_date():
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    retry_at = now + timedelta(seconds=12)
    assert worker.parse_retry_after(format_datetime(retry_at), now=now) == pytest.approx(12.0)


def test_backoff_is_exponential():
    assert worker.backoff_delay(1, None) == 1.0
    assert worker.backoff_delay(2, None) == 2.0
    assert worker.backoff_delay(3, None) == 4.0


def test_backoff_respects_retry_after():
    assert worker.backoff_delay(1, 10.0) == 10.0
    assert worker.backoff_delay(3, 1.0) == 4.0


def test_settings_require_secrets(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://example/db")
    monkeypatch.setenv("RECEIVER_URL", "http://receiver:9000/webhook")
    monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="WEBHOOK_SECRET"):
        worker.Settings.from_env()


def test_settings_reject_non_http_receiver(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://example/db")
    monkeypatch.setenv("RECEIVER_URL", "ftp://receiver/webhook")
    monkeypatch.setenv("WEBHOOK_SECRET", "secret")
    with pytest.raises(RuntimeError, match="RECEIVER_URL"):
        worker.Settings.from_env()