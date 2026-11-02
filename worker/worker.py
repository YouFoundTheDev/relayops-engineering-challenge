"""RelayOps delivery worker.

Events live in PostgreSQL, which is the source of truth. Every worker claims
one due event at a time with ``SELECT ... FOR UPDATE SKIP LOCKED`` and marks it
``processing`` with a lease. If the worker dies, another worker reclaims the
event after the lease expires. This is an at-least-once system: a crash during
delivery may produce a duplicate, but it never loses an event.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import signal
import socket
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse

import httpx
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

HTTP_TIMEOUT_SECONDS = 5.0
MAX_ATTEMPTS = 3
MAX_BACKOFF_SECONDS = 30.0
DEFAULT_LEASE_SECONDS = 60

logger = logging.getLogger("relayops.worker")

_EXTRA_FIELDS = (
    "worker_id",
    "event_id",
    "attempt",
    "status_code",
    "outcome",
    "reason",
    "delay_seconds",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in _EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    logging.getLogger("httpx").setLevel(logging.WARNING)


@dataclass(frozen=True)
class Settings:
    database_url: str
    receiver_url: str
    webhook_secret: str
    poll_interval: float
    lease_seconds: int
    retry_base_seconds: float

    @classmethod
    def from_env(cls) -> Settings:
        database_url = _required("DATABASE_URL")
        receiver_url = _required("RECEIVER_URL")
        webhook_secret = _required("WEBHOOK_SECRET")

        parsed = urlparse(receiver_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise RuntimeError("RECEIVER_URL must be an absolute http(s) URL")

        return cls(
            database_url=database_url,
            receiver_url=receiver_url,
            webhook_secret=webhook_secret,
            poll_interval=_positive_float("POLL_INTERVAL", 1.0),
            lease_seconds=_positive_int(
                "PROCESSING_LEASE_SECONDS", DEFAULT_LEASE_SECONDS
            ),
            retry_base_seconds=_positive_float("RETRY_BASE_SECONDS", 1.0),
        )


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be greater than zero")
    return value


def _positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be greater than zero")
    return value


def build_event_body(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": event["event_id"],
        "type": event["type"],
        "payload": event["payload"],
    }


def encode_event_body(body: dict[str, Any]) -> bytes:
    """Serialise once and sign/transmit the exact same bytes."""
    return json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def sign_body(secret: str, raw: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def classify_response(status_code: int) -> str:
    """Return ``success``, ``retry`` or ``permanent`` for an HTTP status."""
    if 200 <= status_code < 300:
        return "success"
    if status_code == 429 or 500 <= status_code < 600:
        return "retry"
    return "permanent"


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None

    try:
        seconds = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        seconds = (retry_at - (now or datetime.now(UTC))).total_seconds()

    return max(0.0, seconds)


def backoff_delay(
    attempt: int, retry_after: float | None, base_seconds: float = 1.0
) -> float:
    exponential = min(base_seconds * (2 ** max(0, attempt - 1)), MAX_BACKOFF_SECONDS)
    if retry_after is not None:
        return max(exponential, retry_after)
    return exponential


_CLAIM_SQL = """
WITH candidate AS (
    SELECT id, status AS previous_status
    FROM events
    WHERE (
            status = 'pending'
            AND next_attempt_at <= now()
          )
       OR (
            status = 'processing'
            AND (
                locked_at IS NULL
                OR locked_at <= now() - make_interval(secs => %s)
            )
          )
    ORDER BY next_attempt_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE events AS e
SET status = 'processing',
    locked_at = now(),
    locked_by = %s,
    updated_at = now()
FROM candidate AS c
WHERE e.id = c.id
RETURNING e.id,
          e.event_id,
          e.type,
          e.payload,
          e.attempts,
          e.locked_at,
          e.locked_by,
          c.previous_status AS previous_status
"""


def claim_event(
    pool: ConnectionPool, worker_id: str, lease_seconds: int
) -> dict[str, Any] | None:
    with pool.connection() as conn:
        with conn.transaction():
            return conn.execute(_CLAIM_SQL, (lease_seconds, worker_id)).fetchone()


def record_attempt(
    pool: ConnectionPool,
    *,
    event_pk: int,
    attempt: int,
    status_code: int | None,
    error: str | None,
    new_status: str,
    next_attempt_at: datetime | None,
    last_error: str | None,
) -> None:
    """Persist the attempt and the resulting event state in one transaction."""
    with pool.connection() as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO delivery_attempts (event_pk, attempt, status_code, error)
                VALUES (%s, %s, %s, %s)
                """,
                (event_pk, attempt, status_code, error),
            )
            conn.execute(
                """
                UPDATE events
                SET status = %s,
                    attempts = %s,
                    next_attempt_at = COALESCE(%s, next_attempt_at),
                    last_error = %s,
                    locked_at = NULL,
                    locked_by = NULL,
                    updated_at = now()
                WHERE id = %s
                """,
                (new_status, attempt, next_attempt_at, last_error, event_pk),
            )


def mark_event_failed(
    pool: ConnectionPool, event_pk: int, attempt: int, error: str
) -> None:
    """Mark a claimed event failed without inventing a delivery attempt row."""
    with pool.connection() as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE events
                SET status = 'failed',
                    attempts = %s,
                    last_error = %s,
                    locked_at = NULL,
                    locked_by = NULL,
                    updated_at = now()
                WHERE id = %s
                """,
                (attempt, error, event_pk),
            )


def release_event(pool: ConnectionPool, event_pk: int, reason: str) -> None:
    """Return a claimed event to the queue without losing its attempt count."""
    with pool.connection() as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE events
                SET status = 'pending',
                    next_attempt_at = now(),
                    last_error = %s,
                    locked_at = NULL,
                    locked_by = NULL,
                    updated_at = now()
                WHERE id = %s AND status = 'processing'
                """,
                (reason, event_pk),
            )


def process_event(
    client: httpx.Client,
    pool: ConnectionPool,
    settings: Settings,
    worker_id: str,
    event: dict[str, Any],
    stop_event: threading.Event,
) -> str:
    """Perform exactly one delivery attempt for a claimed event.

    Retries are scheduled in the database via ``next_attempt_at`` instead of
    sleeping inside the worker. This keeps the event claimable by exactly one
    worker per attempt: after the attempt is recorded as pending, the event is
    due only in the future, so another worker cannot deliver it concurrently.
    """
    event_pk = int(event["id"])
    event_id = str(event["event_id"])
    attempt = int(event["attempts"])

    if stop_event.is_set():
        release_event(pool, event_pk, "released before delivery on worker shutdown")
        logger.info(
            "event released on shutdown",
            extra={
                "worker_id": worker_id,
                "event_id": event_id,
                "attempt": attempt,
                "outcome": "released",
            },
        )
        return "released"

    if attempt >= MAX_ATTEMPTS:
        message = "maximum attempts already reached"
        mark_event_failed(pool, event_pk, attempt, message)
        logger.warning(
            "event failed",
            extra={
                "worker_id": worker_id,
                "event_id": event_id,
                "attempt": attempt,
                "outcome": "failed",
                "reason": "max_attempts",
            },
        )
        return "failed"

    body = build_event_body(event)
    raw = encode_event_body(body)
    headers = {
        "Content-Type": "application/json",
        "X-RelayOps-Signature": sign_body(settings.webhook_secret, raw),
    }

    attempt += 1
    status_code: int | None = None
    error: str | None = None
    retry_after: float | None = None

    try:
        response = client.post(settings.receiver_url, content=raw, headers=headers)
        status_code = response.status_code
        outcome = classify_response(status_code)
        retry_after = parse_retry_after(response.headers.get("Retry-After"))
        if outcome != "success":
            error = f"HTTP {status_code}"
    except httpx.HTTPError as exc:
        outcome = "retry"
        error = f"{type(exc).__name__}: {exc}"
    except Exception as exc:  # defensive: never leave an event claimed
        outcome = "retry"
        error = f"{type(exc).__name__}: {exc}"

    if outcome == "success":
        record_attempt(
            pool,
            event_pk=event_pk,
            attempt=attempt,
            status_code=status_code,
            error=None,
            new_status="delivered",
            next_attempt_at=None,
            last_error=None,
        )
        logger.info(
            "event delivered",
            extra={
                "worker_id": worker_id,
                "event_id": event_id,
                "attempt": attempt,
                "status_code": status_code,
                "outcome": "delivered",
            },
        )
        return "delivered"

    if outcome == "permanent":
        record_attempt(
            pool,
            event_pk=event_pk,
            attempt=attempt,
            status_code=status_code,
            error=error,
            new_status="failed",
            next_attempt_at=None,
            last_error=error,
        )
        logger.warning(
            "event failed permanently",
            extra={
                "worker_id": worker_id,
                "event_id": event_id,
                "attempt": attempt,
                "status_code": status_code,
                "outcome": "failed",
                "reason": error,
            },
        )
        return "failed"

    if attempt >= MAX_ATTEMPTS:
        record_attempt(
            pool,
            event_pk=event_pk,
            attempt=attempt,
            status_code=status_code,
            error=error,
            new_status="failed",
            next_attempt_at=None,
            last_error=error,
        )
        logger.warning(
            "event failed after maximum attempts",
            extra={
                "worker_id": worker_id,
                "event_id": event_id,
                "attempt": attempt,
                "status_code": status_code,
                "outcome": "failed",
                "reason": error,
            },
        )
        return "failed"

    delay = backoff_delay(attempt, retry_after, settings.retry_base_seconds)
    next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
    record_attempt(
        pool,
        event_pk=event_pk,
        attempt=attempt,
        status_code=status_code,
        error=error,
        new_status="pending",
        next_attempt_at=next_attempt_at,
        last_error=error,
    )
    logger.warning(
        "delivery retry scheduled",
        extra={
            "worker_id": worker_id,
            "event_id": event_id,
            "attempt": attempt,
            "status_code": status_code,
            "outcome": "retry",
            "reason": error,
            "delay_seconds": delay,
        },
    )
    return "retry_scheduled"


def _safe_release(pool: ConnectionPool, event_pk: int) -> None:
    try:
        release_event(pool, event_pk, "worker error")
    except Exception:
        logger.exception("failed to release event")


def main() -> int:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))

    try:
        settings = Settings.from_env()
    except RuntimeError as exc:
        logger.error("invalid worker configuration", extra={"reason": str(exc)})
        return 2

    worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    stop_event = threading.Event()

    def _handle_signal(signum: int, _frame: Any) -> None:
        logger.info(
            "shutdown signal received",
            extra={
                "worker_id": worker_id,
                "reason": signal.Signals(signum).name,
            },
        )
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    pool = ConnectionPool(
        settings.database_url,
        min_size=1,
        max_size=2,
        timeout=5.0,
        kwargs={"row_factory": dict_row},
        name=f"relayops-worker-{worker_id}",
        open=False,
    )
    try:
        pool.open(wait=True, timeout=30.0)
    except Exception:
        logger.exception("database unavailable at startup", extra={"worker_id": worker_id})
        pool.close()
        return 1

    logger.info("worker started", extra={"worker_id": worker_id})

    try:
        with httpx.Client(timeout=httpx.Timeout(HTTP_TIMEOUT_SECONDS)) as client:
            while not stop_event.is_set():
                try:
                    event = claim_event(pool, worker_id, settings.lease_seconds)
                except psycopg.Error:
                    logger.exception(
                        "failed to claim event", extra={"worker_id": worker_id}
                    )
                    stop_event.wait(settings.poll_interval)
                    continue

                if event is None:
                    stop_event.wait(settings.poll_interval)
                    continue

                if event["previous_status"] == "processing":
                    logger.warning(
                        "recovered event with expired lease",
                        extra={
                            "worker_id": worker_id,
                            "event_id": event["event_id"],
                            "outcome": "recovered",
                            "reason": "expired_lease",
                        },
                    )

                try:
                    process_event(client, pool, settings, worker_id, event, stop_event)
                except psycopg.Error:
                    logger.exception(
                        "database error while processing event",
                        extra={
                            "worker_id": worker_id,
                            "event_id": event["event_id"],
                        },
                    )
                    _safe_release(pool, int(event["id"]))
                except Exception:
                    logger.exception(
                        "unexpected error while processing event",
                        extra={
                            "worker_id": worker_id,
                            "event_id": event["event_id"],
                        },
                    )
                    _safe_release(pool, int(event["id"]))
    finally:
        pool.close()
        logger.info("worker stopped", extra={"worker_id": worker_id})

    return 0


if __name__ == "__main__":
    sys.exit(main())