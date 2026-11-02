import threading
from datetime import UTC, datetime, timedelta

import httpx

import worker

INSERT_EVENT = """
INSERT INTO events (event_id, type, payload, status, attempts, next_attempt_at, locked_at)
VALUES (%s, 'invoice.created', '{}'::jsonb, %s, %s, COALESCE(%s, now()), %s)
RETURNING id
"""


def _insert(
    pool,
    event_id="evt-1",
    status="pending",
    attempts=0,
    next_attempt_at=None,
    locked_at=None,
):
    with pool.connection() as conn:
        with conn.transaction():
            return conn.execute(
                INSERT_EVENT,
                (event_id, status, attempts, next_attempt_at, locked_at),
            ).fetchone()["id"]


def _settings(retry_base_seconds=0.01):
    return worker.Settings(
        database_url="postgresql://unused/unused",
        receiver_url="http://receiver.invalid/webhook",
        webhook_secret="test-secret",
        poll_interval=0.01,
        lease_seconds=60,
        retry_base_seconds=retry_base_seconds,
    )


def _event_row(pool, event_pk):
    with pool.connection() as conn:
        return conn.execute(
            "SELECT status, attempts, locked_at, locked_by, next_attempt_at "
            "FROM events WHERE id = %s",
            (event_pk,),
        ).fetchone()


def _attempt_rows(pool, event_pk):
    with pool.connection() as conn:
        return conn.execute(
            "SELECT attempt, status_code, error FROM delivery_attempts "
            "WHERE event_pk = %s ORDER BY attempt, id",
            (event_pk,),
        ).fetchall()


def test_claim_is_atomic_between_workers(worker_pool):
    _insert(worker_pool, "evt-race")

    barrier = threading.Barrier(2)
    results = []

    def claim(worker_id):
        barrier.wait()
        results.append(worker.claim_event(worker_pool, worker_id, 60))

    threads = [
        threading.Thread(target=claim, args=(f"worker-{index}",)) for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    claimed = [row for row in results if row is not None]
    assert len(claimed) == 1
    assert claimed[0]["previous_status"] == "pending"
    assert claimed[0]["locked_by"].startswith("worker-")


def test_active_lease_is_not_claimed(worker_pool):
    _insert(
        worker_pool,
        "evt-active",
        status="processing",
        locked_at=datetime.now(UTC),
    )
    assert worker.claim_event(worker_pool, "worker-1", 60) is None


def test_expired_lease_is_recovered(worker_pool):
    event_pk = _insert(
        worker_pool,
        "evt-stale",
        status="processing",
        attempts=1,
        locked_at=datetime.now(UTC) - timedelta(seconds=120),
    )

    claimed = worker.claim_event(worker_pool, "worker-2", 60)
    assert claimed is not None
    assert claimed["id"] == event_pk
    assert claimed["previous_status"] == "processing"
    assert claimed["attempts"] == 1


def test_future_retry_is_not_claimed(worker_pool):
    _insert(
        worker_pool,
        "evt-future",
        next_attempt_at=datetime.now(UTC) + timedelta(hours=1),
    )
    assert worker.claim_event(worker_pool, "worker-1", 60) is None


def test_release_returns_event_to_queue(worker_pool):
    event_pk = _insert(worker_pool, "evt-release")
    claimed = worker.claim_event(worker_pool, "worker-1", 60)
    assert claimed is not None

    worker.release_event(worker_pool, event_pk, "test release")

    row = _event_row(worker_pool, event_pk)
    assert row["status"] == "pending"
    assert row["locked_at"] is None
    assert row["locked_by"] is None


def test_record_attempt_updates_event_and_history(worker_pool):
    event_pk = _insert(worker_pool, "evt-attempt")
    worker.record_attempt(
        worker_pool,
        event_pk=event_pk,
        attempt=1,
        status_code=503,
        error="HTTP 503",
        new_status="pending",
        next_attempt_at=datetime.now(UTC) + timedelta(seconds=5),
        last_error="HTTP 503",
    )

    row = _event_row(worker_pool, event_pk)
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    attempts = _attempt_rows(worker_pool, event_pk)
    assert attempts == [{"attempt": 1, "status_code": 503, "error": "HTTP 503"}]


def test_network_failures_are_retried_and_recorded(worker_pool):
    event_pk = _insert(worker_pool, "evt-network")

    def handler(request):
        raise httpx.ConnectError("connection refused", request=request)

    result = None
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        for _ in range(5):
            event = worker.claim_event(worker_pool, "worker-1", 60)
            if event is None:
                continue
            result = worker.process_event(
                client,
                worker_pool,
                _settings(retry_base_seconds=0),
                "worker-1",
                event,
                threading.Event(),
            )
            if result == "failed":
                break

    assert result == "failed"
    row = _event_row(worker_pool, event_pk)
    assert row["status"] == "failed"
    assert row["attempts"] == 3

    attempts = _attempt_rows(worker_pool, event_pk)
    assert len(attempts) == 3
    assert all(attempt["status_code"] is None for attempt in attempts)
    assert all("ConnectError" in attempt["error"] for attempt in attempts)


def test_shutdown_before_delivery_releases_event(worker_pool):
    event_pk = _insert(worker_pool, "evt-shutdown")

    event = worker.claim_event(worker_pool, "worker-1", 60)
    assert event is not None
    stop_event = threading.Event()
    stop_event.set()

    with httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200))
    ) as client:
        result = worker.process_event(
            client, worker_pool, _settings(), "worker-1", event, stop_event
        )

    assert result == "released"
    row = _event_row(worker_pool, event_pk)
    assert row["status"] == "pending"
    assert row["locked_at"] is None
    assert row["attempts"] == 0
    assert _attempt_rows(worker_pool, event_pk) == []


def test_transient_failure_schedules_retry(worker_pool):
    event_pk = _insert(worker_pool, "evt-interrupted")

    def handler(request):
        return httpx.Response(503)

    event = worker.claim_event(worker_pool, "worker-1", 60)
    assert event is not None
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = worker.process_event(
            client,
            worker_pool,
            _settings(retry_base_seconds=30),
            "worker-1",
            event,
            threading.Event(),
        )

    assert result == "retry_scheduled"
    row = _event_row(worker_pool, event_pk)
    assert row["status"] == "pending"
    assert row["locked_at"] is None
    assert row["attempts"] == 1
    assert row["next_attempt_at"] > datetime.now(UTC)
    attempts = _attempt_rows(worker_pool, event_pk)
    assert len(attempts) == 1
    assert attempts[0]["status_code"] == 503
