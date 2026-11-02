import time
import uuid

import httpx
import pytest
from helpers import (
    parse_prometheus,
    received_for,
    wait_for_received,
    wait_for_status,
)

pytestmark = pytest.mark.integration


def _event(event_id, payload=None, event_type="invoice.created"):
    return {
        "event_id": event_id,
        "type": event_type,
        "payload": {"amount": 250} if payload is None else payload,
    }


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"event_id": "", "type": "invoice.created", "payload": {}},
        {"event_id": "x", "type": "", "payload": {}},
        {"event_id": "x", "type": "invoice.created", "payload": "not-an-object"},
        {"event_id": "x", "type": "invoice.created", "payload": [1, 2]},
    ],
)
def test_invalid_payload_returns_422(api, body):
    response = api.post("/events", json=body)
    assert response.status_code == 422


def test_new_event_returns_202(api):
    event_id = f"evt-{uuid.uuid4()}"
    response = api.post("/events", json=_event(event_id))
    assert response.status_code == 202
    assert response.json() == {"event_id": event_id, "status": "pending"}


def test_duplicate_same_content_is_idempotent(api, receiver):
    event_id = f"evt-{uuid.uuid4()}"
    event = _event(event_id)

    assert api.post("/events", json=event).status_code == 202
    duplicate = api.post("/events", json=event)
    assert duplicate.status_code == 200
    assert duplicate.json()["event_id"] == event_id

    wait_for_status(api, event_id, "delivered")
    wait_for_received(receiver, event_id, 1)
    time.sleep(1.0)
    assert len(received_for(receiver, event_id)) == 1


def test_conflicting_content_returns_409(api):
    event_id = f"evt-{uuid.uuid4()}"
    assert api.post("/events", json=_event(event_id)).status_code == 202

    conflict = api.post(
        "/events",
        json={"event_id": event_id, "type": "invoice.paid", "payload": {"amount": 1}},
    )
    assert conflict.status_code == 409

    stored = api.get(f"/events/{event_id}").json()
    assert stored["type"] == "invoice.created"
    assert stored["payload"] == {"amount": 250}


def test_missing_event_returns_404(api):
    response = api.get(f"/events/{uuid.uuid4()}")
    assert response.status_code == 404


def test_request_id_is_preserved_and_generated(api):
    provided = f"req-{uuid.uuid4()}"
    response = api.get("/health", headers={"X-Request-ID": provided})
    assert response.status_code == 200
    assert response.headers["x-request-id"] == provided

    generated = api.get("/health")
    assert generated.headers.get("x-request-id")


def test_event_includes_delivery_attempt_history(api, receiver):
    event_id = f"evt-{uuid.uuid4()}"
    api.post("/events", json=_event(event_id))

    wait_for_status(api, event_id, "delivered")
    body = api.get(f"/events/{event_id}").json()

    assert body["attempts"] == 1
    assert len(body["delivery_attempts"]) == 1
    attempt = body["delivery_attempts"][0]
    assert attempt["attempt"] == 1
    assert attempt["status_code"] == 200
    assert attempt["error"] is None
    assert attempt["created_at"]


def test_health_reports_database(api):
    response = api.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}


def test_metrics_reflect_actual_state(api, receiver):
    before = parse_prometheus(api.get("/metrics").text)
    event_id = f"evt-{uuid.uuid4()}"
    api.post("/events", json=_event(event_id))
    wait_for_status(api, event_id, "delivered")

    after = parse_prometheus(api.get("/metrics").text)
    assert set(after) >= {
        "events_pending",
        "events_processing",
        "events_delivered",
        "events_failed",
        "delivery_attempts_total",
    }
    assert after["events_delivered"] >= before["events_delivered"] + 1
    assert after["delivery_attempts_total"] >= before["delivery_attempts_total"] + 1


def test_one_megabyte_payload_is_accepted(api):
    event_id = f"evt-{uuid.uuid4()}"
    response = api.post("/events", json=_event(event_id, payload={"blob": "x" * 1_000_000}))
    assert response.status_code == 202
    wait_for_status(api, event_id, "delivered", timeout=30)

def test_concurrent_duplicate_submissions_create_one_event(api, receiver):
    import concurrent.futures

    event_id = f"evt-{uuid.uuid4()}"
    event = _event(event_id)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        responses = list(
            executor.map(
                lambda _: httpx.post(
                    f"{api.base_url}/events", json=event, timeout=10
                ),
                range(5),
            )
        )

    codes = sorted(response.status_code for response in responses)
    assert codes.count(202) == 1
    assert codes.count(200) == 4

    wait_for_status(api, event_id, "delivered")
    wait_for_received(receiver, event_id, 1)
    time.sleep(1.0)
    assert len(received_for(receiver, event_id)) == 1
