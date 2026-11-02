import time
import uuid

import pytest
from helpers import received_for, wait_for_received, wait_for_status

pytestmark = pytest.mark.integration


def _submit(api, event_id, payload=None):
    return api.post(
        "/events",
        json={
            "event_id": event_id,
            "type": "invoice.created",
            "payload": {"amount": 250} if payload is None else payload,
        },
    )


def test_successful_delivery_is_signed_and_recorded(api, receiver):
    event_id = f"evt-{uuid.uuid4()}"
    _submit(api, event_id)

    entry = wait_for_received(receiver, event_id, 1)[0]
    assert entry["signature_valid"] is True
    assert entry["status_code"] == 200
    assert entry["body"] == {
        "event_id": event_id,
        "type": "invoice.created",
        "payload": {"amount": 250},
    }

    state = wait_for_status(api, event_id, "delivered")
    assert state["attempts"] == 1
    assert state["delivery_attempts"][0]["status_code"] == 200


def test_retry_on_503_then_success(api, receiver):
    receiver.post("/control", json={"fail_next": 1, "status_code": 503})
    event_id = f"evt-{uuid.uuid4()}"
    _submit(api, event_id)

    entries = wait_for_received(receiver, event_id, 2)
    assert [entry["status_code"] for entry in entries] == [503, 200]

    state = wait_for_status(api, event_id, "delivered")
    assert state["attempts"] == 2
    assert [a["status_code"] for a in state["delivery_attempts"]] == [503, 200]


def test_retry_on_429_then_success(api, receiver):
    receiver.post("/control", json={"fail_next": 1, "status_code": 429})
    event_id = f"evt-{uuid.uuid4()}"
    _submit(api, event_id)

    entries = wait_for_received(receiver, event_id, 2)
    assert [entry["status_code"] for entry in entries] == [429, 200]
    state = wait_for_status(api, event_id, "delivered")
    assert state["attempts"] == 2


def test_retry_after_delays_next_attempt(api, receiver):
    receiver.post(
        "/control", json={"fail_next": 1, "status_code": 503, "retry_after": 2}
    )
    event_id = f"evt-{uuid.uuid4()}"
    _submit(api, event_id)

    entries = wait_for_received(receiver, event_id, 2)
    assert [entry["status_code"] for entry in entries] == [503, 200]
    gap = entries[1]["arrived_at"] - entries[0]["arrived_at"]
    assert gap >= 1.8, f"Retry-After was not respected, gap={gap:.2f}s"


def test_permanent_4xx_is_not_retried(api, receiver):
    receiver.post("/control", json={"fail_next": 10, "status_code": 400})
    event_id = f"evt-{uuid.uuid4()}"
    _submit(api, event_id)

    state = wait_for_status(api, event_id, "failed")
    assert state["attempts"] == 1
    assert state["delivery_attempts"][0]["status_code"] == 400
    time.sleep(1.0)
    assert len(received_for(receiver, event_id)) == 1


def test_maximum_three_attempts(api, receiver):
    receiver.post("/control", json={"fail_next": 10, "status_code": 500})
    event_id = f"evt-{uuid.uuid4()}"
    _submit(api, event_id)

    state = wait_for_status(api, event_id, "failed", timeout=25)
    entries = wait_for_received(receiver, event_id, 3)
    assert len(entries) == 3
    assert state["attempts"] == 3
    assert [a["status_code"] for a in state["delivery_attempts"]] == [500, 500, 500]


def test_receiver_rejects_invalid_signature(receiver):
    response = receiver.post(
        "/webhook",
        content=b'{"event_id":"forged","type":"x","payload":{}}',
        headers={
            "Content-Type": "application/json",
            "X-RelayOps-Signature": "sha256=deadbeef",
        },
    )
    assert response.status_code == 401


def test_concurrent_workers_deliver_each_event_once(api, receiver):
    event_ids = [f"evt-{uuid.uuid4()}" for _ in range(8)]
    for event_id in event_ids:
        assert _submit(api, event_id).status_code == 202

    for event_id in event_ids:
        wait_for_status(api, event_id, "delivered")

    time.sleep(1.0)
    for event_id in event_ids:
        assert len(received_for(receiver, event_id)) == 1