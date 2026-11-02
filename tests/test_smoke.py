"""End-to-end smoke tests. Require the stack: docker compose up --build"""
import time
import uuid

import pytest

pytestmark = pytest.mark.integration


def test_create_and_get_event(api):
    event_id = f"evt-{uuid.uuid4()}"
    resp = api.post(
        "/events",
        json={"event_id": event_id, "type": "invoice.created", "payload": {"amount": 250}},
    )
    assert resp.status_code == 202

    resp = api.get(f"/events/{event_id}")
    assert resp.status_code == 200
    assert resp.json()["event_id"] == event_id


def test_event_is_delivered(api, receiver):
    event_id = f"evt-{uuid.uuid4()}"
    api.post("/events", json={"event_id": event_id, "type": "invoice.created", "payload": {}})

    deadline = time.time() + 15
    while time.time() < deadline:
        ids = [r["event_id"] for r in receiver.get("/received").json()]
        if event_id in ids:
            break
        time.sleep(0.5)
    assert event_id in ids
    assert api.get(f"/events/{event_id}").json()["status"] == "delivered"
