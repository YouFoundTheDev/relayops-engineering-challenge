import pytest
from app.models import EventIn
from pydantic import ValidationError


def test_valid_event():
    event = EventIn(event_id="evt-001", type="invoice.created", payload={"amount": 250})
    assert event.event_id == "evt-001"


@pytest.mark.parametrize(
    "data",
    [
        {"type": "invoice.created", "payload": {}},
        {"event_id": "", "type": "invoice.created", "payload": {}},
        {"event_id": "evt-001", "type": "invoice.created", "payload": "not-an-object"},
    ],
)
def test_invalid_event(data):
    with pytest.raises(ValidationError):
        EventIn(**data)
