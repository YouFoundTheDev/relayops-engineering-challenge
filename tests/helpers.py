import time


def get_event(api, event_id):
    return api.get(f"/events/{event_id}")


def wait_for_status(api, event_id, expected, timeout=20.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        response = api.get(f"/events/{event_id}")
        if response.status_code == 200:
            last = response.json()
            if last["status"] == expected:
                return last
        time.sleep(0.2)
    raise AssertionError(
        f"event {event_id} did not reach status {expected!r}; last={last!r}"
    )


def wait_for_received(receiver, event_id, count=1, timeout=20.0):
    deadline = time.time() + timeout
    entries = []
    while time.time() < deadline:
        entries = [
            item
            for item in receiver.get("/received").json()
            if item["event_id"] == event_id
        ]
        if len(entries) >= count:
            return entries
        time.sleep(0.2)
    raise AssertionError(
        f"expected {count} deliveries for {event_id}, got {len(entries)}: {entries!r}"
    )


def received_for(receiver, event_id):
    return [
        item for item in receiver.get("/received").json() if item["event_id"] == event_id
    ]


def parse_prometheus(text):
    values = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, _, value = line.rpartition(" ")
        if name.startswith("relayops_events{"):
            status = name.split('status="', 1)[1].split('"', 1)[0]
            values[f"events_{status}"] = int(value)
        elif name == "relayops_delivery_attempts_total":
            values["delivery_attempts_total"] = int(value)
    return values