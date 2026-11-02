import json
import logging

from app.logging_config import JsonFormatter, request_id_var


def test_json_formatter_includes_request_id_and_extras():
    record = logging.LogRecord(
        name="relayops.api",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="http request",
        args=(),
        exc_info=None,
    )
    record.method = "POST"
    record.path = "/events"
    record.status_code = 202

    token = request_id_var.set("rid-test")
    try:
        payload = json.loads(JsonFormatter().format(record))
    finally:
        request_id_var.reset(token)

    assert payload["message"] == "http request"
    assert payload["request_id"] == "rid-test"
    assert payload["method"] == "POST"
    assert payload["path"] == "/events"
    assert payload["status_code"] == 202
