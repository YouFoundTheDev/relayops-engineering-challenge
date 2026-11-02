"""Small structured-logging helper shared by the HTTP request path.

The service emits one JSON object per line so that request ids and delivery
metadata can be queried without a logging stack. No payloads or secrets are
included in the records.
"""
import contextvars
import json
import logging
import os
from datetime import UTC, datetime

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)

_EXTRA_FIELDS = (
    "method",
    "path",
    "status_code",
    "duration_ms",
    "event_id",
    "worker_id",
    "attempt",
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
            "request_id": request_id_var.get(),
        }
        for field in _EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers[:] = [handler]
        uvicorn_logger.propagate = False