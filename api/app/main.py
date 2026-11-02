import logging
import re
import time
import uuid
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, PlainTextResponse
from psycopg.types.json import Jsonb

from .db import close_pool, get_pool, init_pool
from .logging_config import configure_logging, request_id_var
from .models import EventIn

logger = logging.getLogger(__name__)

STATUSES = ("pending", "processing", "delivered", "failed")

_EVENT_SELECT = """
    SELECT
        e.event_id,
        e.type,
        e.payload,
        e.status,
        e.attempts,
        e.created_at,
        e.updated_at,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'attempt', da.attempt,
                    'status_code', da.status_code,
                    'error', da.error,
                    'created_at', da.created_at
                )
                ORDER BY da.attempt, da.id
            ) FILTER (WHERE da.id IS NOT NULL),
            '[]'::jsonb
        ) AS delivery_attempts
    FROM events e
    LEFT JOIN delivery_attempts da ON da.event_pk = e.id
    WHERE e.event_id = %s
    GROUP BY e.id
"""


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging()
    init_pool()
    try:
        yield
    finally:
        close_pool()


app = FastAPI(title="RelayOps API", lifespan=lifespan)


def _database_healthy() -> bool:
    try:
        with get_pool().connection(timeout=2.0) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        logger.debug("database health check failed")
        return False


def _fetch_event(conn: psycopg.Connection, event_id: str) -> dict | None:
    return conn.execute(_EVENT_SELECT, (event_id,)).fetchone()


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    incoming = request.headers.get("x-request-id")
    request_id = re.sub(r"[\r\n]", "", incoming or "").strip()
    if not request_id:
        request_id = uuid.uuid4().hex
    request_id = request_id[:200]

    token = request_id_var.set(request_id)
    started = time.perf_counter()
    try:
        try:
            response = await call_next(request)
        except Exception:
            logger.exception(
                "unhandled request error",
                extra={"method": request.method, "path": request.url.path},
            )
            response = JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"detail": "internal server error"},
            )

        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "http request",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
            },
        )
        return response
    finally:
        request_id_var.reset(token)


@app.get("/health")
def health(response: Response):
    if not _database_healthy():
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unhealthy", "database": "unavailable"}
    return {"status": "ok", "database": "ok"}


@app.post("/events")
def create_event(event: EventIn, response: Response):
    try:
        with get_pool().connection() as conn:
            with conn.transaction():
                inserted = conn.execute(
                    """
                    INSERT INTO events (event_id, type, payload)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING event_id, status
                    """,
                    (event.event_id, event.type, Jsonb(event.payload)),
                ).fetchone()

                if inserted is not None:
                    logger.info("event accepted", extra={"event_id": event.event_id})
                    response.status_code = status.HTTP_202_ACCEPTED
                    return {
                        "event_id": inserted["event_id"],
                        "status": inserted["status"],
                    }

                existing = _fetch_event(conn, event.event_id)
    except psycopg.Error:
        logger.exception("database error while creating event")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="database unavailable",
        ) from None

    if existing is None:
        # The conflicting row disappeared between the INSERT and the SELECT
        # (for example a rolled-back concurrent transaction). The client can
        # safely retry instead of receiving a misleading success.
        logger.warning("event conflict disappeared", extra={"event_id": event.event_id})
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="event could not be resolved, retry",
        )

    if existing["type"] != event.type or existing["payload"] != event.payload:
        logger.info("event id conflict", extra={"event_id": event.event_id})
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="event_id already exists with different content",
        )

    logger.info("idempotent event submission", extra={"event_id": event.event_id})
    response.status_code = status.HTTP_200_OK
    return existing


@app.get("/events/{event_id}")
def get_event(event_id: str):
    try:
        with get_pool().connection() as conn:
            row = _fetch_event(conn, event_id)
    except psycopg.Error:
        logger.exception("database error while reading event")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="database unavailable",
        ) from None

    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="event not found")
    return row


@app.get("/metrics")
def metrics():
    try:
        with get_pool().connection(timeout=2.0) as conn:
            rows = conn.execute(
                "SELECT status, count(*) AS count FROM events GROUP BY status"
            ).fetchall()
            attempts_row = conn.execute(
                "SELECT count(*) AS count FROM delivery_attempts"
            ).fetchone()
    except Exception:
        logger.warning("metrics query failed")
        return PlainTextResponse(
            "relayops_metrics_unavailable 1\n",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    counts = {name: 0 for name in STATUSES}
    for row in rows:
        if row["status"] in counts:
            counts[row["status"]] = int(row["count"])

    lines = [
        "# HELP relayops_events Current number of events by status.",
        "# TYPE relayops_events gauge",
    ]
    for name in STATUSES:
        lines.append(f'relayops_events{{status="{name}"}} {counts[name]}')
    lines.extend(
        [
            "# HELP relayops_delivery_attempts_total Total webhook delivery attempts.",
            "# TYPE relayops_delivery_attempts_total counter",
            f"relayops_delivery_attempts_total {int(attempts_row['count'])}",
        ]
    )
    return PlainTextResponse(
        "\n".join(lines) + "\n",
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )