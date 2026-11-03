"""API database pool.

psycopg connections are not thread-safe and FastAPI runs sync handlers in a
thread pool, so each request borrows a connection instead of sharing one.
The pool also reconnects after a DB restart.
"""
import logging
import os

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

_pool: ConnectionPool | None = None

_DEFAULT_POOL_MAX = 10


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is required")
    return url


def _pool_max_size() -> int:
    raw = os.environ.get("DB_POOL_MAX", str(_DEFAULT_POOL_MAX))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError("DB_POOL_MAX must be an integer") from exc
    if value < 1:
        raise RuntimeError("DB_POOL_MAX must be at least 1")
    return value


def init_pool() -> ConnectionPool:
    """Create the pool without blocking startup.

    If Postgres is still down, let the process start and let /health report
    it. Crash-looping here makes the failure harder to see.
    """
    global _pool
    if _pool is not None:
        return _pool

    _pool = ConnectionPool(
        _database_url(),
        min_size=1,
        max_size=_pool_max_size(),
        timeout=5.0,
        kwargs={"row_factory": dict_row},
        name="relayops-api",
        open=False,
    )
    _pool.open(wait=False)
    logger.info("database pool initialised")
    return _pool


def get_pool() -> ConnectionPool:
    if _pool is None:
        raise RuntimeError("database pool is not initialised")
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
        logger.info("database pool closed")