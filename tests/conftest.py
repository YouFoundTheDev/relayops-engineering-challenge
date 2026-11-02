import os
import sys
from pathlib import Path

import httpx
import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "api"))
sys.path.insert(0, str(ROOT / "worker"))
sys.path.insert(0, str(ROOT / "tests"))

BASE_URL = os.environ.get("RELAYOPS_BASE_URL", "http://localhost:8080")
RECEIVER_URL = os.environ.get("RELAYOPS_RECEIVER_URL", "http://localhost:9000")

SCHEMA_PATH = ROOT / "db" / "001_schema.sql"


@pytest.fixture
def api():
    try:
        httpx.get(f"{BASE_URL}/health", timeout=2).raise_for_status()
    except httpx.HTTPError as exc:
        pytest.skip(f"stack not reachable at {BASE_URL}: {exc}")
    with httpx.Client(base_url=BASE_URL, timeout=10) as client:
        yield client


@pytest.fixture
def receiver():
    try:
        httpx.get(f"{RECEIVER_URL}/health", timeout=2).raise_for_status()
    except httpx.HTTPError as exc:
        pytest.skip(f"receiver not reachable at {RECEIVER_URL}: {exc}")
    with httpx.Client(base_url=RECEIVER_URL, timeout=5) as client:
        client.delete("/received")
        yield client


@pytest.fixture
def test_database_url():
    url = os.environ.get("RELAYOPS_TEST_DATABASE_URL")
    if not url:
        pytest.skip("RELAYOPS_TEST_DATABASE_URL is not configured")
    return url


@pytest.fixture
def clean_database(test_database_url):
    """Recreate the application schema so DB tests are isolated."""
    schema = SCHEMA_PATH.read_text()
    with psycopg.connect(test_database_url, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS delivery_attempts CASCADE")
        conn.execute("DROP TABLE IF EXISTS events CASCADE")
        conn.execute(schema)
    return test_database_url


@pytest.fixture
def worker_pool(clean_database):
    pool = ConnectionPool(
        clean_database,
        min_size=1,
        max_size=4,
        timeout=5.0,
        kwargs={"row_factory": dict_row},
        open=True,
    )
    try:
        yield pool
    finally:
        pool.close()