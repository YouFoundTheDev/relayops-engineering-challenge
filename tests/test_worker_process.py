import hashlib
import hmac
import http.server
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parents[1]
SECRET = "process-test-secret"


class _Receiver(http.server.BaseHTTPRequestHandler):
    received: list[tuple[bytes, bool]] = []

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler API
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        expected = "sha256=" + hmac.new(
            SECRET.encode(), raw, hashlib.sha256
        ).hexdigest()
        signature = self.headers.get("X-RelayOps-Signature", "")
        valid = hmac.compare_digest(signature, expected)
        type(self).received.append((raw, valid))
        self.send_response(200 if valid else 401)
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_receiver():
    _Receiver.received = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_worker_process_recovers_stale_event_and_shuts_down(
    clean_database, fake_receiver
):
    with psycopg.connect(clean_database, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO events
                (event_id, type, payload, status, attempts, locked_at)
            VALUES
                ('evt-process', 'invoice.created', '{}'::jsonb, 'processing', 1,
                 now() - interval '120 seconds')
            """
        )

    env = os.environ.copy()
    env.update(
        {
            "DATABASE_URL": clean_database,
            "RECEIVER_URL": f"http://127.0.0.1:{fake_receiver}/webhook",
            "WEBHOOK_SECRET": SECRET,
            "POLL_INTERVAL": "0.1",
            "PROCESSING_LEASE_SECONDS": "60",
            "LOG_LEVEL": "WARNING",
        }
    )
    process = subprocess.Popen(
        [sys.executable, str(ROOT / "worker" / "worker.py")],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        deadline = time.time() + 15
        event_status = None
        while time.time() < deadline:
            with psycopg.connect(clean_database) as conn:
                row = conn.execute(
                    "SELECT status FROM events WHERE event_id = 'evt-process'"
                ).fetchone()
            event_status = row[0] if row else None
            if event_status == "delivered":
                break
            time.sleep(0.1)

        if event_status != "delivered":
            process.terminate()
            output, _ = process.communicate(timeout=5)
            pytest.fail(f"worker did not recover the stale event: {output}")

        assert len(_Receiver.received) == 1
        raw, signature_valid = _Receiver.received[0]
        assert signature_valid is True
        assert b'"event_id":"evt-process"' in raw

        process.send_signal(signal.SIGTERM)
        output, _ = process.communicate(timeout=10)
        assert process.returncode == 0, output
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
