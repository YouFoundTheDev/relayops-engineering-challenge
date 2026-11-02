from app import main
from starlette.responses import Response


def test_health_returns_503_when_database_unavailable(monkeypatch):
    monkeypatch.setattr(main, "_database_healthy", lambda: False)

    response = Response()
    body = main.health(response)

    assert response.status_code == 503
    assert body == {"status": "unhealthy", "database": "unavailable"}


def test_health_returns_200_when_database_available(monkeypatch):
    monkeypatch.setattr(main, "_database_healthy", lambda: True)

    response = Response()
    body = main.health(response)

    assert response.status_code == 200
    assert body == {"status": "ok", "database": "ok"}


def test_metrics_returns_503_when_pool_is_unavailable():
    response = main.metrics()

    assert response.status_code == 503
    assert b"relayops_metrics_unavailable" in response.body
