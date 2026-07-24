from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.operational import OperationalEventService
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.web import create_app


def test_version_013():
    assert __version__ == "0.13.0"


def test_operational_event_persistence_filtering_and_pagination(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    events = OperationalEventService(store)
    events.record("info", "provider.start", "Started", provider="ICMarkets.MT5", details={"attempt": 1})
    events.record("error", "provider.failure", "Failed", provider="Oanda.MT5", instrument="EUR/USD", details={"error": "offline"})

    page = events.list(page=1, page_size=1)
    assert page["total"] == 2
    assert page["pages"] == 2
    assert len(page["items"]) == 1
    filtered = events.list(severity="error", provider="Oanda.MT5", search="offline")
    assert filtered["total"] == 1
    assert filtered["items"][0]["details"]["error"] == "offline"


def test_operational_events_api_and_retention(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    store = app.state.store
    events = app.state.events
    events.record("info", "test.old", "Old event", timestamp=datetime.now(UTC) - timedelta(days=100))
    events.record("warning", "test.current", "Current event")
    with TestClient(app) as client:
        response = client.get("/api/operational-events", params={"severity": "warning", "page_size": 10})
        assert response.status_code == 200
        assert response.json()["total"] == 1
        cleanup = client.post("/api/database/retention/run", json={"tick_days": 30, "operational_days": 90, "vacuum": False})
        assert cleanup.status_code == 200
        assert cleanup.json()["operational_events_deleted"] >= 1
        assert client.get("/api/operational-events", params={"category": "test.old"}).json()["total"] == 0
