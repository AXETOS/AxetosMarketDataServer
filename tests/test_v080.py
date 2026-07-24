from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from axetos_market_data.domain import Candle, Tick
from axetos_market_data.housekeeping import HousekeepingService, RetentionPolicy
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.web import create_app


def test_retention_deletes_old_ticks_but_never_candles(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    now = datetime(2026, 7, 24, 12, tzinfo=UTC)
    old = now - timedelta(days=45)
    recent = now - timedelta(days=2)
    store.insert_ticks([
        Tick("mock", "EUR/USD", old, Decimal("1.1"), Decimal("1.2"), None),
        Tick("mock", "EUR/USD", recent, Decimal("1.2"), Decimal("1.3"), None),
    ])
    store.upsert_candle(Candle(
        "mock", "EUR/USD", "1m", old.replace(second=0, microsecond=0),
        Decimal("1.1"), Decimal("1.2"), Decimal("1.0"), Decimal("1.15"), 1, None, True,
    ))

    preview = HousekeepingService(store).preview(RetentionPolicy(30, 90), now)
    assert preview["would_delete"]["ticks"] == 1
    assert preview["candles_deleted"] == 0

    result = HousekeepingService(store).run(RetentionPolicy(30, 90), False, now)
    assert result["ticks_deleted"] == 1
    assert store.statistics()["ticks"] == 1
    assert store.statistics()["candles"] == 1
    assert result["integrity_status"] == "ok"


def test_database_lifecycle_endpoints(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        integrity = client.get("/api/database/integrity")
        assert integrity.status_code == 200
        assert integrity.json()["status"] == "ok"

        preview = client.post(
            "/api/database/retention/preview",
            json={"tick_days": 30, "operational_days": 90, "vacuum": False},
        )
        assert preview.status_code == 200
        assert preview.json()["candles_deleted"] == 0

        cleanup = client.post(
            "/api/database/retention/run",
            json={"tick_days": 30, "operational_days": 90, "vacuum": False},
        )
        assert cleanup.status_code == 200
        assert cleanup.json()["integrity_status"] == "ok"

        history = client.get("/api/database/retention/history")
        assert history.status_code == 200
        assert history.json()["count"] == 1
