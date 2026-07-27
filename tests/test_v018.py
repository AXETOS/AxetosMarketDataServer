from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.domain import Candle, Tick
from axetos_market_data.streaming import LiveStreamHub, StreamFilter
from axetos_market_data.web import create_app


def test_version_and_stream_status(tmp_path):
    assert __version__ == "0.60.5"
    app = create_app(tmp_path / "data.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        response = client.get("/api/stream/status")
        assert response.status_code == 200
        assert response.json() == {
            "subscribers": 0,
            "published_events": 0,
            "dropped_events": 0,
            "last_sequence": 0,
            "queue_size": 1000,
        }


def test_stream_hub_filters_and_serializes_tick():
    async def scenario():
        hub = LiveStreamHub(queue_size=10)
        stream = hub.subscribe(StreamFilter(instruments=frozenset({"EUR/USD"}), event_types=frozenset({"tick"})))
        iterator = stream.__aiter__()
        pending = asyncio.create_task(iterator.__anext__())
        await asyncio.sleep(0)
        hub.publish_tick(Tick("ICMarkets.MT5", "GBP/USD", datetime.now(UTC), Decimal("1.2"), Decimal("1.3")))
        hub.publish_tick(Tick("ICMarkets.MT5", "EUR/USD", datetime.now(UTC), Decimal("1.1"), Decimal("1.2")))
        event = await asyncio.wait_for(pending, timeout=1)
        assert event["type"] == "tick"
        assert event["instrument"] == "EUR/USD"
        assert event["sequence"] == 2
        payload = hub.sse(event)
        assert "event: tick" in payload
        assert '"instrument":"EUR/USD"' in payload
        await stream.aclose()
        assert hub.status()["subscribers"] == 0

    asyncio.run(scenario())


def test_store_publishes_ticks_and_candles(tmp_path):
    app = create_app(tmp_path / "data.sqlite", tmp_path / "providers.json")
    store = app.state.store
    hub = app.state.stream_hub
    tick = Tick("Mock", "EUR/USD", datetime(2026, 7, 24, 20, 0, tzinfo=UTC), Decimal("1.10"), Decimal("1.12"))
    candle = Candle("Mock", "EUR/USD", "1m", datetime(2026, 7, 24, 20, 0, tzinfo=UTC), Decimal("1.11"), Decimal("1.12"), Decimal("1.10"), Decimal("1.11"), 1)
    store.insert_ticks([tick])
    store.upsert_candle(candle)
    status = hub.status()
    assert status["published_events"] == 2
    assert status["last_sequence"] == 2
