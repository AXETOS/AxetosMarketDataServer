from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.clock import server_now
from axetos_market_data.domain import Tick
from axetos_market_data.web import create_app


def test_server_builds_ephemeral_current_minute_for_chart(tmp_path) -> None:
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    now = server_now().replace(second=10, microsecond=0)
    bridge = app.state.bridge
    assert bridge._ingest_observation(
        "terminal", Tick("ICMarkets.MT5", "BTC/USD", now, Decimal("100"), Decimal("102"))
    )
    assert bridge._ingest_observation(
        "terminal", Tick("ICMarkets.MT5", "BTC/USD", now + timedelta(seconds=5), Decimal("104"), Decimal("106"))
    )

    with TestClient(app) as client:
        payload = client.get(
            "/api/candles",
            params={"instrument": "BTC/USD", "timeframe": "1m", "provider": "ICMarkets.MT5"},
        ).json()

    assert payload["count"] == 1
    candle = payload["candles"][0]
    assert candle["open"] == "101"
    assert candle["high"] == "105"
    assert candle["low"] == "101"
    assert candle["close"] == "105"
    assert candle["complete"] is False
    assert app.state.store.statistics()["ticks"] == 0
    assert app.state.store.statistics()["candles"] == 0


def test_new_minute_replaces_ephemeral_state_without_persistence(tmp_path) -> None:
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    bridge = app.state.bridge
    start = server_now().replace(second=10, microsecond=0)
    bridge._ingest_observation(
        "terminal", Tick("ICMarkets.MT5", "EUR/USD", start, Decimal("1.1000"), Decimal("1.1002"))
    )
    bridge._ingest_observation(
        "terminal", Tick("ICMarkets.MT5", "EUR/USD", start + timedelta(minutes=1), Decimal("1.1010"), Decimal("1.1012"))
    )
    temporary = bridge.temporary_minute("ICMarkets.MT5", "EUR/USD")
    assert temporary is not None
    assert temporary.open_time.minute == (start.minute + 1) % 60
    assert temporary.open == Decimal("1.1011")
    assert temporary.complete is False
    assert app.state.store.statistics()["candles"] == 0


def test_v0673_metadata() -> None:
    assert __version__ == "0.67.8"
    assert 'version = "0.67.8"' in Path("pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.67.8" in Path("README.md").read_text(encoding="utf-8")
