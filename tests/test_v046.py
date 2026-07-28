from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.domain import Candle
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.web import create_app

ROOT = Path(__file__).resolve().parents[1]


def test_release_metadata():
    assert __version__ == "0.61.4"
    assert 'version = "0.61.4"' in (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.61.4" in (ROOT / "README.md").read_text(encoding="utf-8")


def test_candles_fall_back_when_routed_provider_has_no_history(tmp_path):
    db = tmp_path / "market.sqlite"
    store = MarketDataStore(f"sqlite:///{db}")
    store.initialize()
    at = datetime(2026, 7, 26, 12, 45, tzinfo=UTC)
    store.upsert_symbol_policy("Oanda.MT5", "BTCUSD", "BTC/USD", True, True, True, 1)
    store.upsert_symbol_policy("ICMarkets.MT5", "BTCUSD", "BTC/USD", True, True, True, 2)
    store.upsert_candle(Candle("ICMarkets.MT5", "BTC/USD", "1m", at, Decimal("64478"), Decimal("64498"), Decimal("64470"), Decimal("64486"), 4, None, True))

    with TestClient(create_app(f"sqlite:///{db}")) as client:
        response = client.get("/api/candles", params={"instrument": "BTC/USD", "timeframe": "1m"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["active_provider"] == "ICMarkets.MT5"
    assert payload["count"] == 1
    assert payload["candles"][0]["close"] == "64486"


def test_explicit_candle_provider_remains_strict(tmp_path):
    db = tmp_path / "market.sqlite"
    store = MarketDataStore(f"sqlite:///{db}")
    store.initialize()
    at = datetime(2026, 7, 26, 12, 45, tzinfo=UTC)
    store.upsert_candle(Candle("ICMarkets.MT5", "BTC/USD", "1m", at, Decimal("1"), Decimal("2"), Decimal("1"), Decimal("2"), 1, None, True))

    with TestClient(create_app(f"sqlite:///{db}")) as client:
        response = client.get("/api/candles", params={"instrument": "BTC/USD", "timeframe": "1m", "provider": "Oanda.MT5"})

    assert response.status_code == 200
    assert response.json()["active_provider"] == "Oanda.MT5"
    assert response.json()["count"] == 0


def test_quote_falls_back_when_routed_provider_has_no_quote(tmp_path):
    db = tmp_path / "market.sqlite"
    store = MarketDataStore(f"sqlite:///{db}")
    store.initialize()
    at = datetime(2026, 7, 26, 12, 45, tzinfo=UTC)
    store.upsert_symbol_policy("Oanda.MT5", "BTCUSD", "BTC/USD", True, True, True, 1)
    store.upsert_symbol_policy("ICMarkets.MT5", "BTCUSD", "BTC/USD", True, True, True, 2)
    store.upsert_bridge_quote("ICMarkets.MT5", "terminal", {
        "provider_symbol": "BTCUSD",
        "canonical_instrument": "BTC/USD",
        "time_utc": at,
        "bid": Decimal("64478"),
        "ask": Decimal("64493"),
        "last": None,
        "volume": None,
    }, at)

    with TestClient(create_app(f"sqlite:///{db}")) as client:
        response = client.get("/api/quotes/BTC/USD")

    assert response.status_code == 200
    payload = response.json()
    assert payload["provider"] == "ICMarkets.MT5"
    assert payload["bid"] == "64478"
    assert payload["ask"] == "64493"
