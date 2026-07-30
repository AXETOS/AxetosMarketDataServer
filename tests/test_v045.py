from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient
from axetos_market_data import __version__
from axetos_market_data.domain import Candle
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.web import create_app

ROOT = Path(__file__).resolve().parents[1]

def test_release_metadata():
    assert __version__ == "0.68.10"
    assert 'version = "0.68.10"' in (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.68.10" in (ROOT / "README.md").read_text(encoding="utf-8")

def test_compatibility_candles_returns_server_rows(tmp_path):
    db = tmp_path / "market.sqlite"
    store = MarketDataStore(f"sqlite:///{db}")
    store.initialize()
    at = datetime(2026, 7, 26, 10, 0, tzinfo=UTC)
    store.upsert_symbol_policy("ICMarkets.MT5", "EURUSD", "EUR/USD", True, True, True, 1)
    store.upsert_candle(Candle("ICMarkets.MT5", "EUR/USD", "1m", at, Decimal("1.1"), Decimal("1.2"), Decimal("1.0"), Decimal("1.15"), 2, None, True))
    with TestClient(create_app(f"sqlite:///{db}")) as client:
        response = client.get("/api/market-data/candles", params={"instrument":"EUR/USD","interval":"1m","from":(at-timedelta(minutes=1)).isoformat(),"to":(at+timedelta(minutes=1)).isoformat()})
    assert response.status_code == 200
    payload = response.json()
    assert payload["active_provider"] == "ICMarkets.MT5"
    assert payload["candles"][0]["time_utc"] == at.isoformat()
