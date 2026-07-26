from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from axetos_market_data.config import ConfigurationStore, ProviderConfig
from axetos_market_data.domain import Candle
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.web import create_app


def _client(tmp_path):
    db = tmp_path / "market.sqlite"
    cfg = tmp_path / "providers.json"
    ConfigurationStore(cfg).write_all([
        ProviderConfig(
            provider_key="ICMarkets.MT5",
            display_name="ICMarkets",
            kind="mt5",
            enabled=True,
            auto_start=False,
            symbols=["BTCUSD"],
            symbol_aliases={"BTCUSD": "BTC/USD"},
            priority=10,
        )
    ])
    app = create_app(db, cfg)
    return TestClient(app), MarketDataStore(db)


def test_quote_is_hidden_when_fresh_heartbeat_only_repeats_old_close(tmp_path):
    client, store = _client(tmp_path)
    now = datetime.now(UTC)
    old = Candle("ICMarkets.MT5", "BTC/USD", "1m", now - timedelta(hours=2),
                 Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), 1, None, True)
    store.upsert_candle(old)
    store.upsert_bridge_quote("ICMarkets.MT5", "terminal", {
        "provider_symbol": "BTCUSD", "canonical_instrument": "BTC/USD",
        "time_utc": now, "bid": Decimal("99"), "ask": Decimal("101"),
        "last": None, "volume": None,
    }, now)
    response = client.get("/api/quote", params={"instrument": "BTC/USD", "provider": "ICMarkets.MT5"})
    assert response.status_code == 404


def test_quote_is_available_when_price_moves_after_long_gap(tmp_path):
    client, store = _client(tmp_path)
    now = datetime.now(UTC)
    old = Candle("ICMarkets.MT5", "BTC/USD", "1m", now - timedelta(hours=2),
                 Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), 1, None, True)
    store.upsert_candle(old)
    store.upsert_bridge_quote("ICMarkets.MT5", "terminal", {
        "provider_symbol": "BTCUSD", "canonical_instrument": "BTC/USD",
        "time_utc": now, "bid": Decimal("104"), "ask": Decimal("106"),
        "last": None, "volume": None,
    }, now)
    response = client.get("/api/quote", params={"instrument": "BTC/USD", "provider": "ICMarkets.MT5"})
    assert response.status_code == 200
    assert response.json()["bid"] == "104"
