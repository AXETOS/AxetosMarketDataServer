from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from axetos_market_data.bridge import BridgeQuotesRequest, BridgeTick
from axetos_market_data.bridge import Mt5BridgeService
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.web import create_app


def test_live_bridge_clock_skew_uses_receipt_utc_for_candle(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    bridge = Mt5BridgeService(store)
    try:
        far_future = datetime.now(UTC) + timedelta(hours=3)
        accepted = bridge.quotes(BridgeQuotesRequest(
            providerKey="ICMarkets.MT5",
            terminalInstanceId="ic-terminal",
            quotes=[BridgeTick(providerSymbol="BTCUSD", canonicalInstrument="BTC/USD", timeUtc=far_future,
                               bid=Decimal("64000"), ask=Decimal("64010"))],
        ))
        assert accepted == 1
        candles = store.read_candles("BTC/USD", "1m", provider="ICMarkets.MT5")
        assert len(candles) == 1
        assert abs(candles[0].open_time - datetime.now(UTC)) < timedelta(minutes=2)
    finally:
        bridge.shutdown()


def test_query_quote_endpoint_supports_canonical_symbol_with_slash(tmp_path, monkeypatch):
    monkeypatch.setenv("AXETOS_MARKET_DATA_DB", str(tmp_path / "market.sqlite"))
    app = create_app()
    store = app.state.store
    now = datetime.now(UTC)
    store.upsert_bridge_quote("ICMarkets.MT5", "ic-terminal", {
        "provider_symbol": "BTCUSD",
        "canonical_instrument": "BTC/USD",
        "time_utc": now,
        "bid": Decimal("64000"),
        "ask": Decimal("64010"),
        "last": None,
        "volume": None,
    }, now)
    with TestClient(app) as client:
        response = client.get("/api/quote", params={"instrument": "BTC/USD"})
        assert response.status_code == 200
        assert response.json()["instrument"] == "BTC/USD"
