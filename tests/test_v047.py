from datetime import UTC, datetime, timedelta
from decimal import Decimal

from axetos_market_data.bridge import (
    BridgeHeartbeatRequest,
    BridgeQuotesRequest,
    BridgeTick,
    Mt5BridgeService,
)
from axetos_market_data.storage import MarketDataStore


def test_bridge_quote_builds_provider_scoped_candle(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    bridge = Mt5BridgeService(store)
    try:
        at = datetime(2026, 7, 26, 10, 15, 10, tzinfo=UTC)
        accepted = bridge.quotes(BridgeQuotesRequest(
            provider_key="ICMarkets.MT5",
            terminal_instance_id="ic-terminal",
            quotes=[BridgeTick(
                provider_symbol="BTCUSD",
                canonical_instrument="BTC/USD",
                time_utc=at,
                bid=Decimal("64478"),
                ask=Decimal("64494"),
            )],
        ))
        assert accepted == 1
        candles = store.read_candles("BTC/USD", "1m", provider="ICMarkets.MT5")
        assert len(candles) == 1
        assert candles[0].open_time == at.replace(second=0, microsecond=0)
        assert candles[0].complete is False
    finally:
        bridge.shutdown()


def test_bridge_providers_do_not_share_quotes_or_candles(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    bridge = Mt5BridgeService(store)
    try:
        at = datetime(2026, 7, 26, 10, 15, 10, tzinfo=UTC)
        bridge.quotes(BridgeQuotesRequest(
            provider_key="ICMarkets.MT5", terminal_instance_id="ic",
            quotes=[BridgeTick(provider_symbol="BTCUSD", canonical_instrument="BTC/USD", time_utc=at, bid=Decimal("64000"), ask=Decimal("64010"))],
        ))
        bridge.quotes(BridgeQuotesRequest(
            provider_key="Oanda.MT5", terminal_instance_id="oa",
            quotes=[BridgeTick(provider_symbol="BTCUSD", canonical_instrument="BTC/USD", time_utc=at + timedelta(seconds=1), bid=Decimal("65000"), ask=Decimal("65020"))],
        ))
        ic = store.read_candles("BTC/USD", "1m", provider="ICMarkets.MT5")
        oa = store.read_candles("BTC/USD", "1m", provider="Oanda.MT5")
        assert ic[0].close == Decimal("64005")
        assert oa[0].close == Decimal("65010")
        assert store.latest_bridge_quote("BTC/USD", "ICMarkets.MT5")["bid"] == "64000"
        assert store.latest_bridge_quote("BTC/USD", "Oanda.MT5")["bid"] == "65000"
    finally:
        bridge.shutdown()


def test_latest_bridge_heartbeat_is_provider_scoped(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    store.upsert_bridge_heartbeat(BridgeHeartbeatRequest(
        provider_key="ICMarkets.MT5", terminal_instance_id="ic", broker_name="IC",
        server_name="IC-Demo", account_login=1, time_utc=datetime.now(UTC),
    ).model_dump(by_alias=False))
    store.upsert_bridge_heartbeat(BridgeHeartbeatRequest(
        provider_key="Oanda.MT5", terminal_instance_id="oa", broker_name="Oanda",
        server_name="Oanda-Demo", account_login=2, time_utc=datetime.now(UTC),
    ).model_dump(by_alias=False))
    assert store.latest_bridge_heartbeat("ICMarkets.MT5")["terminal_instance_id"] == "ic"
    assert store.latest_bridge_heartbeat("Oanda.MT5")["terminal_instance_id"] == "oa"
