from datetime import UTC, datetime, timedelta
from decimal import Decimal

from axetos_market_data.bridge import BridgeHeartbeatRequest, BridgeQuotesRequest, BridgeTick, Mt5BridgeService
from axetos_market_data.config import ProviderConfig
from axetos_market_data.routing import ProviderAuthorityRegistry
from axetos_market_data.runtime import ProviderWorker
from axetos_market_data.storage import MarketDataStore


def _worker(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    config = ProviderConfig(provider_key="ICMarkets.MT5", display_name="IC", kind="mt5", symbols=["BTCUSD"])
    authority = ProviderAuthorityRegistry()
    authority.replace_configs([config])
    return store, ProviderWorker(config, store, authority)


def test_stale_persisted_heartbeat_is_not_treated_as_live(tmp_path):
    store, worker = _worker(tmp_path)
    stale = {"received_utc": (datetime.now(UTC) - timedelta(minutes=5)).isoformat()}
    assert worker._heartbeat_is_fresh(stale) is False
    assert worker._heartbeat_is_fresh({"received_utc": datetime.now(UTC).isoformat()}) is True


def test_bridge_updates_provider_runtime_and_feed(tmp_path):
    store, worker = _worker(tmp_path)
    bridge = Mt5BridgeService(
        store,
        heartbeat_sink=lambda provider, value: worker.record_bridge_heartbeat(value),
        observation_sink=worker.record_bridge_observation,
    )
    try:
        now = datetime.now(UTC)
        bridge.heartbeat(BridgeHeartbeatRequest(
            provider_key="ICMarkets.MT5", terminal_instance_id="ic", broker_name="IC",
            server_name="IC-Demo", account_login=1, time_utc=now,
        ))
        bridge.quotes(BridgeQuotesRequest(
            provider_key="ICMarkets.MT5", terminal_instance_id="ic",
            quotes=[BridgeTick(provider_symbol="BTCUSD", canonical_instrument="BTC/USD", time_utc=now,
                               bid=Decimal("64000"), ask=Decimal("64010"))],
        ))
        assert worker.runtime.status == "Live"
        assert worker.runtime.ticks_received == 1
        assert worker.runtime.accepted_market_ticks == 1
        last = datetime.fromisoformat(worker.runtime.last_tick_utc)
        assert last.tzinfo is not None
        assert abs((datetime.now().astimezone() - last).total_seconds()) < 5
        assert worker.feed.reports()[0]["instrument"] == "BTC/USD"
        assert store.read_candles("BTC/USD", "1m", provider="ICMarkets.MT5")
    finally:
        bridge.shutdown()
