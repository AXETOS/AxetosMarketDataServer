from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data.bridge import BridgeCandle, BridgeCandlesRequest, BridgeTick, BridgeTicksRequest, Mt5BridgeService
from axetos_market_data.config import ConfigurationStore
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.web import create_app


def _tick(second: int, bid: str = "64000", ask: str = "64002") -> BridgeTick:
    return BridgeTick(
        provider_symbol="BTCUSD",
        canonical_instrument="BTC/USD",
        time_utc=datetime(2026, 7, 26, 15, 0, second, tzinfo=timezone.utc),
        bid=Decimal(bid), ask=Decimal(ask),
    )


def test_tick_ingestion_updates_latest_quote_without_building_candles(tmp_path: Path) -> None:
    store = MarketDataStore(str(tmp_path / "market.sqlite")); store.initialize()
    store.upsert_symbol_policy("ICMarkets.MT5", "BTCUSD", "BTC/USD")
    bridge = Mt5BridgeService(store)
    try:
        assert bridge.enqueue_ticks(BridgeTicksRequest(
            provider_key="ICMarkets.MT5", terminal_instance_id="ic-1", ticks=[_tick(1)]
        )) == 1
        bridge._queue.join()
        quote = store.latest_bridge_quote("BTC/USD", "ICMarkets.MT5")
        assert quote is not None
        assert quote["bid"] == "64000"
        candles = store.read_candles("BTC/USD", "1m", provider="ICMarkets.MT5")
        assert candles == []
    finally:
        bridge.shutdown()


def test_live_quote_endpoint_uses_freshest_provider_tick(tmp_path: Path) -> None:
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    older = datetime.now().astimezone() - timedelta(minutes=5)
    newer = datetime.now().astimezone()
    with TestClient(app) as client:
        for provider, terminal, observed, bid, ask in [
            ("Oanda.MT5", "oa", older, "63000", "63002"),
            ("ICMarkets.MT5", "ic", newer, "64000", "64002"),
        ]:
            response = client.post('/api/market-data/ingest/mt5/quotes', json={
                'providerKey': provider, 'terminalInstanceId': terminal, 'quotes': [{
                    'providerSymbol':'BTCUSD', 'canonicalInstrument':'BTC/USD',
                    'timeUtc':observed.isoformat(), 'receivedUtc':observed.isoformat(),
                    'bid':bid, 'ask':ask
                }]
            })
            assert response.status_code == 200
        response=client.get('/api/quote',params={'instrument':'BTC/USD'})
        assert response.status_code == 200
        assert response.json()['provider'] == 'ICMarkets.MT5'
        assert response.json()['bid'] == '64000'


def test_historical_backfill_discards_long_and_trailing_identical_flat_runs(tmp_path: Path) -> None:
    store = MarketDataStore(str(tmp_path / "market.sqlite")); store.initialize()
    store.upsert_symbol_policy("ICMarkets.MT5", "BTCUSD", "BTC/USD")
    bridge = Mt5BridgeService(store)
    start=datetime(2026,7,24,20,0,tzinfo=timezone.utc)
    values=[BridgeCandle(time_utc=start,open=Decimal('64000'),high=Decimal('64010'),low=Decimal('63990'),close=Decimal('64000'))]
    for minute in range(1, 121):
        values.append(BridgeCandle(time_utc=start+timedelta(minutes=minute),open=Decimal('64000'),high=Decimal('64010'),low=Decimal('63990'),close=Decimal('64000')))
    values.append(BridgeCandle(time_utc=start+timedelta(minutes=121),open=Decimal('64100'),high=Decimal('64110'),low=Decimal('64090'),close=Decimal('64105')))
    values.append(BridgeCandle(time_utc=start+timedelta(minutes=122),open=Decimal('64100'),high=Decimal('64110'),low=Decimal('64090'),close=Decimal('64105')))
    try:
        bridge.candles(BridgeCandlesRequest(provider_key='ICMarkets.MT5',terminal_instance_id='ic',provider_symbol='BTCUSD',canonical_instrument='BTC/USD',interval='1m',candles=values))
        candles=store.read_candles('BTC/USD','1m',limit=500,provider='ICMarkets.MT5')
        assert [c.open_time for c in candles] == [start.astimezone(), (start+timedelta(minutes=121)).astimezone()]
    finally:
        bridge.shutdown()
