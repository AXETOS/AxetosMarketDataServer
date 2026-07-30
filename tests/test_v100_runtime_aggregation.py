from datetime import timedelta
from decimal import Decimal

from axetos_market_data.aggregation import CandleAggregator
from axetos_market_data.bridge import Mt5BridgeService
from axetos_market_data.clock import server_now
from axetos_market_data.domain import Candle, Tick
from axetos_market_data.storage import MarketDataStore


def _store(tmp_path):
    store = MarketDataStore(f"sqlite:///{tmp_path / 'market.sqlite'}")
    store.initialize()
    return store


def _minute(provider: str, instrument: str, at, price: int) -> Candle:
    value = Decimal(price)
    return Candle(
        provider=provider,
        instrument=instrument,
        timeframe="1m",
        open_time=at,
        open=value,
        high=value + Decimal("1"),
        low=value - Decimal("1"),
        close=value,
        tick_count=1,
        volume=Decimal("1"),
        complete=True,
    )


def test_temporary_15m_uses_fourteen_stored_minutes_plus_current_bid(tmp_path) -> None:
    provider = "ICMarkets.MT5"
    instrument = "EUR/USD"
    store = _store(tmp_path)
    start = server_now().replace(minute=0, second=0, microsecond=0)
    store.upsert_candles(_minute(provider, instrument, start + timedelta(minutes=i), 100 + i) for i in range(14))
    bridge = Mt5BridgeService(store)
    try:
        bridge._ingest_observation(
            "terminal",
            Tick(provider, instrument, start + timedelta(minutes=14, seconds=30), Decimal("120"), Decimal("122")),
        )
        candle = bridge.temporary_candle(provider, instrument, "15m")
        assert candle is not None
        assert candle.open_time == start
        assert candle.open == Decimal("100")
        assert candle.high == Decimal("120")
        assert candle.low == Decimal("99")
        assert candle.close == Decimal("120")
        assert candle.complete is False
        assert store.read_candles(instrument, "15m", provider=provider) == []
    finally:
        bridge.shutdown()


def test_closed_15m_and_hour_are_persisted_from_official_m1(tmp_path) -> None:
    provider = "ICMarkets.MT5"
    instrument = "EUR/USD"
    store = _store(tmp_path)
    start = server_now().replace(minute=0, second=0, microsecond=0)
    store.upsert_candles(_minute(provider, instrument, start + timedelta(minutes=i), 100 + i) for i in range(60))
    aggregator = CandleAggregator(store)

    finalized = aggregator.finalize_closed_buckets(
        instrument,
        provider,
        "1m",
        [start + timedelta(minutes=14), start + timedelta(minutes=59)],
        start + timedelta(hours=1),
    )

    assert [item.timeframe for item in finalized].count("15m") == 2
    assert [item.timeframe for item in finalized].count("1h") == 1
    quarter = store.read_candles(instrument, "15m", provider=provider)
    hour = store.read_candles(instrument, "1h", provider=provider)
    assert quarter[0].open_time == start
    assert quarter[0].open == Decimal("100")
    assert quarter[0].close == Decimal("114")
    assert quarter[0].complete is True
    assert hour[0].open_time == start
    assert hour[0].open == Decimal("100")
    assert hour[0].close == Decimal("159")
    assert hour[0].complete is True


def test_temporary_hour_uses_fifty_nine_stored_minutes_plus_current_bid(tmp_path) -> None:
    provider = "ICMarkets.MT5"
    instrument = "BTC/USD"
    store = _store(tmp_path)
    start = server_now().replace(minute=0, second=0, microsecond=0)
    store.upsert_candles(_minute(provider, instrument, start + timedelta(minutes=i), 100 + i) for i in range(59))
    bridge = Mt5BridgeService(store)
    try:
        bridge._ingest_observation(
            "terminal",
            Tick(provider, instrument, start + timedelta(minutes=59, seconds=30), Decimal("200"), Decimal("212")),
        )
        candle = bridge.temporary_candle(provider, instrument, "1h")
        assert candle is not None
        assert candle.open_time == start
        assert candle.open == Decimal("100")
        assert candle.high == Decimal("200")
        assert candle.low == Decimal("99")
        assert candle.close == Decimal("200")
        assert candle.complete is False
        assert store.read_candles(instrument, "1h", provider=provider) == []
    finally:
        bridge.shutdown()


def test_daily_weekly_and_monthly_boundaries_are_finalized_automatically(tmp_path) -> None:
    provider = "ICMarkets.MT5"
    instrument = "EUR/USD"
    store = _store(tmp_path)
    # Use server-local calendar boundaries so bucket alignment matches production.
    month_start = server_now().replace(year=2026, month=7, day=1, hour=0, minute=0, second=0, microsecond=0)
    day = month_start + timedelta(days=5)
    store.upsert_candles([
        _minute(provider, instrument, day, 100),
        _minute(provider, instrument, day.replace(hour=23, minute=59), 110),
    ])
    aggregator = CandleAggregator(store)
    finalized = aggregator.finalize_closed_buckets(
        instrument, provider, "1m", [day.replace(hour=23, minute=59)], day + timedelta(days=1),
    )
    daily = [item for item in finalized if item.timeframe == "1d"]
    assert len(daily) == 1
    assert daily[0].open_time == day
    assert daily[0].open == Decimal("100")
    assert daily[0].close == Decimal("110")

    # Populate the rest of the week/month with completed daily source rows, then
    # finalize the closed calendar buckets from D1 exactly once at the boundary.
    for offset in range(1, 7):
        value = Decimal(110 + offset)
        store.upsert_candle(Candle(
            provider, instrument, "1d", day + timedelta(days=offset),
            value, value + 1, value - 1, value, 1, Decimal("1"), True,
        ))
    weekly_monthly = aggregator.finalize_closed_buckets(
        instrument, provider, "1d", [day + timedelta(days=6)], month_start + timedelta(days=32),
    )
    assert any(item.timeframe == "1w" for item in weekly_monthly)
    assert any(item.timeframe == "1mo" for item in weekly_monthly)
    assert store.read_candles(instrument, "1w", provider=provider)
    assert store.read_candles(instrument, "1mo", provider=provider)
