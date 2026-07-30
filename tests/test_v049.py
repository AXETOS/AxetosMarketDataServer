from datetime import datetime, timedelta, timezone
from decimal import Decimal

from axetos_market_data.aggregation import CandleAggregator
from axetos_market_data.clock import server_now
from axetos_market_data.candle_builder import CandleBuilder
from axetos_market_data.domain import Candle, Tick
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.timeframes import bucket_start


def test_server_local_calendar_buckets():
    value = datetime(2026, 7, 26, 14, 37, tzinfo=timezone(timedelta(hours=3)))
    assert bucket_start(value, "15m").minute == 30
    assert bucket_start(value, "1h").hour == value.astimezone().hour
    assert bucket_start(value, "1w").weekday() == 0
    assert bucket_start(value, "1mo").day == 1


def test_connected_minute_uses_previous_close_but_gap_detaches(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    builder = CandleBuilder(store)
    base = datetime.now().astimezone().replace(second=5, microsecond=0)
    builder.ingest(Tick("P", "EUR/USD", base, Decimal("1.1"), Decimal("1.2")))
    builder.ingest(Tick("P", "EUR/USD", base + timedelta(minutes=1), Decimal("1.3"), Decimal("1.4")))
    rows = store.read_candles("EUR/USD", "1m", provider="P")
    assert rows[-1].open == rows[-2].close
    builder.ingest(Tick("P", "EUR/USD", base + timedelta(minutes=3), Decimal("1.50"), Decimal("1.60")))
    rows = store.read_candles("EUR/USD", "1m", provider="P")
    assert rows[-1].open == rows[-2].close
    assert any(item.open_time == rows[-2].open_time + timedelta(minutes=1) for item in rows)


def test_week_and_month_are_built_from_daily_candles(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    at = server_now().replace(year=2026, month=7, day=6, hour=0, minute=0, second=0, microsecond=0)
    store.upsert_candle(Candle("P", "BTC/USD", "1d", at, Decimal("10"), Decimal("12"), Decimal("9"), Decimal("11"), 1))
    store.upsert_candle(Candle("P", "BTC/USD", "1d", at + timedelta(days=1), Decimal("11"), Decimal("14"), Decimal("10"), Decimal("13"), 1))
    agg = CandleAggregator(store)
    assert agg.aggregate("BTC/USD", "1w", "P") == 1
    assert agg.aggregate("BTC/USD", "1mo", "P") == 1
    assert store.read_candles("BTC/USD", "1w", provider="P")[0].close == Decimal("13")
