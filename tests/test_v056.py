from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from axetos_market_data.aggregation import CandleAggregator
from axetos_market_data.domain import Candle
from axetos_market_data.history import AuthoritativeHistoryRebuildService
from axetos_market_data.storage import MarketDataStore


class _HistoryProvider:
    def __init__(self, candles: list[Candle]) -> None:
        self.candles = candles

    def fetch_candles(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> list[Candle]:
        assert timeframe == "1m"
        return self.candles


def _c(provider: str, instrument: str, t: datetime, o: str, h: str, l: str, c: str) -> Candle:
    return Candle(provider, instrument, "1m", t, Decimal(o), Decimal(h), Decimal(l), Decimal(c), 1, None, True)


def test_clean_rebuild_removes_old_provider_fragments_and_long_flatline(tmp_path: Path) -> None:
    store = MarketDataStore(str(tmp_path / "market.sqlite")); store.initialize()
    provider = "ICMarkets.MT5"; instrument = "BTC/USD"
    start = datetime(2026, 7, 26, 10, 0, tzinfo=timezone.utc)

    # Old disconnected fragments across multiple timeframes must be destroyed.
    store.upsert_candle(_c(provider, instrument, start - timedelta(days=2), "60000", "60100", "59900", "60050"))
    store.upsert_candle(Candle(provider, instrument, "15m", start - timedelta(days=2), Decimal("60000"), Decimal("60100"), Decimal("59900"), Decimal("60050"), 1, None, True))

    values = [_c(provider, instrument, start, "64000", "64010", "63990", "64000")]
    # Closed-market style flatline: every OHLC equals prior close.
    for minute in range(1, 121):
        t = start + timedelta(minutes=minute)
        values.append(_c(provider, instrument, t, "64000", "64000", "64000", "64000"))
    values.append(_c(provider, instrument, start + timedelta(minutes=121), "64100", "64120", "64090", "64110"))

    result = AuthoritativeHistoryRebuildService(store).run(
        _HistoryProvider(values), provider, "BTCUSD", instrument, start, start + timedelta(days=1)
    )
    minutes = store.read_candles(instrument, "1m", provider=provider, limit=1000)
    assert [x.open_time for x in minutes] == [start, start + timedelta(minutes=121)]
    assert result.discarded_flat_minutes == 120
    assert all(x.open_time >= start for x in store.read_candles(instrument, "15m", provider=provider, limit=1000))


def test_aggregate_replace_removes_orphaned_target_buckets(tmp_path: Path) -> None:
    store = MarketDataStore(str(tmp_path / "market.sqlite")); store.initialize()
    provider = "ICMarkets.MT5"; instrument = "EUR/USD"
    start = datetime(2026, 7, 26, 10, 0, tzinfo=timezone.utc)
    store.upsert_candle(Candle(provider, instrument, "15m", start - timedelta(days=1), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), 0, None, True))
    store.upsert_candle(_c(provider, instrument, start, "1.1", "1.2", "1.0", "1.15"))
    CandleAggregator(store).aggregate(instrument, "15m", provider, replace=True)
    values = store.read_candles(instrument, "15m", provider=provider, limit=100)
    assert len(values) == 1
    assert values[0].open_time == start
