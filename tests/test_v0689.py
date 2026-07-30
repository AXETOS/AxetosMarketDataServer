from datetime import UTC, datetime, timedelta
from decimal import Decimal

from axetos_market_data.domain import Candle
from axetos_market_data.storage import MarketDataStore


def _candle(ts: datetime, close: str) -> Candle:
    price = Decimal(close)
    return Candle(
        provider="ICMarkets.MT5",
        instrument="EUR/USD",
        timeframe="1m",
        open_time=ts,
        open=price,
        high=price,
        low=price,
        close=price,
        tick_count=1,
        volume=Decimal("1"),
        complete=True,
    )


def test_authoritative_window_tolerates_duplicate_timestamp_and_overwrites(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    start = datetime(2026, 7, 30, 9, 17, tzinfo=UTC)
    end = start + timedelta(minutes=9)
    store.force_replace_candles([_candle(start, "1.10")])

    values = [_candle(start + timedelta(minutes=i), f"1.{20+i:02d}") for i in range(10)]
    values.append(_candle(start + timedelta(minutes=5), "1.99"))

    written = store.replace_candle_window(values, start, end)

    assert written == 10
    rows = store.read_candles_range("EUR/USD", "1m", start, end + timedelta(minutes=1), "ICMarkets.MT5")
    assert len(rows) == 10
    assert rows[5].close == Decimal("1.99")
