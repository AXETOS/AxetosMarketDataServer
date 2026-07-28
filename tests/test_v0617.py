from datetime import UTC, datetime, timedelta
from decimal import Decimal

from axetos_market_data.domain import Candle
from axetos_market_data.storage import MarketDataStore


def test_off_minute_m1_rows_are_removed_without_touching_aligned_rows(tmp_path):
    store = MarketDataStore(str(tmp_path / "market.sqlite"))
    store.initialize()
    start = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
    aligned = Candle("ICMarkets.MT5", "EUR/USD", "1m", start, Decimal("1.1"), Decimal("1.2"), Decimal("1.0"), Decimal("1.15"), 1, Decimal("1"), True)
    malformed = Candle("ICMarkets.MT5", "EUR/USD", "1m", start + timedelta(seconds=14), Decimal("1.1"), Decimal("1.3"), Decimal("0.9"), Decimal("1.2"), 1, Decimal("1"), True)
    store.force_replace_candles([aligned, malformed])
    store.set_candle_provenance("ICMarkets.MT5", "EUR/USD", "1m", malformed.open_time, source_kind="live_observation", source_timeframe=None, quality_rank=200, coverage_complete=False, repair_run_id=None)

    removed = store.delete_off_minute_candles("ICMarkets.MT5", "EUR/USD", start, start + timedelta(hours=1))

    assert removed == 1
    rows = store.read_candles_range("EUR/USD", "1m", start - timedelta(minutes=1), start + timedelta(hours=1), provider="ICMarkets.MT5")
    assert [row.open_time for row in rows] == [start]
