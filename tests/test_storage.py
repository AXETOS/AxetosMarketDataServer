from datetime import datetime, timezone
from decimal import Decimal

from axetos_market_data.domain import Candle
from axetos_market_data.storage import MarketDataStore


def test_upsert_replaces_existing_candle(tmp_path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    open_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    first = Candle("test", "EUR/USD", "1m", open_time, Decimal("1"), Decimal("2"), Decimal("1"), Decimal("2"), 2)
    second = Candle("test", "EUR/USD", "1m", open_time, Decimal("1"), Decimal("3"), Decimal("1"), Decimal("2.5"), 3)
    store.upsert_candle(first)
    store.upsert_candle(second)

    candles = store.read_candles("EUR/USD", "1m", provider="test")
    assert len(candles) == 1
    assert candles[0].high == Decimal("3")
    assert candles[0].tick_count == 3
