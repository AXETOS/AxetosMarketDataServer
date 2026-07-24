from datetime import datetime, timezone
from decimal import Decimal

from axetos_market_data.candle_builder import CandleBuilder
from axetos_market_data.domain import Tick
from axetos_market_data.storage import MarketDataStore


def tick(second: int, bid: str, ask: str) -> Tick:
    return Tick(
        provider="test",
        instrument="EUR/USD",
        timestamp=datetime(2026, 1, 1, 12, 0, second, tzinfo=timezone.utc),
        bid=Decimal(bid),
        ask=Decimal(ask),
    )


def test_builds_and_finalizes_one_minute_candle(tmp_path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    builder = CandleBuilder(store)

    builder.ingest(tick(1, "1.1000", "1.1002"))
    builder.ingest(tick(20, "1.1004", "1.1006"))
    builder.ingest(tick(59, "1.0998", "1.1000"))
    builder.ingest(
        Tick(
            provider="test",
            instrument="EUR/USD",
            timestamp=datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc),
            bid=Decimal("1.1001"),
            ask=Decimal("1.1003"),
        )
    )

    candles = store.read_candles("EUR/USD", "1m", provider="test")
    assert len(candles) == 1
    candle = candles[0]
    assert candle.open == Decimal("1.1001")
    assert candle.high == Decimal("1.1005")
    assert candle.low == Decimal("1.0999")
    assert candle.close == Decimal("1.0999")
    assert candle.tick_count == 3
    assert candle.complete is True
