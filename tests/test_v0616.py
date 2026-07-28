from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from axetos_market_data.domain import Candle
from axetos_market_data.history_worker import HistoryIngestionProcess
from axetos_market_data.storage import MarketDataStore


def candle(when: datetime, value: str) -> Candle:
    price = Decimal(value)
    return Candle(
        "ICMarkets.MT5", "EUR/USD", "1m", when,
        price, price + Decimal("0.0002"), price - Decimal("0.0002"), price,
        60, Decimal("60"), True,
    )


def test_recent_refresh_force_upserts_returned_timestamps_and_leaves_others(tmp_path: Path) -> None:
    target = tmp_path / "market.sqlite"
    store = MarketDataStore(target)
    store.initialize()
    start = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
    untouched = start + timedelta(minutes=1)
    store.upsert_candles([candle(start, "9.0000"), candle(untouched, "8.0000")])

    worker = HistoryIngestionProcess(target)
    try:
        assert worker.insert_missing([candle(start, "1.1000")], replace_all=True) == 1
    finally:
        worker.shutdown()

    saved = store.read_candles_range(
        "EUR/USD", "1m", start, untouched + timedelta(minutes=1), "ICMarkets.MT5"
    )
    assert len(saved) == 2
    assert saved[0].open == Decimal("1.1000")
    assert saved[1].open == Decimal("8.0000")
