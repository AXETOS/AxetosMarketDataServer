from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from axetos_market_data.domain import Candle
from axetos_market_data.history_worker import HistoryIngestionProcess
from axetos_market_data.storage import MarketDataStore


def candle(provider: str, instrument: str, when: datetime, value: str, *, high: str | None = None) -> Candle:
    price = Decimal(value)
    return Candle(
        provider=provider,
        instrument=instrument,
        timeframe="1m",
        open_time=when,
        open=price,
        high=Decimal(high) if high else price,
        low=price,
        close=price,
        tick_count=1,
        volume=Decimal("1"),
        complete=True,
    )


def test_replace_flatline_only_when_provider_candidate_is_better(tmp_path: Path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    now = datetime(2026, 7, 28, 10, 0, tzinfo=UTC)
    store.upsert_candle(candle("P", "EUR/USD", now, "1.10"))

    changed = store.insert_missing_or_replace_flatline([
        candle("P", "EUR/USD", now, "1.10", high="1.11")
    ])
    assert changed == 1
    saved = store.read_candles_range("EUR/USD", "1m", now, now + timedelta(minutes=1), "P")[0]
    assert saved.high == Decimal("1.11")

    # Equal/provider-confirmed flatline is retained without a write.
    assert store.insert_missing_or_replace_flatline([
        candle("P", "EUR/USD", now, "1.10")
    ]) == 0

    # Existing non-flat data is protected from a later candidate.
    assert store.insert_missing_or_replace_flatline([
        candle("P", "EUR/USD", now, "1.09", high="1.12")
    ]) == 0


def test_history_process_supports_flatline_replacement_mode(tmp_path: Path) -> None:
    target = tmp_path / "market.sqlite"
    store = MarketDataStore(target)
    store.initialize()
    now = datetime(2026, 7, 28, 10, 0, tzinfo=UTC)
    store.upsert_candle(candle("P", "SOL/USD", now, "73"))
    worker = HistoryIngestionProcess(target)
    try:
        assert worker.insert_missing([
            candle("P", "SOL/USD", now, "73", high="74")
        ], replace_flatline=True) == 1
    finally:
        worker.shutdown()
