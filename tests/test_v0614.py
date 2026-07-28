from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from axetos_market_data.bridge import BridgeCandlesRequest
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


def test_authoritative_window_removes_stale_and_misaligned_minutes(tmp_path: Path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    start = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
    end = start + timedelta(minutes=2)
    stale = candle(start + timedelta(seconds=20), "9.0000")
    old_aligned = candle(start, "8.0000")
    store.upsert_candles([stale, old_aligned])

    official = [candle(start, "1.1000"), candle(start + timedelta(minutes=1), "1.1001")]
    assert store.replace_candle_window(official, start, end) == 2

    saved = store.read_candles_range("EUR/USD", "1m", start, end, "ICMarkets.MT5")
    assert [item.open_time for item in saved] == [start, start + timedelta(minutes=1)]
    assert saved[0].open == Decimal("1.1000")


def test_history_worker_force_replaces_each_returned_timestamp(tmp_path: Path) -> None:
    target = tmp_path / "market.sqlite"
    store = MarketDataStore(target)
    store.initialize()
    start = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
    end = start + timedelta(minutes=3)
    store.upsert_candles([candle(start + timedelta(seconds=30), "9.0"), candle(start, "8.0")])

    worker = HistoryIngestionProcess(target)
    try:
        assert worker.insert_missing(
            [candle(start, "1.1"), candle(start + timedelta(minutes=1), "1.2")],
            replace_all=True,
        ) == 2
        assert worker.insert_missing(
            [candle(start + timedelta(minutes=2), "1.3")], replace_all=True
        ) == 1
    finally:
        worker.shutdown()

    saved = store.read_candles_range("EUR/USD", "1m", start, end, "ICMarkets.MT5")
    assert [item.open_time for item in saved] == [
        start, start + timedelta(seconds=30), start + timedelta(minutes=1),
        start + timedelta(minutes=2)
    ]


def test_candle_request_accepts_chunk_coordinates() -> None:
    request = BridgeCandlesRequest.model_validate({
        "providerKey": "ICMarkets.MT5",
        "terminalInstanceId": "terminal",
        "providerSymbol": "EURUSD",
        "canonicalInstrument": "EUR/USD",
        "interval": "1m",
        "requestId": "request",
        "chunkIndex": 2,
        "chunkCount": 15,
        "candles": [],
    })
    assert request.chunk_index == 2
    assert request.chunk_count == 15
