from datetime import UTC, datetime
from decimal import Decimal

from axetos_market_data.domain import Candle
from axetos_market_data.history_worker import HistoryIngestionProcess
from axetos_market_data.storage import MarketDataStore


def _candle(minute: int) -> Candle:
    return Candle(
        "ICMarkets.MT5", "SOL/USD", "1m", datetime(2026, 7, 27, 20, minute, tzinfo=UTC),
        Decimal("190"), Decimal("191"), Decimal("189"), Decimal("190.5"), 10, Decimal("10"), True,
    )


def test_dedicated_history_process_stores_only_missing_candles(tmp_path) -> None:
    path = tmp_path / "market.sqlite"
    store = MarketDataStore(path)
    store.initialize()
    store.insert_candles_missing([_candle(0)])
    worker = HistoryIngestionProcess(path)
    try:
        stored = worker.insert_missing([_candle(0), _candle(1)], timeout_seconds=10)
        assert stored == 1
        assert store.candle_count_range(
            "ICMarkets.MT5", "SOL/USD", "1m", _candle(0).open_time, _candle(1).open_time
        ) == 2
        status = worker.view()
        assert status["running"] is True
        assert status["completed_jobs"] == 1
    finally:
        worker.shutdown()


def test_history_process_is_wired_separately_from_live_bridge() -> None:
    source = open("src/axetos_market_data/web.py", encoding="utf-8").read()
    assert "HistoryIngestionProcess(store.database_target)" in source
    assert "history_process=history_process" in source
    assert '"history_process": history_process.view()' in source
