from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from axetos_market_data.bridge import Mt5BridgeService
from axetos_market_data.domain import Candle
from axetos_market_data.history_worker import HistoryIngestionProcess
from axetos_market_data.storage import MarketDataStore


def _candle(when: datetime, open_value: str, close_value: str) -> Candle:
    open_price = Decimal(open_value)
    close_price = Decimal(close_value)
    return Candle(
        provider="ICMarkets.MT5",
        instrument="EUR/USD",
        timeframe="1m",
        open_time=when,
        open=open_price,
        high=max(open_price, close_price),
        low=min(open_price, close_price),
        close=close_price,
        tick_count=60,
        volume=Decimal("60"),
        complete=True,
    )


def test_mt5_live_service_does_not_persist_individual_ticks(tmp_path: Path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    bridge = Mt5BridgeService(store)
    try:
        assert bridge._service.persist_ticks is False
    finally:
        bridge.shutdown()


def test_history_worker_full_provider_replacement_overwrites_recent_m1(tmp_path: Path) -> None:
    target = tmp_path / "market.sqlite"
    store = MarketDataStore(target)
    store.initialize()
    when = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
    store.upsert_candle(_candle(when, "1.10", "1.10"))

    worker = HistoryIngestionProcess(target)
    try:
        changed = worker.insert_missing(
            [_candle(when, "1.11", "1.12")], replace_all=True
        )
        assert changed == 1
    finally:
        worker.shutdown()

    saved = store.read_candles_range(
        "EUR/USD", "1m", when, when.replace(minute=1), "ICMarkets.MT5"
    )[0]
    assert saved.open == Decimal("1.11")
    assert saved.close == Decimal("1.12")
    provenance = store.get_candle_provenance("ICMarkets.MT5", "EUR/USD", "1m", when)
    assert provenance is not None
    assert provenance["source_kind"] == "mt5_provider"
    assert int(provenance["quality_rank"]) == 700


def test_recent_repair_is_full_provider_window_not_shape_detection() -> None:
    source = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert 'return {instrument: [PlannedRange("1m", start, end, 4)] for instrument in instruments}' in source
    assert 'recent_m1_mode": "full_provider_replacement"' in source
    assert "candle.open == candle.high == candle.low == candle.close" not in source
