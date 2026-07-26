from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from axetos_market_data.domain import Candle
from axetos_market_data.full_history import FullHistoryBackfillManager
from axetos_market_data.storage import MarketDataStore


def test_full_history_discovers_then_pages_older_without_overwrite(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    existing = Candle("ICMarkets.MT5", "EUR/USD", "1m", datetime(2026, 7, 1, tzinfo=UTC), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), 1, None, True)
    store.upsert_candle(existing)
    manager = FullHistoryBackfillManager(lambda p, i: store.earliest_candle_time(p, i), batch_days=3)
    manager.start("ICMarkets.MT5", [("EURUSD", "EUR/USD")])
    request = manager.next_request("ICMarkets.MT5")
    assert request.startswith("AVAILABILITY|EURUSD|1m|")
    request_id = request.split("|")[-1]
    manager.availability_result("ICMarkets.MT5", request_id, datetime(2020, 1, 1, tzinfo=UTC))
    batch = manager.next_request("ICMarkets.MT5")
    parts = batch.split("|")
    assert parts[0] == "BACKFILL"
    assert datetime.fromisoformat(parts[4]) < existing.open_time


def test_insert_candles_missing_preserves_existing(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    when = datetime(2026, 7, 1, tzinfo=UTC)
    original = Candle("P", "EUR/USD", "1m", when, Decimal("1"), Decimal("2"), Decimal("1"), Decimal("2"), 1, None, True)
    replacement = Candle("P", "EUR/USD", "1m", when, Decimal("9"), Decimal("9"), Decimal("9"), Decimal("9"), 1, None, True)
    store.upsert_candle(original)
    assert store.insert_candles_missing([replacement]) == 0
    loaded = store.read_candles("EUR/USD", "1m", provider="P")[-1]
    assert loaded.close == Decimal("2")


def test_management_ui_removes_duplicate_symbol_summary_and_adds_full_history():
    source = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert "Configured / selected / failed" not in source
    assert "Backfill full history" in source
    assert "/api/full-history/" in source

def test_bridge_is_server_controlled_and_reports_availability():
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.16"' in source
    assert "SERIES_SERVER_FIRSTDATE" in source
    assert "AVAILABILITY" in source
    assert "BACKFILL" in source
    assert "InpBackfillBarsM1" not in source
