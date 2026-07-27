from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from axetos_market_data.domain import Candle
from axetos_market_data.full_history import FullHistoryBackfillManager
from axetos_market_data.storage import MarketDataStore


def test_tiered_history_probes_exact_range_before_downloading(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    now = datetime(2026, 7, 27, tzinfo=UTC)
    manager = FullHistoryBackfillManager(
        lambda p, i, t, start, end: store.candle_count_range(p, i, t, start, end),
        now_factory=lambda: now,
    )
    manager.start("ICMarkets.MT5", [("EURUSD", "EUR/USD")])
    probe = manager.next_request("ICMarkets.MT5")
    parts = probe.split("|")
    assert parts[:3] == ["AVAILABILITY", "EURUSD", "1m"]
    request_id = parts[-1]
    manager.availability_result(
        "ICMarkets.MT5", request_id,
        earliest=datetime.fromisoformat(parts[3]), latest=datetime.fromisoformat(parts[4]), count=1440,
    )
    download = manager.next_request("ICMarkets.MT5")
    assert download.split("|")[:3] == ["BACKFILL", "EURUSD", "1m"]


def test_matching_local_count_skips_download(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    now = datetime(2026, 7, 27, tzinfo=UTC)
    manager = FullHistoryBackfillManager(
        lambda _p, _i, _t, _s, _e: 1440,
        now_factory=lambda: now,
    )
    manager.start("P", [("EURUSD", "EUR/USD")])
    probe = manager.next_request("P")
    parts = probe.split("|")
    manager.availability_result("P", parts[-1], earliest=datetime.fromisoformat(parts[3]), latest=datetime.fromisoformat(parts[4]), count=1440)
    next_probe = manager.next_request("P")
    assert next_probe.startswith("AVAILABILITY|")
    status = manager.status("P")["jobs"][0]["instruments"][0]
    assert status["ranges_skipped_existing"] == 1


def test_insert_candles_missing_preserves_existing(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    when = datetime(2026, 7, 1, tzinfo=UTC)
    original = Candle("P", "EUR/USD", "1m", when, Decimal("1"), Decimal("2"), Decimal("1"), Decimal("2"), 1, None, True)
    replacement = Candle("P", "EUR/USD", "1m", when, Decimal("9"), Decimal("9"), Decimal("9"), Decimal("9"), 1, None, True)
    store.upsert_candle(original)
    assert store.insert_candles_missing([replacement]) == 0
    assert store.read_candles("EUR/USD", "1m", provider="P")[-1].close == Decimal("2")


def test_management_ui_uses_tiered_history_wording():
    source = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert "Configured / selected / failed" not in source
    assert "Backfill tiered history" in source
    assert "/api/full-history/" in source
