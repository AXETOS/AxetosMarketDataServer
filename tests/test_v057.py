from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from axetos_market_data.domain import Candle
from axetos_market_data.full_history import FullHistoryBackfillManager
from axetos_market_data.storage import MarketDataStore


def _discover(manager, provider, now):
    for timeframe, days, count in (("1m", 30, 43000), ("1h", 700, 15000), ("1d", 2500, 2500)):
        parts = manager.next_request(provider).split("|")
        assert parts[:3] == ["DISCOVER", "EURUSD", timeframe]
        manager.availability_result(provider, parts[5], earliest=now - __import__("datetime").timedelta(days=days), latest=now, count=count)


def test_tiered_history_probes_exact_range_before_downloading(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    now = datetime(2026, 7, 27, tzinfo=UTC)
    manager = FullHistoryBackfillManager(
        lambda p, i, t, start, end: store.candle_count_range(p, i, t, start, end),
        now_factory=lambda: now,
    )
    manager.start("ICMarkets.MT5", [("EURUSD", "EUR/USD")])
    _discover(manager, "ICMarkets.MT5", now)
    probe = manager.next_request("ICMarkets.MT5").split("|")
    assert probe[:3] == ["AVAILABILITY", "EURUSD", "1m"]
    decision = manager.availability_result(
        "ICMarkets.MT5", probe[5], earliest=datetime.fromisoformat(probe[3]), latest=datetime.fromisoformat(probe[4]), count=1440,
    )
    assert decision.startswith("PLAN_SPLIT|1440|0|")
    assert manager.next_request("ICMarkets.MT5").split("|")[:3] == ["AVAILABILITY", "EURUSD", "1m"]


def test_matching_local_count_skips_download(tmp_path):
    now = datetime(2026, 7, 27, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_: 1440, now_factory=lambda: now)
    manager.start("P", [("EURUSD", "EUR/USD")])
    _discover(manager, "P", now)
    probe = manager.next_request("P").split("|")
    decision = manager.availability_result("P", probe[5], earliest=datetime.fromisoformat(probe[3]), latest=datetime.fromisoformat(probe[4]), count=1440)
    assert decision == "PLAN_COMPLETE|1440|1440"
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
