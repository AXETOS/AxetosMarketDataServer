from datetime import UTC, datetime
from pathlib import Path

from axetos_market_data.full_history import FullHistoryBackfillManager


def test_full_history_allows_read_only_probe_under_live_pressure():
    manager = FullHistoryBackfillManager(
        lambda _p, _i, _t, _s, _e: 0,
        pressure_probe=lambda: False,
        now_factory=lambda: datetime(2026, 7, 27, tzinfo=UTC),
    )
    manager.start("ICMarkets.MT5", [("EURUSD", "EUR/USD")])
    assert manager.next_request("ICMarkets.MT5").startswith("AVAILABILITY|EURUSD|1m|")


def test_release_keeps_low_priority_history_writes_and_live_queue_reservation():
    bridge = Path("src/axetos_market_data/bridge.py").read_text(encoding="utf-8")
    web = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert "_insert_historical_low_priority" in bridge
    assert "while not self.can_run_background_write()" not in bridge
    assert "self._queue.put(request, timeout=2.0)" in bridge
    assert "full_history.set_pressure_probe(bridge.can_run_background_write)" in web
