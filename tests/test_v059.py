from pathlib import Path

from axetos_market_data.full_history import FullHistoryBackfillManager


def test_full_history_pauses_dispatch_under_live_pressure():
    allowed = False
    manager = FullHistoryBackfillManager(lambda _p, _i: None, pressure_probe=lambda: allowed)
    manager.start("ICMarkets.MT5", [("EURUSD", "EUR/USD")])
    assert manager.next_request("ICMarkets.MT5") == ""


def test_release_adds_low_priority_history_writes_and_live_queue_reservation():
    bridge = Path("src/axetos_market_data/bridge.py").read_text(encoding="utf-8")
    web = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert "_insert_historical_low_priority" in bridge
    assert "while not self.can_run_background_write()" in bridge
    assert "self._queue.put(request, timeout=2.0)" in bridge
    assert "full_history.set_pressure_probe(bridge.can_run_background_write)" in web
    assert "batch_days=1" in web
