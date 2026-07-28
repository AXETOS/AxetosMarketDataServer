from datetime import UTC, datetime
from pathlib import Path

from axetos_market_data.full_history import FullHistoryBackfillManager


def _manager():
    return FullHistoryBackfillManager(lambda _p, _i, _t, _s, _e: 0, now_factory=lambda: datetime(2026, 7, 27, tzinfo=UTC))


def test_zero_availability_advances_without_download():
    manager = _manager()
    manager.start("P", [("SOLUSD", "SOL/USD")])
    first = manager.next_request("P")
    manager.availability_result("P", first.split("|")[-1], earliest=None, latest=None, count=0)
    second = manager.next_request("P")
    assert second.startswith("DISCOVER|") and second != first
    status = manager.status("P")["jobs"][0]["instruments"][0]
    assert status["ranges_unavailable"] == 1


def test_transient_download_failure_is_bounded():
    manager = _manager()
    manager.start("P", [("EURUSD", "EUR/USD")])
    for _ in range(3):
        probe = manager.next_request("P")
        parts = probe.split("|")
        manager.availability_result("P", parts[-1], earliest=datetime.fromisoformat(parts[3]), latest=datetime.fromisoformat(parts[4]), count=1)
        request = manager.next_request("P")
        manager.batch_result("P", request.split("|")[-1], 0, 0, False, error_code=4066)
    status = manager.status("P")["jobs"][0]["instruments"][0]
    assert status["ranges_unavailable"] == 1
    assert status["retry_count"] == 0


def test_bridge_probes_exact_symbol_timeframe_range():
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.30"' in source
    assert "ProbeHistoryRange" in source
    assert "candleCount=" in source
    assert "latestUtc=" in source
    assert "IntervalTimeframe" in source
