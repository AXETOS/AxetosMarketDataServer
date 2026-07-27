from datetime import UTC, datetime, timedelta
from pathlib import Path

from axetos_market_data.full_history import FullHistoryBackfillManager


def test_empty_m1_tier_skips_directly_to_hourly_tier() -> None:
    now = datetime(2026, 7, 27, 21, 0, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_args: 0, now_factory=lambda: now)
    manager.start("ICMarkets.MT5", [("SOLUSD", "SOL/USD")])

    monthly = manager.next_request("ICMarkets.MT5").split("|")
    assert monthly[:3] == ["AVAILABILITY", "SOLUSD", "1m"]
    assert datetime.fromisoformat(monthly[4]) - datetime.fromisoformat(monthly[3]) > timedelta(days=29)

    decision = manager.availability_result(
        "ICMarkets.MT5", monthly[5], earliest=None, latest=None, count=0
    )
    assert decision == "UNAVAILABLE|0|0"

    following = manager.next_request("ICMarkets.MT5").split("|")
    assert following[:3] == ["AVAILABILITY", "SOLUSD", "1h"]
    status = manager.status("ICMarkets.MT5")["jobs"][0]["instruments"][0]
    assert status["coarse_ranges_probed"] == 1
    assert status["fine_ranges_probed"] == 0


def test_m1_tier_with_missing_data_drills_into_daily_ranges() -> None:
    now = datetime(2026, 7, 27, 21, 0, tzinfo=UTC)

    def local_count(_provider: str, _instrument: str, _timeframe: str, start: datetime, end: datetime) -> int:
        return 100 if end - start > timedelta(days=2) else 10

    manager = FullHistoryBackfillManager(local_count, now_factory=lambda: now)
    manager.start("ICMarkets.MT5", [("SOLUSD", "SOL/USD")])
    monthly = manager.next_request("ICMarkets.MT5").split("|")

    decision = manager.availability_result(
        "ICMarkets.MT5",
        monthly[5],
        earliest=datetime.fromisoformat(monthly[3]),
        latest=datetime.fromisoformat(monthly[4]),
        count=1000,
    )
    assert decision == "DRILLDOWN|1000|100"

    daily = manager.next_request("ICMarkets.MT5").split("|")
    assert daily[:3] == ["AVAILABILITY", "SOLUSD", "1m"]
    assert datetime.fromisoformat(daily[4]) - datetime.fromisoformat(daily[3]) < timedelta(days=1)
    assert daily[3] == monthly[3]


def test_bridge_limits_history_sync_probe_to_three_attempts() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.25"' in source
    assert "g_pending_probe_attempts < 3" in source
    assert "attempt %d/3" in source
    assert "attempt %d/10" not in source
