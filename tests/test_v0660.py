from datetime import UTC, datetime
from pathlib import Path

from axetos_market_data.live_m1 import LiveM1CommandScheduler


def test_server_schedules_previous_two_completed_minutes() -> None:
    scheduler = LiveM1CommandScheduler()
    command = scheduler.next_command("ICMarkets.MT5", ["EURUSD"], datetime(2026, 7, 29, 10, 5, 30, tzinfo=UTC))
    parts = command.split("|")
    assert parts[:3] == ["FETCH", "EURUSD", "1m"]
    assert parts[3].startswith("2026-07-29T10:03:00")
    assert parts[4].startswith("2026-07-29T10:04:00")
    assert scheduler.accepts("ICMarkets.MT5", parts[5])
    assert scheduler.complete("ICMarkets.MT5", parts[5], True) == "STORED"
    assert scheduler.next_command("ICMarkets.MT5", ["EURUSD"], datetime(2026, 7, 29, 10, 5, 40, tzinfo=UTC)) == ""


def test_bridge_is_thin_server_command_executor() -> None:
    bridge = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "2.00"' in bridge
    assert "ExecuteServerCommand" in bridge
    assert "SendPreviousCompletedM1" not in bridge
    assert "g_last_m1_bar" not in bridge
    assert "InpCompletedM1DelaySeconds" not in bridge
    assert "command != \"BACKFILL\" && command != \"FETCH\"" in bridge
    assert len(bridge.splitlines()) < 900
