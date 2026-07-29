from datetime import UTC, datetime, timedelta
from pathlib import Path

from axetos_market_data.mt5_terminal_clock import Mt5TerminalClockCoordinator


def test_every_copyrates_command_requires_explicit_terminal_time() -> None:
    clock = Mt5TerminalClockCoordinator()
    original = "FETCH|SOLUSD|1m|2026-07-29T16:58:00+00:00|2026-07-29T16:59:00+00:00|live-1"
    time_command = clock.prepare("ICMarkets.MT5", "terminal-1", original)
    fields = time_command.split("|")
    assert fields[0] == "TIME"
    request_id = fields[5]
    received = datetime(2026, 7, 29, 16, 59, tzinfo=UTC)
    offset = clock.record_terminal_time(
        "ICMarkets.MT5", "terminal-1", request_id,
        datetime(2026, 7, 29, 19, 59, tzinfo=UTC), received_utc=received,
    )
    assert offset == timedelta(hours=3)
    shifted = clock.prepare("ICMarkets.MT5", "terminal-1", "")
    shifted_fields = shifted.split("|")
    assert shifted_fields[3] == "2026-07-29T19:58:00+00:00"
    assert shifted_fields[4] == "2026-07-29T19:59:00+00:00"


def test_returned_candle_time_is_translated_back_to_server_time() -> None:
    clock = Mt5TerminalClockCoordinator()
    original = "FETCH|SOLUSD|1m|2026-07-29T16:58:00+00:00|2026-07-29T16:59:00+00:00|live-2"
    time_request = clock.prepare("ICMarkets.MT5", "terminal-1", original).split("|")[5]
    clock.record_terminal_time(
        "ICMarkets.MT5", "terminal-1", time_request,
        datetime(2026, 7, 29, 19, 59, tzinfo=UTC),
        received_utc=datetime(2026, 7, 29, 16, 59, tzinfo=UTC),
    )
    _ = clock.prepare("ICMarkets.MT5", "terminal-1", "")
    normalized = clock.normalize_returned_timestamp(
        "live-2", datetime(2026, 7, 29, 19, 58, tzinfo=UTC)
    )
    assert normalized == datetime(2026, 7, 29, 16, 58, tzinfo=UTC)


def test_bridge_reports_time_trade_server_before_copyrates() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text()
    assert '#property version   "3.04"' in source
    assert 'action == "TIME"' in source
    assert "TimeTradeServer()" in source
    assert "ReportTerminalTime" in source
