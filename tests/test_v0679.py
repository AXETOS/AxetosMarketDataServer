from datetime import datetime, timedelta, timezone
from axetos_market_data.mt5_command_clock import shift_copyrates_command, terminal_clock_offset


def test_server_references_copyrates_to_current_terminal_clock():
    now = datetime.now(timezone.utc)
    quote = {
        "source_time_utc": now + timedelta(hours=3),
        "received_utc": now,
    }
    command = "FETCH|ETHUSD|1m|2026-07-29T15:20:00+00:00|2026-07-29T15:21:00+00:00|abc"
    shifted = shift_copyrates_command(command, quote)
    assert shifted == "FETCH|ETHUSD|1m|2026-07-29T18:20:00+00:00|2026-07-29T18:21:00+00:00|abc"


def test_stale_terminal_clock_is_not_used():
    now = datetime.now(timezone.utc)
    quote = {"source_time_utc": now + timedelta(hours=3), "received_utc": now - timedelta(minutes=1)}
    assert terminal_clock_offset(quote) == timedelta(0)
