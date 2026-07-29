from __future__ import annotations

from datetime import datetime, timedelta


def _parse(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def terminal_clock_offset(quote: dict[str, object] | None, *, max_age_seconds: float = 15.0) -> timedelta:
    """Return the MT5 terminal-clock minus server-clock offset.

    The bridge already posts a fresh terminal tick timestamp every second.  The
    server reads that immediately before issuing CopyRates and compares it with
    the server receive timestamp.  Offsets are rounded to whole minutes to
    remove network latency and are rejected when the observation is stale.
    """
    if not quote:
        return timedelta(0)
    source = _parse(quote["source_time_utc"])
    received = _parse(quote["received_utc"])
    age = (datetime.now(received.tzinfo) - received).total_seconds()
    if age < -5 or age > max_age_seconds:
        return timedelta(0)
    seconds = (source - received).total_seconds()
    if abs(seconds) > 14 * 3600:
        return timedelta(0)
    return timedelta(minutes=round(seconds / 60.0))


def shift_copyrates_command(command: str, quote: dict[str, object] | None) -> str:
    """Translate a server-time CopyRates command into the terminal clock."""
    if not command:
        return command
    fields = command.split("|")
    if len(fields) != 6 or fields[0] not in {"FETCH", "BACKFILL", "DISCOVER", "AVAILABILITY"}:
        return command
    offset = terminal_clock_offset(quote)
    if offset == timedelta(0):
        return command
    start = _parse(fields[3]) + offset
    end = _parse(fields[4]) + offset
    fields[3] = start.isoformat()
    fields[4] = end.isoformat()
    return "|".join(fields)
