from __future__ import annotations

from datetime import datetime, timedelta

from .clock import ensure_server_local

_TIMEFRAME_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
}


def timeframe_seconds(timeframe: str) -> int:
    if timeframe == "1mo":
        raise ValueError("1mo is calendar based and has no fixed number of seconds")
    try:
        return _TIMEFRAME_SECONDS[timeframe]
    except KeyError as exc:
        raise ValueError(f"unsupported timeframe: {timeframe}") from exc


def bucket_start(timestamp: datetime, timeframe: str) -> datetime:
    value = ensure_server_local(timestamp)
    if timeframe == "1m":
        return value.replace(second=0, microsecond=0)
    if timeframe in {"5m", "15m", "30m"}:
        minutes = int(timeframe[:-1])
        return value.replace(minute=value.minute - value.minute % minutes, second=0, microsecond=0)
    if timeframe == "1h":
        return value.replace(minute=0, second=0, microsecond=0)
    if timeframe == "4h":
        return value.replace(hour=value.hour - value.hour % 4, minute=0, second=0, microsecond=0)
    if timeframe == "1d":
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    if timeframe == "1w":
        day = value.replace(hour=0, minute=0, second=0, microsecond=0)
        return day - timedelta(days=day.weekday())
    if timeframe == "1mo":
        return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"unsupported timeframe: {timeframe}")


def bucket_end(timestamp: datetime, timeframe: str) -> datetime:
    start = bucket_start(timestamp, timeframe)
    if timeframe == "1mo":
        if start.month == 12:
            return start.replace(year=start.year + 1, month=1)
        return start.replace(month=start.month + 1)
    return start + timedelta(seconds=timeframe_seconds(timeframe))
