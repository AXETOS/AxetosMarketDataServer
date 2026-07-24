from __future__ import annotations

from datetime import datetime, timedelta

from .domain import ensure_utc


_TIMEFRAME_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


def timeframe_seconds(timeframe: str) -> int:
    try:
        return _TIMEFRAME_SECONDS[timeframe]
    except KeyError as exc:
        raise ValueError(f"unsupported timeframe: {timeframe}") from exc


def bucket_start(timestamp: datetime, timeframe: str) -> datetime:
    timestamp = ensure_utc(timestamp)
    seconds = timeframe_seconds(timeframe)
    epoch = int(timestamp.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % seconds), tz=timestamp.tzinfo)


def bucket_end(timestamp: datetime, timeframe: str) -> datetime:
    return bucket_start(timestamp, timeframe) + timedelta(seconds=timeframe_seconds(timeframe))
