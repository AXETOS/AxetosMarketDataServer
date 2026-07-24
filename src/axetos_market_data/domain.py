from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal


UTC = timezone.utc


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class Tick:
    provider: str
    instrument: str
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    volume: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", ensure_utc(self.timestamp))
        if not self.provider.strip():
            raise ValueError("provider is required")
        if not self.instrument.strip():
            raise ValueError("instrument is required")
        if self.bid <= 0 or self.ask <= 0:
            raise ValueError("bid and ask must be positive")
        if self.ask < self.bid:
            raise ValueError("ask cannot be lower than bid")

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")


@dataclass(frozen=True, slots=True)
class Candle:
    provider: str
    instrument: str
    timeframe: str
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    tick_count: int
    volume: Decimal | None = None
    complete: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "open_time", ensure_utc(self.open_time))
        if self.timeframe not in {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}:
            raise ValueError(f"unsupported timeframe: {self.timeframe}")
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("OHLC values must be positive")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("invalid OHLC range")
        if self.tick_count < 0:
            raise ValueError("tick_count cannot be negative")
