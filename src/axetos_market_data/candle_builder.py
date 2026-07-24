from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .domain import Candle, Tick
from .storage import MarketDataStore
from .timeframes import bucket_start


@dataclass(slots=True)
class _MutableCandle:
    provider: str
    instrument: str
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    tick_count: int = 0
    volume: Decimal | None = None

    @classmethod
    def from_tick(cls, tick: Tick) -> "_MutableCandle":
        price = tick.mid
        return cls(
            provider=tick.provider,
            instrument=tick.instrument,
            open_time=bucket_start(tick.timestamp, "1m"),
            open=price,
            high=price,
            low=price,
            close=price,
            tick_count=1,
            volume=tick.volume,
        )

    def apply(self, tick: Tick) -> None:
        price = tick.mid
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.tick_count += 1
        if tick.volume is not None:
            self.volume = (self.volume or Decimal("0")) + tick.volume

    def freeze(self, complete: bool) -> Candle:
        return Candle(
            provider=self.provider,
            instrument=self.instrument,
            timeframe="1m",
            open_time=self.open_time,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            tick_count=self.tick_count,
            volume=self.volume,
            complete=complete,
        )


class CandleBuilder:
    """Builds deterministic one-minute candles from normalized ticks."""

    def __init__(self, store: MarketDataStore) -> None:
        self._store = store
        self._active: dict[tuple[str, str], _MutableCandle] = {}

    def ingest(self, tick: Tick) -> None:
        key = (tick.provider, tick.instrument)
        minute = bucket_start(tick.timestamp, "1m")
        active = self._active.get(key)
        if active is None:
            self._active[key] = _MutableCandle.from_tick(tick)
            return
        if minute < active.open_time:
            raise ValueError("out-of-order tick belongs to an already finalized minute")
        if minute > active.open_time:
            self._store.upsert_candle(active.freeze(complete=True))
            self._active[key] = _MutableCandle.from_tick(tick)
            return
        active.apply(tick)

    def flush(self, complete: bool = False) -> int:
        for candle in self._active.values():
            self._store.upsert_candle(candle.freeze(complete=complete))
        return len(self._active)
