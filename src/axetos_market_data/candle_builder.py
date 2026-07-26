from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
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
    def from_tick(cls, tick: Tick, opening_price: Decimal | None = None) -> "_MutableCandle":
        first_price = tick.market_price
        open_price = first_price if opening_price is None else opening_price
        return cls(
            provider=tick.provider,
            instrument=tick.instrument,
            open_time=bucket_start(tick.timestamp, "1m"),
            open=open_price,
            high=max(open_price, first_price),
            low=min(open_price, first_price),
            close=first_price,
            tick_count=1,
            volume=tick.volume,
        )

    def apply(self, tick: Tick) -> None:
        price = tick.market_price
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
    """Build one-minute candles while preserving feed-confirmed continuity."""

    def __init__(self, store: MarketDataStore) -> None:
        self._store = store
        self._active: dict[tuple[str, str], _MutableCandle] = {}

    def ingest(self, tick: Tick, continuity: str = "CONNECTED") -> None:
        if continuity not in {"CONNECTED", "DETACHED"}:
            raise ValueError("continuity must be CONNECTED or DETACHED")
        key = (tick.provider, tick.instrument)
        minute = bucket_start(tick.timestamp, "1m")
        active = self._active.get(key)
        if active is None:
            opening = self._previous_close(tick, minute) if continuity == "CONNECTED" else None
            self._active[key] = _MutableCandle.from_tick(tick, opening)
            self._store.upsert_candle(self._active[key].freeze(complete=False))
            return
        if minute < active.open_time:
            raise ValueError("out-of-order tick belongs to an already finalized minute")
        if minute > active.open_time:
            previous_close = active.close
            self._store.upsert_candle(active.freeze(complete=True))
            opening = previous_close if continuity == "CONNECTED" and minute == active.open_time + timedelta(minutes=1) else None
            self._active[key] = _MutableCandle.from_tick(tick, opening)
            self._store.upsert_candle(self._active[key].freeze(complete=False))
            return
        active.apply(tick)
        self._store.upsert_candle(active.freeze(complete=False))

    def finalize(self, provider: str, instrument: str, complete: bool = True) -> Candle | None:
        active = self._active.pop((provider, instrument), None)
        if active is None:
            return None
        candle = active.freeze(complete=complete)
        self._store.upsert_candle(candle)
        return candle

    def flush(self, complete: bool = False) -> int:
        for candle in self._active.values():
            self._store.upsert_candle(candle.freeze(complete=complete))
        return len(self._active)

    def _previous_close(self, tick: Tick, minute: datetime) -> Decimal | None:
        candles = self._store.read_candles(tick.instrument, "1m", limit=1, provider=tick.provider)
        if not candles:
            return None
        previous = candles[-1]
        return previous.close if previous.open_time + timedelta(minutes=1) == minute else None
