from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from .domain import Candle, Tick
from .storage import MarketDataStore
from .timeframes import bucket_start

GapVerifier = Callable[[str, str, datetime, datetime], list[Candle]]


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
    materialized: bool = False

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
    """Build authoritative one-minute candles without fabricating closed-market flatlines.

    A minute whose complete OHLC is identical to the previous persisted candle is kept
    pending rather than written.  When price movement resumes, provider history is
    consulted first.  Missing minutes are synthesized as flat candles only when the
    elapsed interval is at most ``short_gap_minutes``.  Longer intervals remain gaps
    unless the provider supplies authoritative historical candles.
    """

    def __init__(
        self,
        store: MarketDataStore,
        gap_verifier: GapVerifier | None = None,
        short_gap_minutes: int = 60,
    ) -> None:
        if short_gap_minutes < 1:
            raise ValueError("short_gap_minutes must be positive")
        self._store = store
        self._gap_verifier = gap_verifier
        self._short_gap = timedelta(minutes=short_gap_minutes)
        self._active: dict[tuple[str, str], _MutableCandle] = {}

    def ingest(self, tick: Tick, continuity: str = "CONNECTED") -> list[Candle]:
        if continuity not in {"CONNECTED", "DETACHED"}:
            raise ValueError("continuity must be CONNECTED or DETACHED")
        key = (tick.provider, tick.instrument)
        minute = bucket_start(tick.timestamp, "1m")
        active = self._active.get(key)
        written: list[Candle] = []

        if active is not None and minute < active.open_time:
            raise ValueError("out-of-order tick belongs to an already finalized minute")

        if active is not None and minute == active.open_time:
            active.apply(tick)
            previous = self._previous_completed_before(active.provider, active.instrument, active.open_time)
            if active.materialized or not self._is_unchanged_flat(active, previous):
                written.append(self._persist_active(active))
            return written

        if active is not None:
            written.extend(self._finalize_active(active))

        previous = self._latest_completed(tick.provider, tick.instrument)
        candidate = _MutableCandle.from_tick(tick, previous.close if self._is_adjacent(previous, minute) and continuity == "CONNECTED" else None)
        self._active[key] = candidate

        # Unchanged observations are heartbeat evidence only.  Keep the minute in
        # memory and wait for actual movement before deciding whether the elapsed
        # interval was a short outage or a genuine market closure.
        if self._is_unchanged_flat(candidate, previous):
            return written

        original_previous = previous
        original_gap = minute - previous.open_time if previous is not None else timedelta(0)
        if previous is not None and minute > previous.open_time + timedelta(minutes=1):
            written.extend(self._verify_gap(
                tick.provider,
                tick.instrument,
                previous.open_time + timedelta(minutes=1),
                minute,
            ))
            previous = self._latest_completed(tick.provider, tick.instrument)

        if (
            original_previous is not None
            and original_gap <= self._short_gap
            and minute > original_previous.open_time + timedelta(minutes=1)
            and continuity == "CONNECTED"
        ):
            written.extend(self._fill_short_gap(original_previous, minute))
            previous = self._latest_completed(tick.provider, tick.instrument)

        connect = continuity == "CONNECTED" and self._is_adjacent(previous, minute)
        candidate = _MutableCandle.from_tick(tick, previous.close if connect and previous is not None else None)
        self._active[key] = candidate
        written.append(self._persist_active(candidate))
        return written

    def finalize(self, provider: str, instrument: str, complete: bool = True) -> Candle | None:
        active = self._active.pop((provider, instrument), None)
        if active is None:
            return None
        if not complete:
            if active.materialized:
                candle = active.freeze(complete=False)
                self._store.upsert_candle(candle)
                return candle
            return None
        values = self._finalize_active(active)
        return values[-1] if values else None

    def flush(self, complete: bool = False) -> int:
        written = 0
        for active in self._active.values():
            if active.materialized:
                self._store.upsert_candle(active.freeze(complete=complete))
                written += 1
        return written

    def _finalize_active(self, active: _MutableCandle) -> list[Candle]:
        previous = self._previous_completed_before(active.provider, active.instrument, active.open_time)
        if self._is_unchanged_flat(active, previous):
            if active.materialized:
                self._store.delete_candle(active.provider, active.instrument, "1m", active.open_time)
            return []
        candle = active.freeze(complete=True)
        self._store.upsert_candle(candle)
        active.materialized = True
        return [candle]

    def _persist_active(self, active: _MutableCandle) -> Candle:
        candle = active.freeze(complete=False)
        self._store.upsert_candle(candle)
        active.materialized = True
        return candle

    def _verify_gap(self, provider: str, instrument: str, start: datetime, end: datetime) -> list[Candle]:
        authoritative = self._store.read_candles_range(instrument, "1m", start, end, provider)
        if self._gap_verifier is not None:
            fetched = self._gap_verifier(provider, instrument, start, end)
            valid = [
                Candle(provider, instrument, "1m", item.open_time, item.open, item.high, item.low,
                       item.close, item.tick_count, item.volume, True)
                for item in fetched
                if start <= item.open_time < end
            ]
            if valid:
                self._store.upsert_candles(valid)
                authoritative = self._store.read_candles_range(instrument, "1m", start, end, provider)
        return authoritative

    def _fill_short_gap(self, previous: Candle, minute: datetime) -> list[Candle]:
        written: list[Candle] = []
        cursor = previous.open_time + timedelta(minutes=1)
        existing = {
            item.open_time
            for item in self._store.read_candles_range(
                previous.instrument, "1m", cursor, minute, previous.provider
            )
        }
        while cursor < minute:
            if cursor not in existing:
                flat = Candle(
                    previous.provider,
                    previous.instrument,
                    "1m",
                    cursor,
                    previous.close,
                    previous.close,
                    previous.close,
                    previous.close,
                    0,
                    None,
                    True,
                )
                self._store.upsert_candle(flat)
                written.append(flat)
            cursor += timedelta(minutes=1)
        return written

    def _latest_completed(self, provider: str, instrument: str) -> Candle | None:
        values = self._store.read_candles(instrument, "1m", limit=10, provider=provider)
        for item in reversed(values):
            if item.complete:
                return item
        return None

    def _previous_completed_before(self, provider: str, instrument: str, before: datetime) -> Candle | None:
        values = self._store.read_candles(
            instrument, "1m", limit=10, provider=provider, to_utc=before - timedelta(microseconds=1)
        )
        for item in reversed(values):
            if item.complete:
                return item
        return None

    @staticmethod
    def _is_adjacent(previous: Candle | None, minute: datetime) -> bool:
        return previous is not None and previous.open_time + timedelta(minutes=1) == minute

    @staticmethod
    def _is_unchanged_flat(active: _MutableCandle, previous: Candle | None) -> bool:
        return previous is not None and (
            active.open == active.high == active.low == active.close == previous.close
        )
