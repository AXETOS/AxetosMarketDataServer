from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from .domain import Candle
from .storage import MarketDataStore
from .timeframes import bucket_end, bucket_start


CANONICAL_DERIVED_TIMEFRAMES = ("5m", "15m", "30m", "1h", "4h", "1d", "1w", "1mo")

# Runtime-finalized timeframes requested by the live server lifecycle.
# Intraday and daily candles are authoritative aggregates of official M1 bars;
# weekly and monthly candles are authoritative aggregates of completed D1 bars.
RUNTIME_DERIVED_TIMEFRAMES = ("15m", "1h", "1d", "1w", "1mo")

_SOURCE_TIMEFRAME = {
    "5m": "1m", "15m": "1m", "30m": "1m", "1h": "1m", "4h": "1m", "1d": "1m",
    "1w": "1d", "1mo": "1d",
}


class CandleAggregator:
    def __init__(self, store: MarketDataStore) -> None:
        self._store = store

    def aggregate(
        self, instrument: str, target_timeframe: str, provider: str, *, replace: bool = True
    ) -> int:
        if target_timeframe == "1m":
            raise ValueError("target timeframe must be larger than 1m")
        source_timeframe = _SOURCE_TIMEFRAME[target_timeframe]
        source = self._store.read_candles(instrument, source_timeframe, limit=100_000, provider=provider)
        if replace:
            # Rebuild means replace. Leaving old target buckets behind after source
            # cleanup creates disconnected chart fragments and mixes old pipeline data
            # with the new authoritative series.
            self._store.delete_candles(provider, instrument, target_timeframe)
        groups: dict[object, list[Candle]] = defaultdict(list)
        for candle in source:
            groups[bucket_start(candle.open_time, target_timeframe)].append(candle)

        written = 0
        for open_time, candles in sorted(groups.items()):
            self._store.upsert_candle(self._build(provider, instrument, target_timeframe, open_time, candles))
            written += 1
        return written

    def refresh_bucket(
        self,
        instrument: str,
        target_timeframe: str,
        provider: str,
        reference_time: datetime,
    ) -> Candle | None:
        if target_timeframe == "1m":
            raise ValueError("target timeframe must be larger than 1m")
        start = bucket_start(reference_time, target_timeframe)
        end = bucket_end(reference_time, target_timeframe)
        source_timeframe = _SOURCE_TIMEFRAME[target_timeframe]
        source = self._store.read_candles_range(instrument, source_timeframe, start, end, provider)
        if not source:
            return None
        candle = self._build(provider, instrument, target_timeframe, start, source)
        self._store.upsert_candle(candle)
        return candle


    def build_temporary(
        self,
        instrument: str,
        target_timeframe: str,
        provider: str,
        reference_time: datetime,
        current_bid: Decimal,
    ) -> Candle | None:
        """Build the open aggregate bucket without persisting it.

        The candle uses completed lower-timeframe source candles already stored in
        the active bucket plus the current live Bid as the unfinished source point.
        """
        if target_timeframe not in RUNTIME_DERIVED_TIMEFRAMES:
            raise ValueError(f"unsupported runtime timeframe: {target_timeframe}")
        start = bucket_start(reference_time, target_timeframe)
        end = bucket_end(reference_time, target_timeframe)
        source_timeframe = _SOURCE_TIMEFRAME[target_timeframe]
        source = self._store.read_candles_range(instrument, source_timeframe, start, end, provider)

        if target_timeframe in {"1w", "1mo"}:
            # Include today's still-open daily state, itself built from completed M1
            # bars plus the current Bid. This preserves intraday high/low movement in
            # the temporary weekly/monthly candle without storing provisional rows.
            temporary_day = self.build_temporary(instrument, "1d", provider, reference_time, current_bid)
            if temporary_day is not None:
                source = [item for item in source if item.open_time != temporary_day.open_time]
                source.append(temporary_day)
        else:
            point_time = bucket_start(reference_time, source_timeframe)
            source = [item for item in source if item.open_time != point_time]
            source.append(Candle(
                provider=provider,
                instrument=instrument,
                timeframe=source_timeframe,
                open_time=point_time,
                open=current_bid,
                high=current_bid,
                low=current_bid,
                close=current_bid,
                tick_count=0,
                volume=None,
                complete=False,
            ))

        if not source:
            return None
        candle = self._build(provider, instrument, target_timeframe, start, source)
        return Candle(
            provider=candle.provider, instrument=candle.instrument, timeframe=candle.timeframe,
            open_time=candle.open_time, open=candle.open, high=candle.high, low=candle.low,
            close=current_bid, tick_count=candle.tick_count, volume=candle.volume, complete=False,
        )

    def finalize_closed_buckets(
        self,
        instrument: str,
        provider: str,
        source_timeframe: str,
        reference_times: list[datetime],
        now: datetime,
    ) -> list[Candle]:
        """Persist only aggregate buckets whose time window has fully closed."""
        targets = {
            "1m": ("15m", "1h", "1d"),
            "1d": ("1w", "1mo"),
        }.get(source_timeframe, ())
        written: list[Candle] = []
        seen: set[tuple[str, datetime]] = set()
        for target in targets:
            for reference_time in reference_times:
                start = bucket_start(reference_time, target)
                key = (target, start)
                if key in seen or bucket_end(reference_time, target) > now:
                    continue
                seen.add(key)
                candle = self.refresh_bucket(instrument, target, provider, reference_time)
                if candle is not None:
                    written.append(candle)
        return written

    @staticmethod
    def _build(
        provider: str,
        instrument: str,
        target_timeframe: str,
        open_time: datetime,
        candles: list[Candle],
    ) -> Candle:
        candles.sort(key=lambda item: item.open_time)
        volume_values = [item.volume for item in candles if item.volume is not None]
        return Candle(
            provider=provider,
            instrument=instrument,
            timeframe=target_timeframe,
            open_time=open_time,
            open=candles[0].open,
            high=max(item.high for item in candles),
            low=min(item.low for item in candles),
            close=candles[-1].close,
            tick_count=sum(item.tick_count for item in candles),
            volume=sum(volume_values, Decimal("0")) if volume_values else None,
            complete=all(item.complete for item in candles),
        )
