from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from .domain import Candle
from .storage import MarketDataStore
from .timeframes import bucket_end, bucket_start


CANONICAL_DERIVED_TIMEFRAMES = ("5m", "15m", "30m", "1h", "4h", "1d")


class CandleAggregator:
    def __init__(self, store: MarketDataStore) -> None:
        self._store = store

    def aggregate(self, instrument: str, target_timeframe: str, provider: str) -> int:
        if target_timeframe == "1m":
            raise ValueError("target timeframe must be larger than 1m")
        source = self._store.read_candles(instrument, "1m", limit=100_000, provider=provider)
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
        source = self._store.read_candles_range(instrument, "1m", start, end, provider)
        if not source:
            return None
        candle = self._build(provider, instrument, target_timeframe, start, source)
        self._store.upsert_candle(candle)
        return candle

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
