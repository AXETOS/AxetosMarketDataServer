from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from .domain import Candle
from .storage import MarketDataStore
from .timeframes import bucket_start


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
            candles.sort(key=lambda item: item.open_time)
            volume_values = [item.volume for item in candles if item.volume is not None]
            candle = Candle(
                provider=provider,
                instrument=instrument,
                timeframe=target_timeframe,
                open_time=open_time,  # type: ignore[arg-type]
                open=candles[0].open,
                high=max(item.high for item in candles),
                low=min(item.low for item in candles),
                close=candles[-1].close,
                tick_count=sum(item.tick_count for item in candles),
                volume=sum(volume_values, Decimal("0")) if volume_values else None,
                complete=all(item.complete for item in candles),
            )
            self._store.upsert_candle(candle)
            written += 1
        return written
