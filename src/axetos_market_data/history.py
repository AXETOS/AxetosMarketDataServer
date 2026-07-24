from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from .domain import Candle
from .storage import MarketDataStore
from .timeframes import bucket_start


class HistoricalCandleProvider(Protocol):
    def fetch_candles(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> list[Candle]: ...


@dataclass(slots=True)
class BackfillResult:
    provider: str
    instrument: str
    timeframe: str
    requested_from_utc: str
    requested_to_utc: str
    received: int
    written: int
    invalid: int
    gaps: int


class HistoricalBackfillService:
    def __init__(self, store: MarketDataStore) -> None:
        self.store = store
        self.log = logging.getLogger(__name__)

    def run(self, provider: HistoricalCandleProvider, provider_key: str, symbol: str,
            instrument: str, timeframe: str, start: datetime, end: datetime) -> BackfillResult:
        start, end = start.astimezone(UTC), end.astimezone(UTC)
        candles = provider.fetch_candles(symbol, timeframe, start, end)
        valid: list[Candle] = []
        invalid = 0
        for candle in candles:
            try:
                valid.append(Candle(
                    provider=provider_key,
                    instrument=instrument,
                    timeframe=timeframe,
                    open_time=candle.open_time,
                    open=candle.open, high=candle.high, low=candle.low, close=candle.close,
                    tick_count=candle.tick_count, volume=candle.volume, complete=True,
                ))
            except ValueError:
                invalid += 1
        written = self.store.upsert_candles(valid)
        self.store.set_ingestion_state(provider_key, instrument, timeframe, start, end,
                                       len(candles), written, invalid, "Completed", None)
        gaps = self.detect_gaps(provider_key, instrument, timeframe, start, end)
        return BackfillResult(provider_key, instrument, timeframe, start.isoformat(), end.isoformat(),
                              len(candles), written, invalid, gaps)

    def detect_gaps(self, provider: str, instrument: str, timeframe: str,
                    start: datetime, end: datetime) -> int:
        seconds = {"1m":60,"5m":300,"15m":900,"30m":1800,"1h":3600,"4h":14400,"1d":86400}[timeframe]
        existing = set(self.store.read_candle_times(provider, instrument, timeframe, start, end))
        self.store.clear_gaps(provider, instrument, timeframe, start, end)
        cursor = bucket_start(start, timeframe)
        gaps = 0
        while cursor < end:
            # Markets close on weekends; do not report those periods as data defects.
            if cursor.weekday() < 5 and cursor not in existing:
                self.store.record_gap(provider, instrument, timeframe, cursor, cursor + timedelta(seconds=seconds))
                gaps += 1
            cursor += timedelta(seconds=seconds)
        return gaps
