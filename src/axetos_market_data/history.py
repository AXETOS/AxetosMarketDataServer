from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from .domain import Candle
from .calendar import MarketCalendar
from .storage import MarketDataStore
from .timeframes import bucket_start


_TIMEFRAME_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14_400,
    "1d": 86_400,
}


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


@dataclass(slots=True)
class GapRepairResult:
    provider: str
    instrument: str | None
    timeframe: str | None
    gaps_selected: int
    windows_requested: int
    candles_received: int
    candles_written: int
    invalid_candles: int
    gaps_resolved: int
    gaps_remaining: int


class HistoricalBackfillService:
    def __init__(self, store: MarketDataStore, calendar: MarketCalendar | None = None) -> None:
        self.store = store
        self.calendar = calendar or MarketCalendar()
        self.log = logging.getLogger(__name__)

    def run(
        self,
        provider: HistoricalCandleProvider,
        provider_key: str,
        symbol: str,
        instrument: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> BackfillResult:
        self._validate_timeframe(timeframe)
        start, end = start.astimezone(UTC), end.astimezone(UTC)
        candles = provider.fetch_candles(symbol, timeframe, start, end)
        valid, invalid = self._normalize(candles, provider_key, instrument, timeframe)
        written = self.store.upsert_candles(valid)
        self.store.set_ingestion_state(
            provider_key, instrument, timeframe, start, end,
            len(candles), written, invalid, "Completed", None,
        )
        gaps = self.detect_gaps(provider_key, instrument, timeframe, start, end)
        return BackfillResult(
            provider_key, instrument, timeframe, start.isoformat(), end.isoformat(),
            len(candles), written, invalid, gaps,
        )

    def detect_gaps(
        self,
        provider: str,
        instrument: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> int:
        self._validate_timeframe(timeframe)
        start, end = start.astimezone(UTC), end.astimezone(UTC)
        seconds = _TIMEFRAME_SECONDS[timeframe]
        existing = set(self.store.read_candle_times(provider, instrument, timeframe, start, end))
        self.store.clear_gaps(provider, instrument, timeframe, start, end)
        cursor = bucket_start(start, timeframe)
        gaps = 0
        while cursor < end:
            if self.calendar.is_expected_open(instrument, cursor) and cursor not in existing:
                self.store.record_gap(
                    provider, instrument, timeframe,
                    cursor, cursor + timedelta(seconds=seconds),
                )
                gaps += 1
            cursor += timedelta(seconds=seconds)
        return gaps

    def repair_gaps(
        self,
        provider: HistoricalCandleProvider,
        provider_key: str,
        symbol_for_instrument: dict[str, str],
        instrument: str | None = None,
        timeframe: str | None = None,
        limit: int = 500,
    ) -> GapRepairResult:
        if timeframe is not None:
            self._validate_timeframe(timeframe)
        gaps = self.store.list_gaps(
            limit=limit,
            provider=provider_key,
            instrument=instrument,
            timeframe=timeframe,
        )
        if not gaps:
            return GapRepairResult(provider_key, instrument, timeframe, 0, 0, 0, 0, 0, 0, 0)

        windows = self._group_gaps(gaps)
        received = written = invalid = 0
        attempted_gap_ids: list[int] = []

        for window in windows:
            mapped_symbol = symbol_for_instrument.get(window["instrument"])
            if mapped_symbol is None:
                self.log.warning("No provider symbol mapping for %s", window["instrument"])
                continue
            candles = provider.fetch_candles(
                mapped_symbol,
                window["timeframe"],
                window["start"],
                window["end"],
            )
            valid, bad = self._normalize(
                candles,
                provider_key,
                window["instrument"],
                window["timeframe"],
            )
            received += len(candles)
            invalid += bad
            written += self.store.upsert_candles(valid)
            attempted_gap_ids.extend(window["gap_ids"])

        resolved = 0
        for gap in gaps:
            gap_id = int(gap["id"])
            if gap_id not in attempted_gap_ids:
                continue
            open_time = datetime.fromisoformat(str(gap["gap_from_utc"]))
            if self.store.candle_exists(
                str(gap["provider"]),
                str(gap["instrument"]),
                str(gap["timeframe"]),
                open_time,
            ):
                self.store.mark_gap_resolved(gap_id)
                resolved += 1

        remaining = self.store.count_gaps(
            provider=provider_key,
            instrument=instrument,
            timeframe=timeframe,
        )
        result = GapRepairResult(
            provider=provider_key,
            instrument=instrument,
            timeframe=timeframe,
            gaps_selected=len(gaps),
            windows_requested=len(windows),
            candles_received=received,
            candles_written=written,
            invalid_candles=invalid,
            gaps_resolved=resolved,
            gaps_remaining=remaining,
        )
        self.store.record_repair_run(result)
        return result

    @staticmethod
    def _group_gaps(gaps: list[dict[str, object]]) -> list[dict[str, object]]:
        ordered = sorted(
            gaps,
            key=lambda gap: (
                str(gap["instrument"]),
                str(gap["timeframe"]),
                str(gap["gap_from_utc"]),
            ),
        )
        windows: list[dict[str, object]] = []
        for gap in ordered:
            start = datetime.fromisoformat(str(gap["gap_from_utc"]))
            end = datetime.fromisoformat(str(gap["gap_to_utc"]))
            if (
                windows
                and windows[-1]["instrument"] == gap["instrument"]
                and windows[-1]["timeframe"] == gap["timeframe"]
                and start <= windows[-1]["end"]
            ):
                windows[-1]["end"] = max(windows[-1]["end"], end)
                windows[-1]["gap_ids"].append(int(gap["id"]))
            else:
                windows.append({
                    "instrument": str(gap["instrument"]),
                    "timeframe": str(gap["timeframe"]),
                    "start": start,
                    "end": end,
                    "gap_ids": [int(gap["id"])],
                })
        return windows

    @staticmethod
    def _normalize(
        candles: list[Candle],
        provider_key: str,
        instrument: str,
        timeframe: str,
    ) -> tuple[list[Candle], int]:
        valid: list[Candle] = []
        invalid = 0
        for candle in candles:
            try:
                valid.append(Candle(
                    provider=provider_key,
                    instrument=instrument,
                    timeframe=timeframe,
                    open_time=candle.open_time,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    tick_count=candle.tick_count,
                    volume=candle.volume,
                    complete=True,
                ))
            except ValueError:
                invalid += 1
        return valid, invalid

    @staticmethod
    def _validate_timeframe(timeframe: str) -> None:
        if timeframe not in _TIMEFRAME_SECONDS:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
