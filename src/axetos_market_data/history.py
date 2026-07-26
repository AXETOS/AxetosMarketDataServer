from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from .domain import Candle
from .calendar import MarketCalendar
from .storage import MarketDataStore
from .operational import OperationalEventService
from .timeframes import bucket_start
from .aggregation import CANONICAL_DERIVED_TIMEFRAMES, CandleAggregator


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




@dataclass(slots=True)
class HistoryRebuildResult:
    provider: str
    instrument: str
    requested_from_utc: str
    requested_to_utc: str
    received_minutes: int
    accepted_minutes: int
    discarded_flat_minutes: int
    deleted_candles: int
    derived_candles: int


class AuthoritativeHistoryRebuildService:
    """Destructively rebuild one instrument from one provider's authoritative M1 history."""

    def __init__(self, store: MarketDataStore, short_flat_minutes: int = 60) -> None:
        self.store = store
        self.short_flat_minutes = short_flat_minutes
        self.events = OperationalEventService(store)

    def run(
        self,
        provider: HistoricalCandleProvider,
        provider_key: str,
        symbol: str,
        instrument: str,
        start: datetime,
        end: datetime,
    ) -> HistoryRebuildResult:
        start, end = start.astimezone(UTC), end.astimezone(UTC)
        fetched = provider.fetch_candles(symbol, "1m", start, end)
        normalized, invalid = HistoricalBackfillService._normalize(
            fetched, provider_key, instrument, "1m"
        )
        accepted = self._sanitize(normalized)
        discarded = len(normalized) - len(accepted)

        # Remove every old candle for this provider/instrument. Derived candles outside
        # the requested window can otherwise survive and appear as disconnected chart
        # islands after a clean rebuild.
        deleted = self.store.delete_candles(provider_key, instrument)
        self.store.clear_gaps(provider_key, instrument, "1m", start, end)
        self.store.upsert_candles(accepted)

        aggregator = CandleAggregator(self.store)
        derived = 0
        for timeframe in CANONICAL_DERIVED_TIMEFRAMES:
            derived += aggregator.aggregate(instrument, timeframe, provider_key, replace=True)

        self.store.set_ingestion_state(
            provider_key, instrument, "1m", start, end, len(fetched), len(accepted),
            invalid + discarded, "Rebuilt", None,
        )
        result = HistoryRebuildResult(
            provider_key, instrument, start.isoformat(), end.isoformat(), len(fetched),
            len(accepted), discarded, deleted, derived,
        )
        self.events.record(
            "info", "history.rebuilt", "Authoritative instrument history rebuilt",
            provider=provider_key, instrument=instrument,
            details={name: getattr(result, name) for name in result.__slots__},
        )
        return result

    def _sanitize(self, candles: list[Candle]) -> list[Candle]:
        values = sorted(candles, key=lambda item: item.open_time)
        if not values:
            return []
        accepted: list[Candle] = []
        pending: list[Candle] = []
        last: Candle | None = None
        for candle in values:
            # Exact duplicate timestamps are replaced deterministically by the newest
            # provider row before continuity/flat-run analysis.
            if accepted and candle.open_time == accepted[-1].open_time:
                accepted[-1] = candle
                last = candle
                continue
            if last is not None and self._is_flat_at_previous_close(candle, last):
                pending.append(candle)
                continue
            if pending:
                elapsed_minutes = int((candle.open_time - last.open_time).total_seconds() // 60)
                if elapsed_minutes <= self.short_flat_minutes:
                    accepted.extend(pending)
                    last = pending[-1]
                pending.clear()
            accepted.append(candle)
            last = candle
        # A trailing unchanged run is unresolved. It is omitted until a later changed
        # provider bar proves that it was a short interruption rather than closure.
        return accepted

    @staticmethod
    def _is_flat_at_previous_close(candidate: Candle, previous: Candle) -> bool:
        identical_ohlc = (
            candidate.open, candidate.high, candidate.low, candidate.close
        ) == (previous.open, previous.high, previous.low, previous.close)
        flat_at_close = (
            candidate.open == candidate.high == candidate.low == candidate.close
            == previous.close
        )
        return identical_ohlc or flat_at_close


class HistoricalBackfillService:
    def __init__(self, store: MarketDataStore, calendar: MarketCalendar | None = None) -> None:
        self.store = store
        self.calendar = calendar or MarketCalendar()
        self.log = logging.getLogger(__name__)
        self.events = OperationalEventService(store)

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
        self.events.record("info", "backfill.completed", "Historical backfill completed", provider=provider_key, instrument=instrument, details={"timeframe": timeframe, "received": len(candles), "written": written, "invalid": invalid, "gaps": gaps})
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
        self.events.record("warning" if gaps else "info", "gap.scan", "Candle gap scan completed", provider=provider, instrument=instrument, details={"timeframe": timeframe, "start_utc": start.isoformat(), "end_utc": end.isoformat(), "gaps": gaps})
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
        self.events.record("warning" if remaining else "info", "gap.repair", "Gap repair completed", provider=provider_key, instrument=instrument, details={name: getattr(result, name) for name in result.__slots__})
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
