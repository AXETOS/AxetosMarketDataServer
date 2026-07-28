from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Callable, Iterable

from .domain import Candle, Tick
from .storage import MarketDataStore
from .timeframes import bucket_end, bucket_start


@dataclass(slots=True)
class RepairStageResult:
    source_timeframe: str
    target_timeframe: str
    source_rows: int = 0
    candidates: int = 0
    created: int = 0
    overwritten: int = 0
    retained_same: int = 0
    retained_better_or_equal: int = 0
    incomplete_skipped: int = 0
    discrepancies: int = 0
    errors: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class HierarchicalCandleRepair:
    """Bottom-up, quality-aware candle repair.

    Finer complete data is authoritative. A candidate is written only when the target
    does not exist or the candidate has a strictly higher deterministic quality rank.
    Equal-quality differences are retained and reported as discrepancies.
    """

    STAGES = (
        ("1m", "15m", 520),
        ("1m", "1h", 500),
        ("1h", "1d", 450),
        ("1d", "1w", 400),
        ("1d", "1mo", 400),
    )

    def __init__(self, store: MarketDataStore) -> None:
        self.store = store

    def run(
        self,
        provider: str,
        instruments: Iterable[str],
        *,
        on_stage: Callable[[str, str, str, dict[str, object]], None] | None = None,
    ) -> dict[str, object]:
        run_id = uuid.uuid4().hex
        results: list[dict[str, object]] = []
        totals = {
            "repair_run_id": run_id,
            "instruments": 0,
            "stages": 0,
            "source_rows": 0,
            "candidates": 0,
            "created": 0,
            "overwritten": 0,
            "retained_same": 0,
            "retained_better_or_equal": 0,
            "incomplete_skipped": 0,
            "discrepancies": 0,
            "errors": 0,
        }
        for instrument in instruments:
            totals["instruments"] += 1
            for source, target, quality_rank in self.STAGES:
                if on_stage is not None:
                    on_stage("repair.stage_started", instrument, target, {
                        "repair_run_id": run_id,
                        "source_timeframe": source,
                        "target_timeframe": target,
                    })
                try:
                    result = (
                        self._repair_ticks_to_minutes(provider, instrument, run_id, quality_rank)
                        if source == "ticks"
                        else self._repair_candles(provider, instrument, source, target, run_id, quality_rank)
                    )
                except Exception:
                    result = RepairStageResult(source, target, errors=1)
                details = result.to_dict()
                details.update({"repair_run_id": run_id, "instrument": instrument})
                results.append(details)
                totals["stages"] += 1
                for key in (
                    "source_rows", "candidates", "created", "overwritten", "retained_same",
                    "retained_better_or_equal", "incomplete_skipped", "discrepancies", "errors",
                ):
                    totals[key] += int(getattr(result, key))
                if on_stage is not None:
                    on_stage("repair.stage_completed", instrument, target, details)
        totals["stage_results"] = results
        return totals

    def _repair_ticks_to_minutes(
        self, provider: str, instrument: str, run_id: str, quality_rank: int,
    ) -> RepairStageResult:
        result = RepairStageResult("ticks", "1m")
        first, last = self.store.tick_bounds(provider, instrument)
        if first is None or last is None:
            return result
        cursor = first.replace(hour=0, minute=0, second=0, microsecond=0)
        stop = last + timedelta(microseconds=1)
        while cursor < stop:
            chunk_end = min(stop, cursor + timedelta(days=1))
            ticks = self.store.read_ticks_range(
                provider, instrument, cursor, chunk_end - timedelta(microseconds=1), limit=2_000_000
            )
            result.source_rows += len(ticks)
            groups: dict[datetime, list[Tick]] = defaultdict(list)
            for tick in ticks:
                groups[bucket_start(tick.timestamp, "1m")].append(tick)
            for open_time, values in sorted(groups.items()):
                values.sort(key=lambda value: value.timestamp)
                # Sparse ticks must never overwrite a complete provider minute.
                span = values[-1].timestamp - values[0].timestamp
                coverage_complete = len(values) >= 2 and span >= timedelta(seconds=45)
                if not coverage_complete:
                    result.incomplete_skipped += 1
                    continue
                prices = [value.market_price for value in values]
                volumes = [value.volume for value in values if value.volume is not None]
                candidate = Candle(
                    provider=provider,
                    instrument=instrument,
                    timeframe="1m",
                    open_time=open_time,
                    open=prices[0],
                    high=max(prices),
                    low=min(prices),
                    close=prices[-1],
                    tick_count=len(values),
                    volume=sum(volumes, Decimal("0")) if volumes else None,
                    complete=True,
                )
                result.candidates += 1
                self._apply_candidate(candidate, "ticks", run_id, quality_rank, result)
            cursor = chunk_end
        return result

    def _repair_candles(
        self, provider: str, instrument: str, source_timeframe: str,
        target_timeframe: str, run_id: str, quality_rank: int,
    ) -> RepairStageResult:
        result = RepairStageResult(source_timeframe, target_timeframe)
        first, last = self.store.candle_bounds(provider, instrument, source_timeframe)
        if first is None or last is None:
            return result

        # Chunk size limits memory and yields between bounded transactions. Chunk
        # boundaries are expanded through bucket_start, and processed buckets are
        # de-duplicated when week/month buckets cross a chunk boundary.
        span = {
            "1m": timedelta(days=1),
            "1h": timedelta(days=31),
            "1d": timedelta(days=366),
        }[source_timeframe]
        cursor = first
        stop = last + self._source_step(source_timeframe)
        processed: set[datetime] = set()
        while cursor < stop:
            chunk_end = min(stop, cursor + span)
            source = self.store.read_candles_range(
                instrument, source_timeframe, cursor, chunk_end, provider
            )
            result.source_rows += len(source)
            groups: dict[datetime, list[Candle]] = defaultdict(list)
            for candle in source:
                groups[bucket_start(candle.open_time, target_timeframe)].append(candle)
            for open_time, values in sorted(groups.items()):
                if open_time in processed:
                    continue
                # Fetch the full target bucket because chunk edges may bisect it.
                values = self.store.read_candles_range(
                    instrument, source_timeframe, open_time,
                    bucket_end(open_time, target_timeframe), provider,
                )
                processed.add(open_time)
                if not values or not all(value.complete for value in values):
                    result.incomplete_skipped += 1
                    continue
                candidate = self._build(provider, instrument, target_timeframe, open_time, values)
                result.candidates += 1
                self._apply_candidate(candidate, source_timeframe, run_id, quality_rank, result)
            cursor = chunk_end
        return result

    def _apply_candidate(
        self, candidate: Candle, source_timeframe: str, run_id: str,
        quality_rank: int, result: RepairStageResult,
    ) -> None:
        existing_rows = self.store.read_candles_range(
            candidate.instrument, candidate.timeframe, candidate.open_time,
            bucket_end(candidate.open_time, candidate.timeframe), candidate.provider,
        )
        existing = next((value for value in existing_rows if value.open_time == candidate.open_time), None)
        if existing is None:
            self.store.upsert_candle(candidate)
            self._record_provenance(candidate, source_timeframe, run_id, quality_rank)
            result.created += 1
            return

        if self._same_values(existing, candidate):
            result.retained_same += 1
            return

        provenance = self.store.get_candle_provenance(
            existing.provider, existing.instrument, existing.timeframe, existing.open_time
        )
        existing_rank = int(provenance["quality_rank"]) if provenance else (300 if existing.complete else 100)
        if candidate.complete and quality_rank > existing_rank:
            self.store.upsert_candle(candidate)
            self._record_provenance(candidate, source_timeframe, run_id, quality_rank)
            result.overwritten += 1
            return
        if quality_rank == existing_rank:
            result.discrepancies += 1
        result.retained_better_or_equal += 1

    def _record_provenance(
        self, candle: Candle, source_timeframe: str, run_id: str, quality_rank: int,
    ) -> None:
        self.store.set_candle_provenance(
            candle.provider, candle.instrument, candle.timeframe, candle.open_time,
            source_kind="derived", source_timeframe=source_timeframe,
            quality_rank=quality_rank, coverage_complete=candle.complete,
            repair_run_id=run_id,
        )

    @staticmethod
    def _same_values(first: Candle, second: Candle) -> bool:
        return (
            first.open == second.open and first.high == second.high and first.low == second.low
            and first.close == second.close and first.tick_count == second.tick_count
            and first.volume == second.volume and first.complete == second.complete
        )

    @staticmethod
    def _build(
        provider: str, instrument: str, target_timeframe: str,
        open_time: datetime, candles: list[Candle],
    ) -> Candle:
        candles.sort(key=lambda value: value.open_time)
        volumes = [value.volume for value in candles if value.volume is not None]
        return Candle(
            provider=provider,
            instrument=instrument,
            timeframe=target_timeframe,
            open_time=open_time,
            open=candles[0].open,
            high=max(value.high for value in candles),
            low=min(value.low for value in candles),
            close=candles[-1].close,
            tick_count=sum(value.tick_count for value in candles),
            volume=sum(volumes, Decimal("0")) if volumes else None,
            complete=all(value.complete for value in candles),
        )

    @staticmethod
    def _source_step(timeframe: str) -> timedelta:
        return {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[timeframe]
