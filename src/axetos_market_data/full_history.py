from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from .clock import server_now


@dataclass(frozen=True, slots=True)
class HistoryTier:
    timeframe: str
    start: datetime
    end: datetime
    batch_span: timedelta
    availability_span: timedelta | None = None


@dataclass(slots=True)
class InstrumentProgress:
    provider_symbol: str
    instrument: str
    status: str = "queued"
    tier_index: int = 0
    timeframe: str | None = None
    tier_start: datetime | None = None
    tier_end: datetime | None = None
    cursor_start: datetime | None = None
    current_end: datetime | None = None
    current_request_id: str | None = None
    request_kind: str | None = None
    dispatched_at: datetime | None = None
    dispatch_attempts: int = 0
    last_result: str | None = None
    available_count: int = 0
    local_count: int = 0
    earliest_available: datetime | None = None
    latest_available: datetime | None = None
    batches_completed: int = 0
    ranges_probed: int = 0
    ranges_skipped_existing: int = 0
    ranges_unavailable: int = 0
    bars_available: int = 0
    bars_received: int = 0
    bars_inserted: int = 0
    retry_count: int = 0
    error: str | None = None
    last_error_code: int | None = None
    scan_phase: str = "fine"
    coarse_start: datetime | None = None
    coarse_end: datetime | None = None
    fine_end: datetime | None = None
    coarse_ranges_probed: int = 0
    fine_ranges_probed: int = 0


@dataclass(slots=True)
class FullHistoryJob:
    job_id: str
    provider_key: str
    created_at: datetime
    status: str = "running"
    instruments: list[InstrumentProgress] = field(default_factory=list)
    active_index: int = 0
    completed_at: datetime | None = None


class FullHistoryBackfillManager:
    """Availability-aware, tiered and low-priority MT5 history coordinator.

    Every exact range is probed before it is downloaded. The server compares the
    provider's confirmed candle count with local coverage and requests a download only
    when the local database is missing rows. Existing rows are never overwritten.
    """

    def __init__(
        self,
        local_count: Callable[[str, str, str, datetime, datetime], int],
        *,
        on_instrument_completed: Callable[[str, str], None] | None = None,
        pressure_probe: Callable[[], bool] | None = None,
        now_factory: Callable[[], datetime] = server_now,
        request_lease: timedelta = timedelta(seconds=30),
    ) -> None:
        self._local_count = local_count
        self._lock = threading.RLock()
        self._on_instrument_completed = on_instrument_completed
        self._pressure_probe = pressure_probe
        self._now_factory = now_factory
        self._request_lease = request_lease
        self._jobs: dict[str, FullHistoryJob] = {}
        self._provider_job: dict[str, str] = {}
        self._tiers_by_job: dict[str, list[HistoryTier]] = {}

    def set_pressure_probe(self, pressure_probe: Callable[[], bool] | None) -> None:
        with self._lock:
            self._pressure_probe = pressure_probe

    def _build_tiers(self) -> list[HistoryTier]:
        now = self._now_factory().replace(second=0, microsecond=0)
        recent_start = now - timedelta(days=30)
        hourly_start = now - timedelta(days=365 * 3)
        # D1 availability is cheap to probe deeply and provides honest long-range
        # coverage without millions of M1 rows.
        deep_start = datetime(1970, 1, 1, tzinfo=now.tzinfo)
        hourly_end = recent_start - timedelta(hours=1)
        daily_end = hourly_start - timedelta(days=1)
        return [
            # Every tier starts with one broad provider-availability probe across
            # the complete tier. Only a tier that actually contains provider data
            # is subdivided into its normal storage batches. This prevents an
            # unavailable H1 or D1 tier from being crawled month by month/year by
            # year merely to rediscover the same absence repeatedly.
            HistoryTier("1m", recent_start, now, timedelta(days=1), now - recent_start + timedelta(minutes=1)),
            HistoryTier("1h", hourly_start, hourly_end, timedelta(days=30), hourly_end - hourly_start + timedelta(hours=1)),
            HistoryTier("1d", deep_start, daily_end, timedelta(days=365), daily_end - deep_start + timedelta(days=1)),
        ]

    def start(self, provider_key: str, symbols: list[tuple[str, str]]) -> dict[str, object]:
        with self._lock:
            existing_id = self._provider_job.get(provider_key)
            if existing_id and self._jobs[existing_id].status == "running":
                raise RuntimeError("A full-history backfill is already running for this provider")
            job = FullHistoryJob(
                job_id=uuid.uuid4().hex,
                provider_key=provider_key,
                created_at=self._now_factory(),
                instruments=[InstrumentProgress(symbol, instrument) for symbol, instrument in symbols],
            )
            self._jobs[job.job_id] = job
            self._provider_job[provider_key] = job.job_id
            self._tiers_by_job[job.job_id] = self._build_tiers()
            return self._view(job)

    def next_request(self, provider_key: str) -> str:
        with self._lock:
            job = self._active_job(provider_key)
            if job is None:
                return ""
            self._advance_completed(job)
            if job.status != "running":
                return ""
            item = job.instruments[job.active_index]
            if item.current_request_id:
                # Exactly one command is delivered per provider at a time. Once a
                # command has been handed to MT5, polling returns no work until the
                # result is acknowledged. A bounded lease permits redelivery only
                # when the bridge disappeared before reporting a result.
                now = self._now_factory()
                if item.dispatched_at is not None and now - item.dispatched_at < self._request_lease:
                    return ""
                item.dispatched_at = now
                item.dispatch_attempts += 1
                item.status = "probing" if item.request_kind == "availability" else "importing"
                return self._format_request(item)
            self._ensure_tier(job, item)
            if item.status == "completed":
                self._advance_completed(job)
                return self.next_request(provider_key) if job.status == "running" else ""
            item.current_request_id = uuid.uuid4().hex
            item.request_kind = "availability"
            item.status = "probing"
            item.dispatched_at = self._now_factory()
            item.dispatch_attempts = 1
            return self._format_request(item)

    def availability_result(
        self,
        provider_key: str,
        request_id: str,
        *,
        earliest: datetime | None,
        latest: datetime | None,
        count: int,
    ) -> str:
        with self._lock:
            job, item = self._active_item(provider_key, request_id)
            if item is None or job is None:
                return "IGNORED"
            item.ranges_probed += 1
            if item.scan_phase == "coarse":
                item.coarse_ranges_probed += 1
            else:
                item.fine_ranges_probed += 1
            item.available_count = max(0, count)
            item.earliest_available = earliest
            item.latest_available = latest
            item.bars_available += max(0, count)
            assert item.timeframe and item.cursor_start and item.current_end
            item.local_count = self._local_count(
                provider_key, item.instrument, item.timeframe, item.cursor_start, item.current_end
            )
            item.current_request_id = None
            item.request_kind = None
            item.dispatched_at = None
            item.last_result = (
                f"{item.scan_phase} availability confirmed: provider={count}, local={item.local_count}"
            )

            if item.scan_phase == "coarse":
                local_count = item.local_count
                if count <= 0 or earliest is None or latest is None:
                    item.ranges_unavailable += 1
                    self._advance_coarse_range(job, item)
                    return f"UNAVAILABLE|{count}|{local_count}"
                if local_count >= count:
                    item.ranges_skipped_existing += 1
                    self._advance_coarse_range(job, item)
                    return f"SKIP|{count}|{local_count}"
                # Provider data exists and local coverage is incomplete. Drill
                # into the tier's bounded fine ranges only after the broad probe
                # has established that the provider actually has history.
                assert item.coarse_start and item.coarse_end
                item.scan_phase = "fine"
                item.cursor_start = item.coarse_start
                item.fine_end = item.coarse_end
                item.current_end = min(
                    item.fine_end, item.cursor_start + self._active_tier(job, item).batch_span - self._step(item.timeframe)
                )
                item.available_count = 0
                item.local_count = 0
                item.last_result = (
                    f"coarse {item.timeframe} tier contains {count} provider candles; "
                    f"drilling into bounded ranges"
                )
                return f"DRILLDOWN|{count}|{local_count}"

            if count <= 0 or earliest is None or latest is None:
                item.ranges_unavailable += 1
                local_count = item.local_count
                self._advance_range(job, item)
                return f"UNAVAILABLE|{count}|{local_count}"
            if item.local_count >= count:
                item.ranges_skipped_existing += 1
                local_count = item.local_count
                self._advance_range(job, item)
                return f"SKIP|{count}|{local_count}"
            item.current_request_id = uuid.uuid4().hex
            item.request_kind = "backfill"
            item.status = "importing"
            item.dispatched_at = None
            item.dispatch_attempts = 0
            return f"BACKFILL|{count}|{item.local_count}"

    def batch_result(
        self,
        provider_key: str,
        request_id: str,
        bars_received: int,
        bars_inserted: int,
        completed: bool,
        *,
        unavailable: bool = False,
        error_code: int | None = None,
    ) -> str:
        with self._lock:
            job, item = self._active_item(provider_key, request_id)
            if item is None or job is None:
                return "IGNORED"
            item.last_error_code = error_code
            item.dispatched_at = None
            if unavailable:
                item.ranges_unavailable += 1
                item.retry_count = 0
                item.error = f"Confirmed history became unavailable (MT5 error {error_code})"
                item.last_result = item.error
                self._advance_range(job, item)
                return f"UNAVAILABLE|{max(0, bars_received)}|{max(0, bars_inserted)}|{max(0, bars_received - bars_inserted)}"
            if not completed:
                item.current_request_id = None
                item.request_kind = None
                item.retry_count += 1
                item.status = "retrying"
                item.error = f"Transient MT5 history failure; retry {item.retry_count}/3"
                item.last_result = item.error
                if item.retry_count >= 3:
                    item.ranges_unavailable += 1
                    item.retry_count = 0
                    self._advance_range(job, item)
                return f"ERROR|{max(0, bars_received)}|{max(0, bars_inserted)}|{max(0, bars_received - bars_inserted)}|{error_code or 0}"
            item.retry_count = 0
            item.error = None
            item.batches_completed += 1
            item.bars_received += max(0, bars_received)
            item.bars_inserted += max(0, bars_inserted)
            received = max(0, bars_received)
            inserted = max(0, min(bars_inserted, received))
            skipped = max(0, received - inserted)
            item.last_result = f"batch stored: received={received}, inserted={inserted}, skipped={skipped}"
            self._advance_range(job, item)
            return f"STORED|{received}|{inserted}|{skipped}"

    def _ensure_tier(self, job: FullHistoryJob, item: InstrumentProgress) -> None:
        tiers = self._tiers_by_job[job.job_id]
        while item.tier_index < len(tiers):
            tier = tiers[item.tier_index]
            if tier.end >= tier.start:
                item.timeframe = tier.timeframe
                item.tier_start = tier.start
                item.tier_end = tier.end
                if item.cursor_start is None:
                    item.cursor_start = tier.start
                    if tier.availability_span is not None:
                        item.scan_phase = "coarse"
                        item.coarse_start = tier.start
                        item.coarse_end = min(
                            tier.end, tier.start + tier.availability_span - self._step(tier.timeframe)
                        )
                        item.current_end = item.coarse_end
                    else:
                        item.scan_phase = "fine"
                        item.current_end = min(
                            tier.end, item.cursor_start + tier.batch_span - self._step(tier.timeframe)
                        )
                item.status = "queued"
                return
            item.tier_index += 1
        item.status = "completed"
        if self._on_instrument_completed is not None:
            self._on_instrument_completed(job.provider_key, item.instrument)

    def _advance_range(self, job: FullHistoryJob, item: InstrumentProgress) -> None:
        assert item.timeframe and item.current_end and item.tier_end
        item.cursor_start = item.current_end + self._step(item.timeframe)
        item.current_request_id = None
        item.request_kind = None
        item.dispatched_at = None
        item.dispatch_attempts = 0
        item.available_count = 0
        item.local_count = 0
        if item.scan_phase == "fine" and item.fine_end is not None and item.cursor_start > item.fine_end:
            self._advance_coarse_range(job, item)
            return
        if item.cursor_start > item.tier_end:
            self._complete_tier(item)
        else:
            tier = self._active_tier(job, item)
            item.current_end = min(
                item.tier_end, item.cursor_start + tier.batch_span - self._step(item.timeframe)
            )
        self._ensure_tier(job, item)

    def _advance_coarse_range(self, job: FullHistoryJob, item: InstrumentProgress) -> None:
        assert item.timeframe and item.coarse_end and item.tier_end
        next_start = item.coarse_end + self._step(item.timeframe)
        item.current_request_id = None
        item.request_kind = None
        item.dispatched_at = None
        item.dispatch_attempts = 0
        item.available_count = 0
        item.local_count = 0
        item.fine_end = None
        if next_start > item.tier_end:
            self._complete_tier(item)
            self._ensure_tier(job, item)
            return
        tier = self._active_tier(job, item)
        assert tier.availability_span is not None
        item.scan_phase = "coarse"
        item.coarse_start = next_start
        item.coarse_end = min(
            item.tier_end, next_start + tier.availability_span - self._step(item.timeframe)
        )
        item.cursor_start = item.coarse_start
        item.current_end = item.coarse_end
        self._ensure_tier(job, item)

    def _complete_tier(self, item: InstrumentProgress) -> None:
        item.tier_index += 1
        item.cursor_start = None
        item.current_end = None
        item.timeframe = None
        item.tier_start = None
        item.tier_end = None
        item.coarse_start = None
        item.coarse_end = None
        item.fine_end = None
        item.scan_phase = "fine"

    def _active_tier(self, job: FullHistoryJob, item: InstrumentProgress) -> HistoryTier:
        return self._tiers_by_job[job.job_id][item.tier_index]

    @staticmethod
    def _step(timeframe: str) -> timedelta:
        return {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[timeframe]

    def status(self, provider_key: str | None = None) -> dict[str, object]:
        with self._lock:
            jobs = [job for job in self._jobs.values() if provider_key is None or job.provider_key == provider_key]
            return {"jobs": [self._view(job) for job in sorted(jobs, key=lambda x: x.created_at, reverse=True)]}

    def _format_request(self, item: InstrumentProgress) -> str:
        assert item.current_request_id and item.timeframe and item.cursor_start and item.current_end
        command = "AVAILABILITY" if item.request_kind == "availability" else "BACKFILL"
        return "|".join((
            command,
            item.provider_symbol,
            item.timeframe,
            item.cursor_start.isoformat(),
            item.current_end.isoformat(),
            item.current_request_id,
        ))

    def _active_job(self, provider_key: str) -> FullHistoryJob | None:
        job_id = self._provider_job.get(provider_key)
        if not job_id:
            return None
        job = self._jobs[job_id]
        return job if job.status == "running" else None

    def _active_item(self, provider_key: str, request_id: str) -> tuple[FullHistoryJob | None, InstrumentProgress | None]:
        job = self._active_job(provider_key)
        if job is None or job.active_index >= len(job.instruments):
            return None, None
        item = job.instruments[job.active_index]
        return (job, item) if item.current_request_id == request_id else (None, None)

    def _advance_completed(self, job: FullHistoryJob) -> None:
        while job.active_index < len(job.instruments) and job.instruments[job.active_index].status == "completed":
            job.active_index += 1
        if job.active_index >= len(job.instruments):
            job.status = "completed"
            job.completed_at = self._now_factory()

    @staticmethod
    def _view(job: FullHistoryJob) -> dict[str, object]:
        return {
            "job_id": job.job_id,
            "provider_key": job.provider_key,
            "status": job.status,
            "created_at": job.created_at.isoformat(),
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "instruments": [
                {
                    "provider_symbol": item.provider_symbol,
                    "instrument": item.instrument,
                    "status": item.status,
                    "timeframe": item.timeframe,
                    "tier_index": item.tier_index,
                    "tier_start": item.tier_start.isoformat() if item.tier_start else None,
                    "tier_end": item.tier_end.isoformat() if item.tier_end else None,
                    "cursor_start": item.cursor_start.isoformat() if item.cursor_start else None,
                    "current_end": item.current_end.isoformat() if item.current_end else None,
                    "available_count": item.available_count,
                    "local_count": item.local_count,
                    "earliest_available": item.earliest_available.isoformat() if item.earliest_available else None,
                    "latest_available": item.latest_available.isoformat() if item.latest_available else None,
                    "batches_completed": item.batches_completed,
                    "ranges_probed": item.ranges_probed,
                    "ranges_skipped_existing": item.ranges_skipped_existing,
                    "ranges_unavailable": item.ranges_unavailable,
                    "bars_available": item.bars_available,
                    "bars_received": item.bars_received,
                    "bars_inserted": item.bars_inserted,
                    "error": item.error,
                    "retry_count": item.retry_count,
                    "last_error_code": item.last_error_code,
                    "request_kind": item.request_kind,
                    "request_id": item.current_request_id,
                    "dispatched_at": item.dispatched_at.isoformat() if item.dispatched_at else None,
                    "dispatch_attempts": item.dispatch_attempts,
                    "last_result": item.last_result,
                    "scan_phase": item.scan_phase,
                    "coarse_start": item.coarse_start.isoformat() if item.coarse_start else None,
                    "coarse_end": item.coarse_end.isoformat() if item.coarse_end else None,
                    "fine_end": item.fine_end.isoformat() if item.fine_end else None,
                    "coarse_ranges_probed": item.coarse_ranges_probed,
                    "fine_ranges_probed": item.fine_ranges_probed,
                }
                for item in job.instruments
            ],
        }
