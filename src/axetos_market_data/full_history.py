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
    ) -> None:
        self._local_count = local_count
        self._lock = threading.RLock()
        self._on_instrument_completed = on_instrument_completed
        self._pressure_probe = pressure_probe
        self._now_factory = now_factory
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
        return [
            HistoryTier("1m", recent_start, now, timedelta(days=1)),
            HistoryTier("1h", hourly_start, recent_start - timedelta(hours=1), timedelta(days=30)),
            HistoryTier("1d", deep_start, hourly_start - timedelta(days=1), timedelta(days=365)),
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
                # Availability probes are read-only MT5 queries and must never be
                # blocked by SQLite/live-ingestion pressure. Only actual historical
                # downloads are throttled because they result in database writes.
                if (
                    item.request_kind == "backfill"
                    and self._pressure_probe is not None
                    and not self._pressure_probe()
                ):
                    item.status = "throttled"
                    return ""
                return self._format_request(item)
            self._ensure_tier(job, item)
            if item.status == "completed":
                self._advance_completed(job)
                return self.next_request(provider_key) if job.status == "running" else ""
            item.current_request_id = uuid.uuid4().hex
            item.request_kind = "availability"
            item.status = "probing"
            return self._format_request(item)

    def availability_result(
        self,
        provider_key: str,
        request_id: str,
        *,
        earliest: datetime | None,
        latest: datetime | None,
        count: int,
    ) -> None:
        with self._lock:
            job, item = self._active_item(provider_key, request_id)
            if item is None or job is None:
                return
            item.ranges_probed += 1
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
            if count <= 0 or earliest is None or latest is None:
                item.ranges_unavailable += 1
                self._advance_range(job, item)
                return
            if item.local_count >= count:
                item.ranges_skipped_existing += 1
                self._advance_range(job, item)
                return
            item.current_request_id = uuid.uuid4().hex
            item.request_kind = "backfill"
            item.status = "importing"

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
    ) -> None:
        with self._lock:
            job, item = self._active_item(provider_key, request_id)
            if item is None or job is None:
                return
            item.last_error_code = error_code
            if unavailable:
                item.ranges_unavailable += 1
                item.retry_count = 0
                item.error = f"Confirmed history became unavailable (MT5 error {error_code})"
                self._advance_range(job, item)
                return
            if not completed:
                item.current_request_id = None
                item.request_kind = None
                item.retry_count += 1
                item.status = "retrying"
                item.error = f"Transient MT5 history failure; retry {item.retry_count}/3"
                if item.retry_count >= 3:
                    item.ranges_unavailable += 1
                    item.retry_count = 0
                    self._advance_range(job, item)
                return
            item.retry_count = 0
            item.error = None
            item.batches_completed += 1
            item.bars_received += max(0, bars_received)
            item.bars_inserted += max(0, bars_inserted)
            self._advance_range(job, item)

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
                item.current_end = min(tier.end, item.cursor_start + tier.batch_span - self._step(tier.timeframe))
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
        item.available_count = 0
        item.local_count = 0
        if item.cursor_start > item.tier_end:
            item.tier_index += 1
            item.cursor_start = None
            item.current_end = None
            item.timeframe = None
            item.tier_start = None
            item.tier_end = None
        self._ensure_tier(job, item)

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
                }
                for item in job.instruments
            ],
        }
