from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from .clock import server_now


@dataclass(slots=True)
class InstrumentProgress:
    provider_symbol: str
    instrument: str
    status: str = "queued"
    earliest_available: datetime | None = None
    cursor_end: datetime | None = None
    batches_completed: int = 0
    bars_received: int = 0
    bars_inserted: int = 0
    current_request_id: str | None = None
    error: str | None = None
    retry_count: int = 0
    unavailable_ranges: int = 0
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
    """Server-controlled, low-priority MT5 full-history coordinator.

    The bridge polls for one small request at a time. Live ticks are sent before the
    bridge polls this queue, so history work yields naturally to normal ingestion.
    """

    def __init__(
        self,
        earliest_existing: Callable[[str, str], datetime | None],
        batch_days: int = 1,
        on_instrument_completed: Callable[[str, str], None] | None = None,
        pressure_probe: Callable[[], bool] | None = None,
    ) -> None:
        self._earliest_existing = earliest_existing
        self._batch_days = max(1, batch_days)
        self._lock = threading.RLock()
        self._on_instrument_completed = on_instrument_completed
        self._pressure_probe = pressure_probe
        self._jobs: dict[str, FullHistoryJob] = {}
        self._provider_job: dict[str, str] = {}

    def set_pressure_probe(self, pressure_probe: Callable[[], bool] | None) -> None:
        with self._lock:
            self._pressure_probe = pressure_probe

    def start(self, provider_key: str, symbols: list[tuple[str, str]]) -> dict[str, object]:
        with self._lock:
            existing_id = self._provider_job.get(provider_key)
            if existing_id and self._jobs[existing_id].status == "running":
                raise RuntimeError("A full-history backfill is already running for this provider")
            job = FullHistoryJob(
                job_id=uuid.uuid4().hex,
                provider_key=provider_key,
                created_at=server_now(),
                instruments=[InstrumentProgress(symbol, instrument) for symbol, instrument in symbols],
            )
            self._jobs[job.job_id] = job
            self._provider_job[provider_key] = job.job_id
            return self._view(job)

    def next_request(self, provider_key: str) -> str:
        with self._lock:
            if self._pressure_probe is not None and not self._pressure_probe():
                return ""
            job_id = self._provider_job.get(provider_key)
            if not job_id:
                return ""
            job = self._jobs[job_id]
            if job.status != "running":
                return ""
            self._advance_completed(job)
            if job.status != "running":
                return ""
            item = job.instruments[job.active_index]
            if item.current_request_id:
                return self._format_request(item)
            item.current_request_id = uuid.uuid4().hex
            if item.earliest_available is None:
                item.status = "discovering"
            else:
                item.status = "importing"
                if item.cursor_end is None:
                    existing = self._earliest_existing(job.provider_key, item.instrument)
                    item.cursor_end = (existing - timedelta(minutes=1)) if existing else server_now()
            return self._format_request(item)

    def availability_result(self, provider_key: str, request_id: str, earliest: datetime | None) -> None:
        with self._lock:
            item = self._active_item(provider_key, request_id)
            if item is None:
                return
            item.current_request_id = None
            if earliest is None:
                item.status = "completed"
                return
            item.earliest_available = earliest
            existing = self._earliest_existing(provider_key, item.instrument)
            item.cursor_end = (existing - timedelta(minutes=1)) if existing else server_now()
            if item.cursor_end < earliest:
                item.status = "completed"

    def batch_result(self, provider_key: str, request_id: str, bars_received: int, bars_inserted: int, completed: bool, *, unavailable: bool = False, error_code: int | None = None) -> None:
        with self._lock:
            item = self._active_item(provider_key, request_id)
            if item is None:
                return
            item.last_error_code = error_code
            if unavailable:
                item.unavailable_ranges += 1
                item.retry_count = 0
                item.error = f"Provider has no M1 history for requested range (MT5 error {error_code})" if error_code else "Provider has no M1 history for requested range"
                self._advance_batch_cursor(provider_key, item)
                return
            if not completed:
                item.current_request_id = None
                item.retry_count += 1
                item.status = "retrying"
                item.error = f"Transient MT5 history failure (error {error_code}); retry {item.retry_count}/3" if error_code else f"Transient MT5 history failure; retry {item.retry_count}/3"
                if item.retry_count >= 3:
                    item.unavailable_ranges += 1
                    item.retry_count = 0
                    item.error = f"Skipped range after 3 failed attempts (last MT5 error {error_code})" if error_code else "Skipped range after 3 failed attempts"
                    self._advance_batch_cursor(provider_key, item)
                return
            item.retry_count = 0
            item.error = None
            item.batches_completed += 1
            item.bars_received += max(0, bars_received)
            item.bars_inserted += max(0, bars_inserted)
            self._advance_batch_cursor(provider_key, item)

    def _advance_batch_cursor(self, provider_key: str, item: InstrumentProgress) -> None:
        assert item.cursor_end is not None
        batch_start = max(item.earliest_available or item.cursor_end, item.cursor_end - timedelta(days=self._batch_days) + timedelta(minutes=1))
        item.cursor_end = batch_start - timedelta(minutes=1)
        item.current_request_id = None
        if item.earliest_available is None or item.cursor_end < item.earliest_available:
            item.status = "completed"
            if self._on_instrument_completed is not None:
                self._on_instrument_completed(provider_key, item.instrument)
        else:
            item.status = "importing"

    def status(self, provider_key: str | None = None) -> dict[str, object]:
        with self._lock:
            jobs = [job for job in self._jobs.values() if provider_key is None or job.provider_key == provider_key]
            return {"jobs": [self._view(job) for job in sorted(jobs, key=lambda x: x.created_at, reverse=True)]}

    def _format_request(self, item: InstrumentProgress) -> str:
        assert item.current_request_id
        if item.earliest_available is None:
            return f"AVAILABILITY|{item.provider_symbol}|1m|{item.current_request_id}"
        assert item.cursor_end is not None
        start = max(item.earliest_available, item.cursor_end - timedelta(days=self._batch_days) + timedelta(minutes=1))
        return "|".join(("BACKFILL", item.provider_symbol, "1m", start.isoformat(), item.cursor_end.isoformat(), item.current_request_id))

    def _active_item(self, provider_key: str, request_id: str) -> InstrumentProgress | None:
        job_id = self._provider_job.get(provider_key)
        if not job_id:
            return None
        job = self._jobs[job_id]
        if job.status != "running" or job.active_index >= len(job.instruments):
            return None
        item = job.instruments[job.active_index]
        return item if item.current_request_id == request_id else None

    def _advance_completed(self, job: FullHistoryJob) -> None:
        while job.active_index < len(job.instruments) and job.instruments[job.active_index].status == "completed":
            job.active_index += 1
        if job.active_index >= len(job.instruments):
            job.status = "completed"
            job.completed_at = server_now()

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
                    "earliest_available": item.earliest_available.isoformat() if item.earliest_available else None,
                    "cursor_end": item.cursor_end.isoformat() if item.cursor_end else None,
                    "batches_completed": item.batches_completed,
                    "bars_received": item.bars_received,
                    "bars_inserted": item.bars_inserted,
                    "error": item.error,
                    "retry_count": item.retry_count,
                    "unavailable_ranges": item.unavailable_ranges,
                    "last_error_code": item.last_error_code,
                }
                for item in job.instruments
            ],
        }
