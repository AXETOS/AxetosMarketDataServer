from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Callable

from .clock import server_now


@dataclass(frozen=True, slots=True)
class PlannedRange:
    timeframe: str
    start: datetime
    end: datetime
    depth: int


@dataclass(slots=True)
class InstrumentProgress:
    provider_symbol: str
    instrument: str
    provider_key: str = ""
    status: str = "queued"
    phase: str = "discovery"
    timeframe: str | None = None
    cursor_start: datetime | None = None
    current_end: datetime | None = None
    current_request_id: str | None = None
    request_kind: str | None = None
    dispatched_at: datetime | None = None
    dispatch_attempts: int = 0
    retry_count: int = 0
    last_result: str | None = None
    error: str | None = None
    last_error_code: int | None = None
    available_count: int = 0
    local_count: int = 0
    earliest_available: datetime | None = None
    latest_available: datetime | None = None
    discovery_index: int = 0
    discovery_complete: bool = False
    discovery_boundaries: dict[str, datetime | None] = field(default_factory=dict)
    discovery_latest: dict[str, datetime | None] = field(default_factory=dict)
    discovery_counts: dict[str, int] = field(default_factory=dict)
    discovery_local_counts: dict[str, int] = field(default_factory=dict)
    discovery_complete_timeframes: set[str] = field(default_factory=set)
    planning_queue: list[PlannedRange] = field(default_factory=list)
    missing_ranges: list[PlannedRange] = field(default_factory=list)
    download_index: int = 0
    bad_ranges: list[dict[str, object]] = field(default_factory=list)
    ranges_probed: int = 0
    ranges_skipped_existing: int = 0
    ranges_unavailable: int = 0
    bars_available: int = 0
    bars_received: int = 0
    bars_inserted: int = 0
    batches_completed: int = 0
    received_by_timeframe: dict[str, int] = field(default_factory=dict)
    stored_by_timeframe: dict[str, int] = field(default_factory=dict)
    skipped_by_timeframe: dict[str, int] = field(default_factory=dict)
    ranges_by_timeframe: dict[str, int] = field(default_factory=dict)
    planned_by_timeframe: dict[str, int] = field(default_factory=dict)
    attempted_by_timeframe: dict[str, int] = field(default_factory=dict)
    unavailable_by_timeframe: dict[str, int] = field(default_factory=dict)
    # Compatibility/status fields retained for clients and older tests.
    tier_index: int = 0
    tier_start: datetime | None = None
    tier_end: datetime | None = None
    scan_phase: str = "discovery"
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
    phase: str = "discovery"
    instruments: list[InstrumentProgress] = field(default_factory=list)
    active_index: int = 0
    completed_at: datetime | None = None
    repair_started_at: datetime | None = None
    repair_completed_at: datetime | None = None
    recent_refresh_started_at: datetime | None = None
    recent_refresh_completed_at: datetime | None = None
    recent_pass_started_at: datetime | None = None
    workflow: str = "full"
    repair_pass: int = 0


class FullHistoryBackfillManager:
    """Simple fixed-plan MT5 source-history coordinator.

    Each instrument downloads official M1 month-by-month, H1 year-by-year, and D1
    across the ten-year window. Unavailable ranges are marked bad and skipped.
    Exactly one provider history request remains in flight at a time.
    """

    _DISCOVERY_TIMEFRAMES = ("1m", "1h", "1d")
    _LEAF_DEPTH = {"1d": 2, "1h": 3, "1m": 4}  # year/month, +day, +hour

    def __init__(
        self,
        local_count: Callable[[str, str, str, datetime, datetime], int],
        local_bounds: Callable[[str, str, str, datetime, datetime], tuple[datetime | None, datetime | None]] | None = None,
        *,
        on_instrument_completed: Callable[[str, str, dict[str, object]], None] | None = None,
        on_download_completed: Callable[[str, str, list[str]], None] | None = None,
        on_event: Callable[[str, str, str, dict[str, object]], None] | None = None,
        bad_range_recorder: Callable[[str, str, str, datetime, datetime, str, int | None], None] | None = None,
        bad_range_probe: Callable[[str, str, str, datetime, datetime], bool] | None = None,
        pressure_probe: Callable[[], bool] | None = None,
        now_factory: Callable[[], datetime] = server_now,
        request_lease: timedelta = timedelta(seconds=30),
    ) -> None:
        self._local_count = local_count
        self._local_bounds = local_bounds
        self._on_instrument_completed = on_instrument_completed
        self._on_download_completed = on_download_completed
        self._on_event = on_event
        self._bad_range_recorder = bad_range_recorder
        self._bad_range_probe = bad_range_probe
        self._pressure_probe = pressure_probe
        self._now_factory = now_factory
        self._request_lease = request_lease
        self._lock = threading.RLock()
        self._jobs: dict[str, FullHistoryJob] = {}
        self._provider_job: dict[str, str] = {}

    def set_pressure_probe(self, pressure_probe: Callable[[], bool] | None) -> None:
        with self._lock:
            self._pressure_probe = pressure_probe

    def start(self, provider_key: str, symbols: list[tuple[str, str]]) -> dict[str, object]:
        with self._lock:
            existing_id = self._provider_job.get(provider_key)
            if existing_id and self._jobs[existing_id].status in {"running", "repairing"}:
                raise RuntimeError("A full-history backfill is already running for this provider")
            now = self._now_factory().astimezone(UTC).replace(second=0, microsecond=0)
            instruments: list[InstrumentProgress] = []
            for symbol, instrument in symbols:
                item = InstrumentProgress(symbol, instrument, provider_key)
                item.phase = "discovery"
                item.scan_phase = "discovery"
                item.discovery_complete = False
                item.status = "queued"
                instruments.append(item)
            job = FullHistoryJob(
                job_id=uuid.uuid4().hex,
                provider_key=provider_key,
                created_at=now,
                phase="discovery",
                instruments=instruments,
            )
            self._jobs[job.job_id] = job
            self._provider_job[provider_key] = job.job_id
            self._emit("backfill.download_started", "Ten-year MT5 source download started", provider_key,
                       {"job_id": job.job_id, "instruments": len(symbols),
                        "timeframes": ["1m", "1h", "1d"],
                        "window_years": 10})
            return self._view(job)

    def start_targeted(
        self, provider_key: str, symbols: list[tuple[str, str, list[PlannedRange]]],
        *, workflow: str = "recent_m1",
    ) -> dict[str, object]:
        """Start a planning-free targeted download job from already detected gaps."""
        with self._lock:
            existing_id = self._provider_job.get(provider_key)
            if existing_id and self._jobs[existing_id].status in {"running", "repairing"}:
                raise RuntimeError("A history operation is already running for this provider")
            items: list[InstrumentProgress] = []
            for provider_symbol, instrument, ranges in symbols:
                merged = self._merge_ranges(ranges)
                item = InstrumentProgress(provider_symbol, instrument, provider_key)
                item.phase = "download"
                item.scan_phase = "download"
                item.discovery_complete = True
                item.missing_ranges = merged
                item.status = "queued" if merged else "completed"
                items.append(item)
            job = FullHistoryJob(
                job_id=uuid.uuid4().hex, provider_key=provider_key,
                created_at=self._now_factory(), instruments=items,
                phase="download", workflow=workflow,
                recent_refresh_started_at=self._now_factory() if workflow == "recent_m1" else None,
                recent_pass_started_at=self._now_factory() if workflow == "recent_m1" else None,
            )
            self._jobs[job.job_id] = job
            self._provider_job[provider_key] = job.job_id
            self._emit("repair.recent_m1_started", "Recent M1 gap recovery started", provider_key, {
                "job_id": job.job_id, "instruments": len(items),
                "ranges": sum(len(item.missing_ranges) for item in items),
                "workflow": workflow,
            })
            return self._view(job)

    def resume_targeted_after_repair(
        self, provider_key: str, job_id: str,
        ranges_by_instrument: dict[str, list[PlannedRange]],
    ) -> bool:
        """Append a single recent-M1 recovery pass to a full backfill before completion."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.provider_key != provider_key or job.status != "repairing":
                return False
            job.repair_pass += 1
            pass_started = self._now_factory()
            if job.recent_refresh_started_at is None:
                job.recent_refresh_started_at = pass_started
            job.recent_pass_started_at = pass_started
            job.status = "running"
            job.phase = "recent_m1_download"
            job.active_index = 0
            total = 0
            for item in job.instruments:
                ranges = self._merge_ranges(ranges_by_instrument.get(item.instrument, []))
                item.phase = "download"
                item.scan_phase = "download"
                item.status = "queued" if ranges else "completed"
                item.missing_ranges = ranges
                item.download_index = 0
                item.current_request_id = None
                item.request_kind = None
                item.dispatched_at = None
                item.retry_count = 0
                item.bars_received = 0
                item.bars_inserted = 0
                item.batches_completed = 0
                total += len(ranges)
            self._emit("repair.recent_m1_download_started", "Post-backfill recent M1 gap download started", provider_key, {
                "job_id": job.job_id, "ranges": total, "repair_pass": job.repair_pass,
            })
            return True


    def instrument_verified(
        self, provider_key: str, job_id: str, instrument: str,
        details: dict[str, object], *, error: str | None = None,
    ) -> bool:
        """Finish one instrument verification and allow the next instrument to dispatch."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.provider_key != provider_key or job.status != "running":
                return False
            if job.active_index >= len(job.instruments):
                return False
            item = job.instruments[job.active_index]
            if item.instrument != instrument or item.status != "verifying":
                return False
            if error:
                item.error = error
                item.last_result = f"instrument verification failed: {error}"
                self._mark_bad(
                    item,
                    PlannedRange(
                        item.timeframe or "1m",
                        item.missing_ranges[0].start if item.missing_ranges else self._now_factory(),
                        item.missing_ranges[-1].end if item.missing_ranges else self._now_factory(),
                        self._LEAF_DEPTH.get(item.timeframe or "1m", 4),
                    ),
                    "instrument_verification_failed", None,
                )
                self._emit("repair.instrument_failed", "Instrument refresh verification failed", provider_key, {
                    "job_id": job_id, "instrument": instrument, "error": error, **details,
                })
            else:
                item.error = None
                item.last_result = "instrument refresh verified"
                self._emit("repair.instrument_completed", "Instrument refresh and repair verified", provider_key, {
                    "job_id": job_id, "instrument": instrument, **details,
                })
            item.phase = "completed"
            item.status = "completed"
            return True

    def job_context(self, provider_key: str, job_id: str) -> dict[str, object] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.provider_key != provider_key:
                return None
            return {
                "workflow": job.workflow, "repair_pass": job.repair_pass,
                "status": job.status, "phase": job.phase,
                "created_at": job.created_at,
                "repair_started_at": job.repair_started_at,
                "recent_refresh_started_at": job.recent_refresh_started_at,
                "recent_refresh_completed_at": job.recent_refresh_completed_at,
                "recent_pass_started_at": job.recent_pass_started_at,
            }

    def request_context(self, provider_key: str, request_id: str | None) -> dict[str, object] | None:
        """Return immutable context for the provider's active MT5 history request."""
        if not request_id:
            return None
        with self._lock:
            job, item = self._active_item(provider_key, request_id)
            if job is None or item is None:
                return None
            return {
                "workflow": job.workflow,
                "phase": job.phase,
                "repair_pass": job.repair_pass,
                "instrument": item.instrument,
                "timeframe": item.timeframe,
                "from_utc": item.cursor_start,
                "to_utc": item.current_end,
            }

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
                now = self._now_factory()
                if item.dispatched_at is not None and now - item.dispatched_at < self._request_lease:
                    return ""
                item.dispatched_at = now
                item.dispatch_attempts += 1
                return self._format_request(item)

            self._prepare_next(job, item)
            if item.status == "completed":
                self._advance_completed(job)
                return self.next_request(provider_key) if job.status == "running" else ""
            if item.current_request_id is None:
                return ""
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
            assert item.timeframe and item.cursor_start and item.current_end
            timeframe, start, end = item.timeframe, item.cursor_start, item.current_end
            provider_count = max(0, count)
            local_count = self._local_count(provider_key, item.instrument, timeframe, start, end)
            item.ranges_probed += 1
            item.available_count = provider_count
            item.local_count = local_count
            item.earliest_available = earliest
            item.latest_available = latest
            item.bars_available += provider_count
            self._clear_request(item)

            if item.phase == "discovery":
                return self._handle_discovery(job, item, timeframe, start, end, earliest, latest,
                                              provider_count, local_count)
            return self._handle_planning(job, item, timeframe, start, end, earliest, latest,
                                         provider_count, local_count)

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
            assert item.timeframe and item.cursor_start and item.current_end
            received = max(0, bars_received)
            inserted = max(0, min(bars_inserted, received))
            skipped = max(0, received - inserted)
            current = PlannedRange(item.timeframe, item.cursor_start, item.current_end,
                                   self._LEAF_DEPTH[item.timeframe])
            item.last_error_code = error_code

            if unavailable:
                timeframe = item.timeframe
                item.unavailable_by_timeframe[timeframe] = item.unavailable_by_timeframe.get(timeframe, 0) + 1
                item.ranges_by_timeframe[timeframe] = item.ranges_by_timeframe.get(timeframe, 0) + 1
                self._mark_bad(item, current, "provider_unavailable", error_code)
                self._advance_download(item)
                return f"UNAVAILABLE|{received}|{inserted}|{skipped}"

            if not completed:
                item.retry_count += 1
                item.error = f"Transient MT5 history failure; retry {item.retry_count}/3"
                item.last_result = item.error
                self._clear_request(item)
                if item.retry_count >= 3:
                    self._mark_bad(item, current, "retry_exhausted", error_code)
                    item.retry_count = 0
                    self._advance_download(item)
                return f"ERROR|{received}|{inserted}|{skipped}|{error_code or 0}"

            item.retry_count = 0
            item.error = None
            item.batches_completed += 1
            item.bars_received += received
            item.bars_inserted += inserted
            timeframe = item.timeframe
            item.received_by_timeframe[timeframe] = item.received_by_timeframe.get(timeframe, 0) + received
            item.stored_by_timeframe[timeframe] = item.stored_by_timeframe.get(timeframe, 0) + inserted
            item.skipped_by_timeframe[timeframe] = item.skipped_by_timeframe.get(timeframe, 0) + skipped
            item.ranges_by_timeframe[timeframe] = item.ranges_by_timeframe.get(timeframe, 0) + 1
            item.last_result = f"download stored: received={received}, inserted={inserted}, skipped={skipped}"
            self._emit("backfill.download_progress", "Missing history range stored", provider_key, {
                "job_id": job.job_id, "instrument": item.instrument, "timeframe": item.timeframe,
                "from_utc": item.cursor_start.isoformat(), "to_utc": item.current_end.isoformat(),
                "received": received, "stored": inserted, "skipped": skipped,
                "completed_ranges": item.download_index + 1, "planned_ranges": len(item.missing_ranges),
            })
            self._advance_download(item)
            return f"STORED|{received}|{inserted}|{skipped}"

    def _handle_discovery(
        self, job: FullHistoryJob, item: InstrumentProgress, timeframe: str,
        start: datetime, end: datetime, earliest: datetime | None, latest: datetime | None,
        provider_count: int, local_count: int,
    ) -> str:
        boundary = earliest if provider_count > 0 and earliest is not None else None
        available_latest = latest if provider_count > 0 and latest is not None else None
        item.discovery_boundaries[timeframe] = boundary
        item.discovery_latest[timeframe] = available_latest
        item.discovery_counts[timeframe] = provider_count
        item.discovery_local_counts[timeframe] = local_count
        item.discovery_index += 1
        item.last_result = (
            f"availability {timeframe}: count={provider_count}, "
            f"earliest={boundary.isoformat() if boundary else 'unavailable'}, "
            f"latest={available_latest.isoformat() if available_latest else 'unavailable'}"
        )

        if item.discovery_index < len(self._DISCOVERY_TIMEFRAMES):
            self._configure_discovery(item)
            return (f"AVAILABLE|{provider_count}|{boundary.isoformat()}|{available_latest.isoformat()}"
                    if boundary and available_latest else f"UNAVAILABLE|{provider_count}|{local_count}")

        item.discovery_complete = True
        now = self._now_factory().astimezone(UTC).replace(second=0, microsecond=0)
        item.missing_ranges = self._source_ranges_from_availability(
            now, item.discovery_boundaries, item.discovery_latest
        )
        item.planned_by_timeframe = self._count_by_timeframe(item.missing_ranges)
        item.phase = "download"
        item.scan_phase = "download"
        item.status = "queued"
        job.phase = "download"

        availability_text = ", ".join(
            f"{tf.upper()} earliest="
            f"{item.discovery_boundaries.get(tf).isoformat() if item.discovery_boundaries.get(tf) else 'unavailable'}"
            for tf in self._DISCOVERY_TIMEFRAMES
        )
        self._emit(
            "backfill.instrument_availability_checked",
            f"MT5 history availability for {item.instrument}: {availability_text}",
            job.provider_key,
            {
                "job_id": job.job_id,
                "instrument": item.instrument,
                "provider_symbol": item.provider_symbol,
                "earliest_by_timeframe": {
                    key: value.isoformat() if value else None
                    for key, value in item.discovery_boundaries.items()
                },
                "latest_by_timeframe": {
                    key: value.isoformat() if value else None
                    for key, value in item.discovery_latest.items()
                },
                "provider_counts": dict(item.discovery_counts),
            },
        )
        self._emit(
            "backfill.instrument_plan_created",
            f"History plan for {item.instrument}: "
            f"M1 ranges={item.planned_by_timeframe.get('1m', 0)}, "
            f"H1 ranges={item.planned_by_timeframe.get('1h', 0)}, "
            f"D1 ranges={item.planned_by_timeframe.get('1d', 0)}",
            job.provider_key,
            {
                "job_id": job.job_id,
                "instrument": item.instrument,
                "provider_symbol": item.provider_symbol,
                "planned_ranges": dict(item.planned_by_timeframe),
                "from_utc": item.missing_ranges[0].start.isoformat() if item.missing_ranges else None,
                "to_utc": item.missing_ranges[-1].end.isoformat() if item.missing_ranges else None,
            },
        )
        if not item.missing_ranges:
            item.error = "MT5 reported no M1, H1, or D1 history in the ten-year window"
        return "DISCOVERY_COMPLETE"

    def _handle_planning(
        self, job: FullHistoryJob, item: InstrumentProgress, timeframe: str,
        start: datetime, end: datetime, earliest: datetime | None, latest: datetime | None,
        provider_count: int, local_count: int,
    ) -> str:
        current = PlannedRange(timeframe, start, end, self._current_depth(item))
        if provider_count <= 0 or earliest is None or latest is None:
            self._mark_bad(item, current, "no_provider_history", None)
            item.status = "queued"
            return f"BAD|{provider_count}|{local_count}"

        if self._coverage_complete(job.provider_key, item.instrument, timeframe,
                                   start, end, earliest, latest, provider_count, local_count):
            item.ranges_skipped_existing += 1
            item.last_result = f"planning range complete: provider={provider_count}, local={local_count}"
            item.status = "queued"
            return f"PLAN_COMPLETE|{provider_count}|{local_count}"

        if current.depth >= self._LEAF_DEPTH[timeframe]:
            item.missing_ranges.append(current)
            item.last_result = f"planned missing leaf: {timeframe} {start.isoformat()}..{end.isoformat()}"
            item.status = "queued"
            return f"PLAN_MISSING|{provider_count}|{local_count}"

        children = self._split(current)
        if not children:
            item.missing_ranges.append(PlannedRange(timeframe, start, end, self._LEAF_DEPTH[timeframe]))
            item.status = "queued"
            return f"PLAN_MISSING|{provider_count}|{local_count}"
        # Depth-first planning finds a precise plan while retaining bounded memory.
        item.planning_queue[0:0] = children
        item.last_result = f"planning split into {len(children)} smaller ranges"
        item.status = "queued"
        return f"PLAN_SPLIT|{provider_count}|{local_count}|{len(children)}"

    def _prepare_next(self, job: FullHistoryJob, item: InstrumentProgress) -> None:
        if item.current_request_id is not None or item.status == "completed":
            return
        if item.phase == "discovery":
            if item.timeframe is None:
                self._configure_discovery(item)
            self._new_request(item, "discovery")
            return

        if item.phase == "planning":
            while item.planning_queue:
                current = item.planning_queue.pop(0)
                if self._bad_range_probe is not None and self._bad_range_probe(
                    job.provider_key, item.instrument, current.timeframe, current.start, current.end
                ):
                    self._mark_bad(item, current, "previously_marked_bad", None, persist=False)
                    continue
                self._set_range(item, current)
                item.tier_index = current.depth
                self._new_request(item, "availability")
                return
            item.missing_ranges = self._merge_ranges(item.missing_ranges)
            item.phase = "download"
            item.scan_phase = "download"
            job.phase = "download"
            self._emit("backfill.plan_created", "Missing-history download plan created", job.provider_key, {
                "job_id": job.job_id, "instrument": item.instrument,
                "ranges": len(item.missing_ranges), "bad_ranges": len(item.bad_ranges),
                "missing_by_timeframe": self._count_by_timeframe(item.missing_ranges),
            })
            if item.missing_ranges:
                self._emit("backfill.download_started", "Missing-history download started", job.provider_key, {
                    "job_id": job.job_id, "instrument": item.instrument,
                    "ranges": len(item.missing_ranges),
                })
            self._prepare_next(job, item)
            return

        if item.phase == "download":
            if item.download_index < len(item.missing_ranges):
                current = item.missing_ranges[item.download_index]
                self._set_range(item, current)
                item.attempted_by_timeframe[current.timeframe] = item.attempted_by_timeframe.get(current.timeframe, 0) + 1
                self._new_request(item, "backfill")
                return
            details = {
                "job_id": job.job_id, "workflow": job.workflow, "repair_pass": job.repair_pass,
                "phase": job.phase, "timeframe": item.timeframe,
                "from_utc": item.missing_ranges[0].start if item.missing_ranges else None,
                "to_utc": item.missing_ranges[-1].end if item.missing_ranges else None,
                "ranges": len(item.missing_ranges), "candles_received": item.bars_received,
                "candles_stored": item.bars_inserted,
                "candles_skipped": max(0, item.bars_received - item.bars_inserted),
                "planned_ranges": len(item.missing_ranges), "bad_ranges": len(item.bad_ranges),
            }
            if job.workflow == "startup_m1":
                requested = sum(
                    max(0, int((value.end - value.start).total_seconds() // 60) + 1)
                    for value in item.missing_ranges
                    if value.timeframe == "1m"
                )
                received = int(item.received_by_timeframe.get("1m", item.bars_received))
                stored = int(item.stored_by_timeframe.get("1m", item.bars_inserted))
                skipped = max(0, received - stored)
                result = "success" if received == requested and stored == received and skipped == 0 else "partial"
                self._emit(
                    "startup.m1_refresh_completed",
                    f"{job.provider_key} · {item.instrument} · startup M1 refresh\n"
                    f"requested={requested}, received={received}, stored={stored}, "
                    f"skipped={skipped}, result={result}",
                    job.provider_key,
                    {
                        "instrument": item.instrument, "workflow": job.workflow,
                        "requested": requested, "received": received, "stored": stored,
                        "skipped": skipped, "result": result, **details,
                    },
                )
                if self._on_instrument_completed is not None:
                    item.phase = "verifying"
                    item.status = "verifying"
                    self._on_instrument_completed(job.provider_key, item.instrument, details)
                else:
                    item.phase = "completed"
                    item.status = "completed"
                return

            timeframe_summary = self._timeframe_summary(item)
            summary_text = self._format_timeframe_summary(timeframe_summary)
            self._emit(
                "backfill.instrument_download_summary",
                f"MT5 history received for {item.instrument}: {summary_text}",
                job.provider_key,
                {"instrument": item.instrument, "timeframes": timeframe_summary, **details},
            )
            empty_timeframes = [name for name in ("1m", "1h", "1d") if timeframe_summary[name]["received"] == 0]
            all_empty = len(empty_timeframes) == 3
            if empty_timeframes:
                self._emit(
                    "backfill.instrument_history_incomplete",
                    f"History missing for {item.instrument}: no bars returned for {', '.join(empty_timeframes)}",
                    job.provider_key,
                    {"instrument": item.instrument, "missing_timeframes": empty_timeframes,
                     "timeframes": timeframe_summary, **details},
                )
            if all_empty:
                item.error = "No MT5 history returned for M1, H1, or D1"
                item.phase = "completed"
                item.status = "completed"
                self._emit(
                    "backfill.instrument_failed",
                    f"Full-history download failed for {item.instrument}: MT5 returned no M1, H1, or D1 bars; candle repair skipped",
                    job.provider_key,
                    {"instrument": item.instrument, "provider_symbol": item.provider_symbol,
                     "reason": "no_source_history", "timeframes": timeframe_summary, **details},
                )
                return
            self._emit(
                "backfill.download_completed",
                f"Downloaded history saved for {item.instrument}; starting candle repair",
                job.provider_key,
                {"instrument": item.instrument, "timeframes": timeframe_summary, **details},
            )
            if self._on_instrument_completed is not None:
                item.phase = "verifying"
                item.status = "verifying"
                self._on_instrument_completed(job.provider_key, item.instrument, details)
            else:
                item.phase = "completed"
                item.status = "completed"


    @staticmethod
    def _timeframe_summary(item: InstrumentProgress) -> dict[str, dict[str, int]]:
        return {
            timeframe: {
                "planned": item.planned_by_timeframe.get(timeframe, 0),
                "attempted": item.attempted_by_timeframe.get(timeframe, 0),
                "unavailable": item.unavailable_by_timeframe.get(timeframe, 0),
                "received": item.received_by_timeframe.get(timeframe, 0),
                "stored": item.stored_by_timeframe.get(timeframe, 0),
                "skipped": item.skipped_by_timeframe.get(timeframe, 0),
                "ranges": item.ranges_by_timeframe.get(timeframe, 0),
            }
            for timeframe in ("1m", "1h", "1d")
        }

    @staticmethod
    def _format_timeframe_summary(summary: dict[str, dict[str, int]]) -> str:
        labels = (("1m", "M1"), ("1h", "H1"), ("1d", "D1"))
        return "; ".join(
            f"{label} received={summary[key]['received']:,}, stored={summary[key]['stored']:,}, "
            f"skipped={summary[key]['skipped']:,}, planned={summary[key]['planned']:,}, "
            f"attempted={summary[key]['attempted']:,}, unavailable={summary[key]['unavailable']:,}"
            for key, label in labels
        )


    @classmethod
    def _source_ranges_from_availability(
        cls,
        now: datetime,
        earliest_by_timeframe: dict[str, datetime | None],
        latest_by_timeframe: dict[str, datetime | None],
    ) -> list[PlannedRange]:
        ten_year_start = now - timedelta(days=3650)
        ranges: list[PlannedRange] = []
        for timeframe in cls._DISCOVERY_TIMEFRAMES:
            earliest = earliest_by_timeframe.get(timeframe)
            latest = latest_by_timeframe.get(timeframe)
            if earliest is None or latest is None:
                continue
            start = max(ten_year_start, earliest.astimezone(UTC))
            end = min(now, latest.astimezone(UTC))
            if start > end:
                continue
            if timeframe == "1m":
                cursor = start
                while cursor <= end:
                    boundary = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1)
                    part_end = min(end, boundary - timedelta(minutes=1))
                    if part_end >= cursor:
                        ranges.append(PlannedRange("1m", cursor, part_end, 0))
                    cursor = boundary
            elif timeframe == "1h":
                cursor = start
                while cursor <= end:
                    boundary = datetime(cursor.year + 1, 1, 1, tzinfo=UTC)
                    part_end = min(end, boundary - timedelta(hours=1))
                    if part_end >= cursor:
                        ranges.append(PlannedRange("1h", cursor, part_end, 0))
                    cursor = boundary
            else:
                ranges.append(PlannedRange("1d", start, end, 0))
        return ranges

    @staticmethod
    def _simple_source_ranges(now: datetime) -> list[PlannedRange]:
        """Fixed chronological source download plan with no discovery tree.

        M1 is requested month-by-month, H1 year-by-year, and D1 in one ten-year
        request. MT5 returns whatever it has; unavailable ranges are skipped.
        """
        start = now - timedelta(days=3650)
        ranges: list[PlannedRange] = []

        cursor = start
        while cursor < now:
            boundary = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1)
            end = min(now, boundary)
            ranges.append(PlannedRange("1m", cursor, end, 0))
            cursor = end

        cursor = start
        while cursor < now:
            try:
                boundary = cursor.replace(year=cursor.year + 1)
            except ValueError:
                boundary = cursor.replace(month=2, day=28, year=cursor.year + 1)
            end = min(now, boundary)
            ranges.append(PlannedRange("1h", cursor, end, 0))
            cursor = end

        ranges.append(PlannedRange("1d", start, now, 0))
        return ranges

    def _configure_discovery(self, item: InstrumentProgress) -> None:
        timeframe = self._DISCOVERY_TIMEFRAMES[item.discovery_index]
        now = self._now_factory().astimezone(UTC).replace(second=0, microsecond=0)
        item.timeframe = timeframe
        item.cursor_start = now - timedelta(days=3650)
        item.current_end = now
        item.tier_start = item.cursor_start
        item.tier_end = item.current_end
        item.scan_phase = "discovery"
        item.status = "queued"

    def _advance_download(self, item: InstrumentProgress) -> None:
        self._clear_request(item)
        item.download_index += 1
        item.status = "queued"

    def _new_request(self, item: InstrumentProgress, kind: str) -> None:
        item.current_request_id = uuid.uuid4().hex
        item.request_kind = kind
        item.dispatched_at = self._now_factory()
        item.dispatch_attempts = 1
        item.status = "importing" if kind == "backfill" else "probing"

    @staticmethod
    def _set_range(item: InstrumentProgress, value: PlannedRange) -> None:
        item.timeframe = value.timeframe
        item.cursor_start = value.start
        item.current_end = value.end
        item.tier_start = value.start
        item.tier_end = value.end
        item.tier_index = value.depth

    @staticmethod
    def _clear_request(item: InstrumentProgress) -> None:
        item.current_request_id = None
        item.request_kind = None
        item.dispatched_at = None
        item.dispatch_attempts = 0

    def _coverage_complete(
        self, provider: str, instrument: str, timeframe: str, start: datetime, end: datetime,
        earliest: datetime | None, latest: datetime | None, provider_count: int, local_count: int,
    ) -> bool:
        if provider_count <= 0 or earliest is None or latest is None or local_count < provider_count:
            return False
        if self._local_bounds is None:
            return local_count >= provider_count
        local_earliest, local_latest = self._local_bounds(provider, instrument, timeframe, start, end)
        return (local_earliest is not None and local_latest is not None
                and local_earliest <= earliest and local_latest >= latest)

    @staticmethod
    def _current_depth(item: InstrumentProgress) -> int:
        return max(0, item.tier_index)

    def _split(self, value: PlannedRange) -> list[PlannedRange]:
        if value.depth == 0:
            return self._calendar_parts(value, "year")
        if value.depth == 1:
            return self._calendar_parts(value, "month")
        if value.depth == 2:
            return self._calendar_parts(value, "day")
        if value.depth == 3 and value.timeframe == "1m":
            return self._calendar_parts(value, "hour")
        return []

    @staticmethod
    def _calendar_parts(value: PlannedRange, unit: str) -> list[PlannedRange]:
        start, end = value.start, value.end
        parts: list[PlannedRange] = []
        cursor = start
        while cursor <= end:
            if unit == "year":
                boundary = datetime(cursor.year + 1, 1, 1, tzinfo=cursor.tzinfo)
            elif unit == "month":
                boundary = (datetime(cursor.year + 1, 1, 1, tzinfo=cursor.tzinfo)
                            if cursor.month == 12 else datetime(cursor.year, cursor.month + 1, 1, tzinfo=cursor.tzinfo))
            elif unit == "day":
                boundary = datetime(cursor.year, cursor.month, cursor.day, tzinfo=cursor.tzinfo) + timedelta(days=1)
            else:
                boundary = cursor.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            part_end = min(end, boundary - FullHistoryBackfillManager._step(value.timeframe))
            if part_end >= cursor:
                parts.append(PlannedRange(value.timeframe, cursor, part_end, value.depth + 1))
            cursor = boundary
        return parts

    @staticmethod
    def _step(timeframe: str) -> timedelta:
        return {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[timeframe]

    def _mark_bad(self, item: InstrumentProgress, value: PlannedRange, reason: str, error_code: int | None, *, persist: bool = True) -> None:
        item.ranges_unavailable += 1
        item.bad_ranges.append({
            "timeframe": value.timeframe, "from_utc": value.start.isoformat(),
            "to_utc": value.end.isoformat(), "reason": reason, "error_code": error_code,
        })
        item.last_result = f"bad range skipped: {value.timeframe} {value.start.isoformat()}..{value.end.isoformat()} ({reason})"
        if persist and self._bad_range_recorder is not None:
            # provider is resolved by the recorder closure in the web composition root.
            self._bad_range_recorder(item.provider_key, item.instrument, value.timeframe, value.start, value.end, reason, error_code)

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
        if job.active_index >= len(job.instruments) and job.status == "running":
            totals = self._totals(job)
            failed_instruments = [item.instrument for item in job.instruments if item.error]
            event_name = "backfill.download_all_completed_with_failures" if failed_instruments else "backfill.download_all_completed"
            message = (
                f"History download finished with {len(failed_instruments)} instrument failure(s)"
                if failed_instruments else "All planned history downloads completed"
            )
            self._emit(event_name, message, job.provider_key,
                       {"job_id": job.job_id, "failed_instruments": failed_instruments, **totals})
            if self._on_download_completed is None:
                job.status = "completed"
                job.phase = "completed"
                job.completed_at = self._now_factory()
                self._emit("backfill.full_completed", "Full-history workflow completed", job.provider_key,
                           {"job_id": job.job_id, **totals})
            else:
                job.status = "repairing"
                job.phase = "repair"
                job.repair_started_at = self._now_factory()
                self._emit("repair.full_started", "Bottom-up candle repair started", job.provider_key,
                           {"job_id": job.job_id, **totals})
                self._on_download_completed(
                    job.provider_key, job.job_id, [item.instrument for item in job.instruments]
                )

    def _format_request(self, item: InstrumentProgress) -> str:
        assert item.current_request_id and item.timeframe and item.cursor_start and item.current_end
        command = {"discovery": "DISCOVER", "availability": "AVAILABILITY", "backfill": "BACKFILL"}[item.request_kind or "availability"]
        return "|".join((command, item.provider_symbol, item.timeframe,
                         item.cursor_start.isoformat(), item.current_end.isoformat(), item.current_request_id))

    def complete_repair(
        self, provider_key: str, job_id: str, summary: dict[str, object], *, error: str | None = None
    ) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.provider_key != provider_key or job.status != "repairing":
                return
            job.repair_completed_at = self._now_factory()
            job.completed_at = job.repair_completed_at
            job.status = "failed" if error else "completed"
            job.phase = "failed" if error else "completed"
            details = {"job_id": job.job_id, **self._totals(job), **summary}
            if error:
                details["error"] = error
                self._emit("repair.full_failed", "Bottom-up candle repair failed", provider_key, details)
            else:
                self._emit("repair.full_completed", "Bottom-up candle repair completed", provider_key, details)
                self._emit("backfill.full_completed", "Full-history download and candle repair completed", provider_key, details)

    def status(self, provider_key: str | None = None) -> dict[str, object]:
        with self._lock:
            jobs = [job for job in self._jobs.values() if provider_key is None or job.provider_key == provider_key]
            return {"jobs": [self._view(job) for job in sorted(jobs, key=lambda value: value.created_at, reverse=True)]}

    def _emit(self, category: str, message: str, provider: str, details: dict[str, object]) -> None:
        if self._on_event is not None:
            self._on_event(category, message, provider, details)

    @classmethod
    def _merge_ranges(cls, values: list[PlannedRange]) -> list[PlannedRange]:
        merged: list[PlannedRange] = []
        for value in sorted(values, key=lambda item: (item.timeframe, item.start, item.end)):
            if merged and merged[-1].timeframe == value.timeframe                     and value.start <= merged[-1].end + cls._step(value.timeframe):
                previous = merged[-1]
                merged[-1] = PlannedRange(
                    previous.timeframe, previous.start, max(previous.end, value.end),
                    max(previous.depth, value.depth),
                )
            else:
                merged.append(value)
        # Preserve authority order: minute, hour, day.
        order = {"1m": 0, "1h": 1, "1d": 2}
        return sorted(merged, key=lambda item: (order[item.timeframe], item.start))

    @staticmethod
    def _count_by_timeframe(values: list[PlannedRange]) -> dict[str, int]:
        result: dict[str, int] = {}
        for value in values:
            result[value.timeframe] = result.get(value.timeframe, 0) + 1
        return result

    @staticmethod
    def _totals(job: FullHistoryJob) -> dict[str, object]:
        return {
            "instruments": len(job.instruments),
            "ranges_planned": sum(len(item.missing_ranges) for item in job.instruments),
            "bad_ranges": sum(len(item.bad_ranges) for item in job.instruments),
            "candles_received": sum(item.bars_received for item in job.instruments),
            "candles_stored": sum(item.bars_inserted for item in job.instruments),
            "candles_skipped": sum(max(0, item.bars_received - item.bars_inserted) for item in job.instruments),
            "instrument_failures": sum(1 for item in job.instruments if item.error),
            "failed_instruments": [item.instrument for item in job.instruments if item.error],
        }

    @staticmethod
    def _view(job: FullHistoryJob) -> dict[str, object]:
        return {
            "job_id": job.job_id, "provider_key": job.provider_key, "status": job.status,
            "phase": job.phase, "workflow": job.workflow, "repair_pass": job.repair_pass,
            "created_at": job.created_at.isoformat(),
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "repair_started_at": job.repair_started_at.isoformat() if job.repair_started_at else None,
            "repair_completed_at": job.repair_completed_at.isoformat() if job.repair_completed_at else None,
            "instruments": [{
                "provider_symbol": item.provider_symbol, "instrument": item.instrument,
                "status": item.status, "phase": item.phase, "timeframe": item.timeframe,
                "tier_index": item.tier_index,
                "tier_start": item.tier_start.isoformat() if item.tier_start else None,
                "tier_end": item.tier_end.isoformat() if item.tier_end else None,
                "cursor_start": item.cursor_start.isoformat() if item.cursor_start else None,
                "current_end": item.current_end.isoformat() if item.current_end else None,
                "available_count": item.available_count, "local_count": item.local_count,
                "earliest_available": item.earliest_available.isoformat() if item.earliest_available else None,
                "latest_available": item.latest_available.isoformat() if item.latest_available else None,
                "batches_completed": item.batches_completed, "ranges_probed": item.ranges_probed,
                "ranges_skipped_existing": item.ranges_skipped_existing,
                "ranges_unavailable": item.ranges_unavailable, "bars_available": item.bars_available,
                "bars_received": item.bars_received, "bars_inserted": item.bars_inserted,
                "received_by_timeframe": dict(item.received_by_timeframe),
                "stored_by_timeframe": dict(item.stored_by_timeframe),
                "skipped_by_timeframe": dict(item.skipped_by_timeframe),
                "ranges_by_timeframe": dict(item.ranges_by_timeframe),
                "planned_by_timeframe": dict(item.planned_by_timeframe),
                "attempted_by_timeframe": dict(item.attempted_by_timeframe),
                "unavailable_by_timeframe": dict(item.unavailable_by_timeframe),
                "error": item.error, "retry_count": item.retry_count,
                "last_error_code": item.last_error_code, "request_kind": item.request_kind,
                "request_id": item.current_request_id,
                "dispatched_at": item.dispatched_at.isoformat() if item.dispatched_at else None,
                "dispatch_attempts": item.dispatch_attempts, "last_result": item.last_result,
                "scan_phase": item.scan_phase, "coarse_start": None, "coarse_end": None,
                "fine_end": None, "coarse_ranges_probed": item.coarse_ranges_probed,
                "fine_ranges_probed": item.fine_ranges_probed,
                "discovery_index": item.discovery_index, "discovery_complete": item.discovery_complete,
                "discovery_boundaries": {key: value.isoformat() if value else None for key, value in item.discovery_boundaries.items()},
                "discovery_counts": dict(item.discovery_counts),
                "discovery_local_counts": dict(item.discovery_local_counts),
                "discovery_complete_timeframes": sorted(item.discovery_complete_timeframes),
                "planning_ranges_remaining": len(item.planning_queue),
                "missing_ranges_planned": len(item.missing_ranges),
                "download_index": item.download_index,
                "bad_ranges": list(item.bad_ranges),
            } for item in job.instruments],
        }
