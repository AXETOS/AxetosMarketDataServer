from __future__ import annotations

import csv
import io
import json
import logging
import asyncio
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response, StreamingResponse
from pydantic import BaseModel, Field, model_validator

from .config import ConfigurationStore, ProviderConfig, valid_password_env_name
from .secrets import SecretStore, SecretStoreError
from .runtime import ProviderSupervisor
from .history import HistoricalBackfillService, AuthoritativeHistoryRebuildService
from .providers.mt5 import MetaTrader5TickProvider
from .providers.yahoo import YahooHistoricalProvider
from .calendar import MarketCalendar
from .clock import ensure_server_local, server_now
from datetime import datetime, UTC, timedelta
from decimal import Decimal
from .storage import MarketDataStore
from .diagnostics import build_health, build_liveness, build_metrics, build_readiness, prometheus_text
from .housekeeping import HousekeepingService, RetentionPolicy
from .scheduler import MaintenanceScheduler
from . import __version__
from .policies import choose_canonical_source
from .quality import CandleQualityService
from .operational import OperationalEventService
from .symbols import SymbolResolver, normalize_instrument
from .security import SecuritySettings, install_security_middleware
from .streaming import LiveStreamHub, StreamFilter
from .backups import BackupError, BackupService
from .benchmark_jobs import BenchmarkJobManager
from .bridge import (Mt5BridgeService, BridgeHeartbeatRequest, BridgeInstrumentsRequest, BridgeTicksRequest, BridgeQuotesRequest, BridgeCandlesRequest, InstrumentSelectionRequest)
from .full_history import FullHistoryBackfillManager, PlannedRange
from .history_worker import HistoryIngestionProcess
from .repair_worker import CandleRepairProcess
from .hierarchical_repair import HierarchicalCandleRepair
from .live_m1 import LiveM1CommandScheduler
from .mt5_terminal_clock import Mt5TerminalClockCoordinator
from .authoritative_refresh import AuthoritativeRefreshBuffer


class BackfillRequest(BaseModel):
    provider_key: str
    symbol: str
    instrument: str | None = None
    timeframe: str = "1m"
    days: int = Field(default=7, ge=1, le=3650)


class HistoryRebuildRequest(BaseModel):
    provider_key: str
    days: int = Field(default=7, ge=1, le=3650)
    instrument: str | None = None


class GapScanRequest(BaseModel):
    provider_key: str
    instrument: str
    timeframe: str = "1m"
    days: int = Field(default=7, ge=1, le=3650)


class GapRepairRequest(BaseModel):
    provider_key: str
    instrument: str | None = None
    timeframe: str | None = None
    limit: int = Field(default=500, ge=1, le=5000)


class RetentionRequest(BaseModel):
    tick_days: int = Field(default=30, ge=1, le=3650)
    operational_days: int = Field(default=90, ge=1, le=3650)
    vacuum: bool = False


class MaintenanceScheduleRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    task_type: str = "retention"
    enabled: bool = True
    interval_minutes: int = Field(default=1440, ge=1, le=525600)
    tick_days: int = Field(default=30, ge=1, le=3650)
    operational_days: int = Field(default=90, ge=1, le=3650)
    vacuum: bool = False
    run_immediately: bool = False


class SymbolPolicyRequest(BaseModel):
    provider_key: str = Field(min_length=1)
    provider_symbol: str = Field(min_length=1)
    canonical_instrument: str = Field(min_length=1)
    enabled: bool = True
    allow_live: bool = True
    allow_history: bool = True
    priority_override: int | None = Field(default=None, ge=0, le=10000)


class QualityScanRequest(BaseModel):
    provider: str | None = None
    instrument: str | None = None
    timeframe: str | None = None
    limit: int = Field(default=10000, ge=1, le=100000)
    max_move_percent: float = Field(default=20.0, gt=0, le=10000)


class BenchmarkRunRequest(BaseModel):
    ticks: int = Field(default=100_000, ge=1_000, le=10_000_000)
    instruments: int = Field(default=10, ge=1, le=10_000)
    batch_sizes: list[int] = Field(default_factory=lambda: [5_000], min_length=1, max_length=8)

    @model_validator(mode="after")
    def validate_batch_sizes(self):
        if any(value < 1 or value > 100_000 for value in self.batch_sizes):
            raise ValueError("batch sizes must be between 1 and 100000")
        return self


class ProviderRequest(BaseModel):
    provider_key: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    kind: str = "mock"
    enabled: bool = True
    auto_start: bool = True
    poll_interval_seconds: float = Field(default=1.0, gt=0)
    symbols: list[str] | None = None
    symbol_aliases: dict[str, str] | None = None
    terminal_path: str | None = None
    account_login: int | None = Field(default=None, ge=1)
    account_server: str | None = None
    password_env: str | None = None
    mt5_password: str | None = None
    priority: int = Field(default=100, ge=0, le=10000)
    fallback_after_seconds: float = Field(default=10.0, gt=0, le=3600)
    batch_window_seconds: int = Field(default=5, ge=1, le=300)
    batch_limit: int = Field(default=50000, ge=100, le=1000000)
    maintenance_enabled: bool = False
    maintenance_interval_minutes: int = Field(default=60, ge=1, le=10080)
    maintenance_backfill_days: int = Field(default=2, ge=1, le=365)
    feed_quiet_seconds: float = Field(default=60.0, gt=0, le=3600)
    feed_stalled_seconds: float = Field(default=180.0, gt=0, le=7200)
    feed_inactive_seconds: float = Field(default=600.0, gt=0, le=86400)

    @model_validator(mode="after")
    def validate_feed_thresholds(self):
        if not self.feed_quiet_seconds < self.feed_stalled_seconds < self.feed_inactive_seconds:
            raise ValueError("feed thresholds must satisfy quiet < stalled < inactive")
        if self.password_env not in (None, "", "********") and not valid_password_env_name(self.password_env):
            raise ValueError("password_env must be an uppercase environment-variable name")
        return self



def aggregate_feed_state(items: list[dict[str, object]]) -> tuple[str, dict[str, int]]:
    """Return an honest aggregate without letting one stale instrument mask live feeds."""
    counts: dict[str, int] = {}
    for item in items:
        state = str(item.get("feed_state", "INITIALIZING")).upper()
        counts[state] = counts.get(state, 0) + 1

    if not counts:
        return "INITIALIZING", counts
    if len(counts) == 1:
        return next(iter(counts)), counts

    operational = {"LIVE", "QUIET"}
    if operational.intersection(counts) and set(counts).difference(operational):
        return "PARTIAL", counts
    if set(counts).issubset(operational):
        return ("LIVE" if "LIVE" in counts else "QUIET"), counts

    rank = {"INITIALIZING": 0, "RECOVERING": 1, "STALLED": 2, "INACTIVE": 3}
    return max(counts, key=lambda state: rank.get(state, 0)), counts

def create_app(
    database_path: str | Path = "data/market_data.sqlite",
    configuration_path: str | Path = "data/providers.json",
) -> FastAPI:
    store = MarketDataStore(database_path)
    store.initialize()
    stream_hub = LiveStreamHub()
    store.set_live_publishers(stream_hub.publish_tick, stream_hub.publish_candle)
    config_store = ConfigurationStore(configuration_path)
    secret_store = SecretStore(Path(configuration_path).parent / "mt5_secrets.json")
    supervisor = ProviderSupervisor(config_store, store, secret_store)
    history_process = HistoryIngestionProcess(store.database_target)
    repair_process = CandleRepairProcess(store.database_target)
    live_m1 = LiveM1CommandScheduler()
    authoritative_refreshes = AuthoritativeRefreshBuffer()
    scheduler = MaintenanceScheduler(store)
    backups = BackupService(database_path, configuration_path)
    benchmark_jobs = BenchmarkJobManager()
    def record_full_history_event(
        category: str, message: str, provider: str, details: dict[str, object]
    ) -> None:
        events.record("info", category, message, provider=provider, details=details)

    def recent_m1_missing_ranges(
        provider: str, instruments: list[str], *, hours: int = 24,
    ) -> dict[str, list[PlannedRange]]:
        """Return one complete recent M1 verification window per instrument.

        Live observations create provisional M1 candles only. Recent repair therefore
        never guesses from shapes or provenance: it asks MT5 for the complete official
        window and replaces every candle MT5 returns.
        """
        end = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
        start = end - timedelta(hours=hours) + timedelta(minutes=1)
        return {instrument: [PlannedRange("1m", start, end, 4)] for instrument in instruments}

    def startup_m1_refresh_ranges(
        provider: str, instruments: list[str], *, minutes: int = 10,
    ) -> dict[str, list[PlannedRange]]:
        """Return the last completed M1 window for authoritative startup overwrite."""
        end = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
        start = end - timedelta(minutes=minutes - 1)
        return {instrument: [PlannedRange("1m", start, end, 4)] for instrument in instruments}

    def schedule_startup_m1_refresh() -> None:
        """Refresh the last ten completed M1 candles after each MT5 provider connects."""
        def run_provider(provider_key: str) -> None:
            # MT5 terminal startup/login is asynchronous. Wait until the exact provider
            # reports a connected broker session before reserving its history operation.
            for _ in range(180):
                worker = supervisor.get(provider_key)
                if worker is None or not worker.config.enabled:
                    return
                runtime = worker.view().get("runtime", {})
                if runtime.get("terminal_running") and runtime.get("broker_connected"):
                    break
                threading.Event().wait(1.0)
            else:
                events.record(
                    "warning", "startup.m1_refresh_skipped",
                    "Startup M1 overwrite skipped because MT5 did not connect in time",
                    provider=provider_key, details={"minutes": 10},
                )
                return

            worker = supervisor.get(provider_key)
            if worker is None:
                return
            resolver = SymbolResolver(store)
            symbols: list[tuple[str, str]] = []
            for provider_symbol in worker.config.normalized_symbols():
                resolved = resolver.resolve(provider_key, provider_symbol)
                symbols.append((provider_symbol, normalize_instrument(resolved.canonical_instrument)))
            ranges = startup_m1_refresh_ranges(provider_key, [instrument for _, instrument in symbols])
            planned = [
                (provider_symbol, instrument, ranges[instrument])
                for provider_symbol, instrument in symbols if instrument in ranges
            ]
            if not planned:
                return
            try:
                full_history.start_targeted(provider_key, planned, workflow="startup_m1")
                events.record(
                    "info", "startup.m1_refresh_started",
                    "Startup authoritative M1 overwrite started", provider=provider_key,
                    details={"minutes": 10, "instruments": len(planned)},
                )
            except RuntimeError as exc:
                events.record(
                    "warning", "startup.m1_refresh_deferred",
                    "Startup M1 overwrite could not start because another history operation is active",
                    provider=provider_key, details={"minutes": 10, "error": str(exc)},
                )

        for config in config_store.read_all():
            if config.kind.lower() == "mt5" and config.enabled and config.auto_start:
                threading.Thread(
                    target=run_provider, args=(config.provider_key,),
                    name=f"startup-m1-{config.provider_key}", daemon=True,
                ).start()

    def mark_recent_m1_unresolved_bad(
        provider: str, ranges_by_instrument: dict[str, list[PlannedRange]], reason: str,
    ) -> int:
        marked = 0
        for instrument, ranges in ranges_by_instrument.items():
            for value in ranges:
                store.mark_history_bad_range(
                    provider, instrument, value.timeframe, value.start, value.end, reason, None
                )
                marked += 1
        return marked

    def verify_recent_m1_instrument(
        provider: str, instrument: str, details: dict[str, object],
    ) -> None:
        """Rebuild higher intervals after an authoritative timestamp upsert.

        The recent-M1 refresh intentionally performs no post-write verification gate.
        Every M1 timestamp returned by MT5 has already been force-upserted by the
        history worker. After that write completes, rebuild this instrument and
        immediately allow the coordinator to continue.
        """
        workflow = str(details.get("workflow", "full"))
        phase = str(details.get("phase", "download"))
        repair_pass = int(details.get("repair_pass", 0))
        is_recent_refresh = workflow in {"recent_m1", "startup_m1"} or phase == "recent_m1_download" or repair_pass > 0
        job_id = str(details.get("job_id", ""))

        if not is_recent_refresh:
            full_history.instrument_verified(
                provider, job_id, instrument, {"refresh": "not_required_for_history_download"}
            )
            return

        # Startup refresh is deliberately limited to replacing the last ten official
        # M1 candles. It must not run hierarchy repair or emit H1/D1 repair events.
        if workflow == "startup_m1":
            full_history.instrument_verified(
                provider, job_id, instrument, {
                    "workflow": workflow,
                    "bars_returned": int(details.get("candles_received", 0)),
                    "timestamps_upserted": int(details.get("candles_stored", 0)),
                    "timestamps_untouched": int(details.get("candles_skipped", 0)),
                    "higher_timeframes_rebuilt": False,
                    "status": "COMPLETE",
                },
            )
            return

        def run() -> None:
            completion: dict[str, object] = {
                "workflow": workflow,
                "repair_pass": repair_pass,
                "bars_returned": int(details.get("candles_received", 0)),
                "timestamps_upserted": int(details.get("candles_stored", 0)),
                "timestamps_untouched": int(details.get("candles_skipped", 0)),
                "status": "COMPLETE",
            }
            try:
                result = repair_process.run(provider, [instrument])
                for stage in result.get("stages", []):
                    stage_details = dict(stage.get("details", {}))
                    category = str(stage.get("category", "repair.stage_completed"))
                    events.record(
                        "warning" if int(stage_details.get("errors", 0)) else "info",
                        category,
                        "Instrument candle repair stage started" if category.endswith("started") else "Instrument candle repair stage completed",
                        provider=provider, instrument=str(stage.get("instrument", instrument)),
                        details={"job_id": job_id, "target_timeframe": str(stage.get("timeframe", "")),
                                 "repair_worker_pid": result.get("worker_pid"), **stage_details},
                    )
                completion["repair_summary"] = result.get("summary", {})
                completion["repair_worker_pid"] = result.get("worker_pid")
                completion["higher_timeframes_rebuilt"] = True
            except Exception as exc:
                # The authoritative M1 write already succeeded. Do not reject or roll it
                # back because a later derived-timeframe rebuild had a problem. Record
                # the rebuild error and continue to the next instrument.
                completion["higher_timeframes_rebuilt"] = False
                completion["repair_error"] = str(exc)
                events.record(
                    "warning", "repair.instrument_rebuild_warning",
                    "Official M1 timestamps were replaced, but higher-timeframe rebuild reported an error",
                    provider=provider, instrument=instrument,
                    details={"job_id": job_id, **completion},
                )
            full_history.instrument_verified(provider, job_id, instrument, completion)

        threading.Thread(
            target=run, name=f"recent-m1-rebuild-{provider}-{instrument}", daemon=True,
        ).start()

    def repair_full_history(provider: str, job_id: str, instruments: list[str]) -> None:
        def run() -> None:
            context = full_history.job_context(provider, job_id) or {}
            workflow = str(context.get("workflow", "full"))
            repair_pass = int(context.get("repair_pass", 0))

            # Recent-M1 refreshes are followed by two deterministic catch-up passes
            # and a final ten-minute safety pass. Each pass covers the exact runtime
            # of the preceding pass so a long MT5 command cannot leave a visible gap.
            if workflow == "recent_m1" or repair_pass > 0:
                finished = datetime.now(UTC)
                pass_started = context.get("recent_pass_started_at") or context.get("recent_refresh_started_at") or context.get("created_at")

                first_catchup = (workflow == "recent_m1" and repair_pass == 0) or (workflow == "full" and repair_pass == 1)
                second_catchup = (workflow == "recent_m1" and repair_pass == 1) or (workflow == "full" and repair_pass == 2)
                final_safety = (workflow == "recent_m1" and repair_pass == 2) or (workflow == "full" and repair_pass == 3)

                ranges: dict[str, list[PlannedRange]] = {}
                event_category = ""
                event_message = ""
                if (first_catchup or second_catchup) and isinstance(pass_started, datetime):
                    catchup_start = pass_started.astimezone(UTC).replace(second=0, microsecond=0) - timedelta(minutes=2)
                    catchup_end = finished.astimezone(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
                    if catchup_end >= catchup_start:
                        ranges = {instrument: [PlannedRange("1m", catchup_start, catchup_end, 4)] for instrument in instruments}
                        event_category = "repair.recent_m1_catchup_started"
                        event_message = "Repair-window M1 catch-up refresh started"
                elif final_safety:
                    safety_end = finished.astimezone(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
                    safety_start = safety_end - timedelta(minutes=9)
                    ranges = {instrument: [PlannedRange("1m", safety_start, safety_end, 4)] for instrument in instruments}
                    event_category = "repair.recent_m1_final_safety_started"
                    event_message = "Final ten-minute M1 safety refresh started"

                if ranges:
                    first_range = next(iter(ranges.values()))[0]
                    events.record(
                        "info", event_category, event_message, provider=provider,
                        details={
                            "job_id": job_id, "repair_pass": repair_pass,
                            "from_utc": first_range.start.isoformat(), "to_utc": first_range.end.isoformat(),
                            "instruments": len(instruments),
                        },
                    )
                    if full_history.resume_targeted_after_repair(provider, job_id, ranges):
                        return

                events.record(
                    "info", "repair.recent_m1_catchup_completed",
                    "Repair-window M1 catch-up refresh completed", provider=provider,
                    details={
                        "job_id": job_id, "repair_pass": repair_pass,
                        "repair_finished_at": finished.isoformat(), "instruments": len(instruments),
                    },
                )
                full_history.complete_repair(provider, job_id, {
                    "recent_m1_mode": "authoritative_refresh_two_catchups_final_safety",
                    "instruments_refreshed_sequentially": len(instruments),
                    "recent_m1_window_hours": 24,
                    "repair_finished_at": finished.isoformat(),
                    "catchup_completed": True,
                })
                return

            try:
                repair_result = repair_process.run(provider, instruments)
                for stage in repair_result.get("stages", []):
                    stage_details = dict(stage.get("details", {}))
                    category = str(stage.get("category", "repair.stage_completed"))
                    if category.endswith("started"):
                        continue
                    instrument_name = str(stage.get("instrument", ""))
                    source = str(stage_details.get("source_timeframe", ""))
                    target = str(stage_details.get("target_timeframe", stage.get("timeframe", "")))
                    message = (
                        f"Candle repair for {instrument_name}: {source}→{target}, "
                        f"source={int(stage_details.get('source_rows', 0)):,}, "
                        f"candidates={int(stage_details.get('candidates', 0)):,}, "
                        f"created={int(stage_details.get('created', 0)):,}, "
                        f"overwritten={int(stage_details.get('overwritten', 0)):,}, "
                        f"unchanged={int(stage_details.get('retained_same', 0)):,}, "
                        f"errors={int(stage_details.get('errors', 0)):,}"
                    )
                    events.record(
                        "warning" if int(stage_details.get("errors", 0)) else "info",
                        category, message, provider=provider, instrument=instrument_name,
                        details={"target_timeframe": target,
                                 "repair_worker_pid": repair_result.get("worker_pid"), **stage_details},
                    )
                summary = dict(repair_result.get("summary", {}))
                summary["repair_worker_pid"] = repair_result.get("worker_pid")
            except Exception as exc:  # keep the coordinator moving and expose the failure
                full_history.complete_repair(provider, job_id, {}, error=str(exc))
                return

            should_run_official_m1 = workflow == "full" and repair_pass == 0
            ranges = (
                recent_m1_missing_ranges(provider, instruments, hours=24)
                if should_run_official_m1 else {}
            )
            gap_count = sum(len(values) for values in ranges.values())
            events.record(
                "warning" if gap_count else "info",
                "repair.recent_m1_scan_completed",
                "Last-24-hours official M1 replacement window prepared",
                provider=provider,
                details={"job_id": job_id, "workflow": workflow, "repair_pass": repair_pass,
                         "instruments": len(instruments), "verification_ranges": gap_count},
            )

            # Every full backfill gets exactly one targeted recent-M1 recovery pass.
            if should_run_official_m1 and gap_count:
                if full_history.resume_targeted_after_repair(provider, job_id, ranges):
                    return

            unresolved = 0
            summary = {**summary, "recent_m1_verification_ranges": gap_count,
                       "recent_m1_bad_ranges": unresolved, "recent_m1_window_hours": 24,
                       "recent_m1_mode": "full_provider_replacement"}
            full_history.complete_repair(provider, job_id, summary)

        threading.Thread(
            target=run,
            name=f"history-repair-{provider}-{job_id[:8]}",
            daemon=True,
        ).start()

    full_history = FullHistoryBackfillManager(
        lambda provider, instrument, timeframe, start, end: store.candle_count_range(
            provider, instrument, timeframe, start, end
        ),
        lambda provider, instrument, timeframe, start, end: store.candle_bounds_range(
            provider, instrument, timeframe, start, end
        ),
        on_instrument_completed=verify_recent_m1_instrument,
        on_download_completed=repair_full_history,
        on_event=record_full_history_event,
        bad_range_recorder=store.mark_history_bad_range,
        bad_range_probe=store.is_history_bad_range,
    )
    started_utc = datetime.now(UTC)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        history_process.start()
        repair_process.start()
        supervisor.load()
        schedule_startup_m1_refresh()
        scheduler.start()
        try:
            yield
        finally:
            scheduler.stop()
            supervisor.shutdown()
            bridge.shutdown()
            # Process joins are blocking operations. Keep them off the ASGI event loop
            # so an ordinary Ctrl+C can complete without cancelled-request tracebacks.
            await asyncio.gather(
                asyncio.to_thread(repair_process.shutdown),
                asyncio.to_thread(history_process.shutdown),
                return_exceptions=True,
            )

    security = SecuritySettings.from_environment()

    app = FastAPI(
        title="Axetos Market Data Server",
        version=__version__,
        description="Collects market ticks, builds OHLC candles, and stores market data.",
        lifespan=lifespan,
    )
    install_security_middleware(app, security)
    app.state.security = security
    app.state.store = store
    app.state.supervisor = supervisor
    app.state.secret_store = secret_store
    app.state.benchmark_jobs = benchmark_jobs
    app.state.full_history = full_history

    @app.get("/api/alerts/status")
    def alert_status() -> dict[str, object]:
        return OperationalEventService(store).dispatcher.status()

    @app.post("/api/alerts/test")
    def test_alert() -> dict[str, object]:
        service = OperationalEventService(store)
        if not service.dispatcher.settings.enabled:
            raise HTTPException(400, "Alert webhook is not configured")
        event_id = service.record(
            "warning", "alert.test", "Test operational alert",
            details={"requested_via": "management_api"},
        )
        return {"status": "submitted", "event_id": event_id}

    @app.get("/api/benchmarks/status")
    def benchmark_status() -> dict[str, object]:
        return benchmark_jobs.status()

    @app.post("/api/benchmarks/run")
    def run_benchmark(request: BenchmarkRunRequest) -> dict[str, object]:
        try:
            job = benchmark_jobs.start(request.ticks, request.instruments, request.batch_sizes)
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(409 if isinstance(exc, RuntimeError) else 400, str(exc)) from exc
        events.record(
            "warning", "benchmark.started", "Synthetic ingestion benchmark started",
            details={"ticks": request.ticks, "instruments": request.instruments, "batch_sizes": request.batch_sizes},
        )
        return job

    @app.get("/api/storage")
    def storage_status() -> dict[str, object]:
        return {
            "backend": store.backend.kind,
            "target": store.database_target if store.backend.kind == "sqlite" else "PostgreSQL",
            "sqlite_wal": store.backend.kind == "sqlite",
            "retention_vacuum_supported": store.backend.kind == "sqlite",
        }

    @app.get("/api/backups")
    def list_backups() -> dict[str, object]:
        items = backups.list_backups() if backups.is_sqlite else []
        return {"supported": backups.is_sqlite, "count": len(items), "items": items}

    @app.post("/api/backups")
    def create_backup() -> dict[str, object]:
        try:
            result = backups.create()
        except BackupError as exc:
            raise HTTPException(400, str(exc)) from exc
        events.record("info", "database.backup", "Database backup created", details=result)
        return result
    housekeeping = HousekeepingService(store)
    terminal_clock = Mt5TerminalClockCoordinator()
    bridge = Mt5BridgeService(
        store,
        heartbeat_sink=supervisor.record_bridge_heartbeat,
        observation_sink=supervisor.record_bridge_observation,
        history_process=history_process,
        historical_timestamp_normalizer=terminal_clock.normalize_returned_timestamp,
    )
    full_history.set_pressure_probe(bridge.can_run_background_write)
    quality = CandleQualityService(store)
    events = OperationalEventService(store)
    calendar = MarketCalendar()
    app.state.bridge = bridge
    app.state.history_process = history_process
    app.state.repair_process = repair_process
    app.state.quality = quality
    app.state.events = events
    app.state.calendar = calendar
    app.state.scheduler = scheduler
    app.state.stream_hub = stream_hub

    @app.get("/api/stream/status")
    def stream_status() -> dict[str, int]:
        return stream_hub.status()

    @app.get("/api/stream/live")
    async def live_stream(
        request: Request,
        instrument: list[str] = Query(default=[]),
        provider: list[str] = Query(default=[]),
        event_type: list[str] = Query(default=["tick", "candle"]),
    ) -> StreamingResponse:
        allowed_types = frozenset(value.lower() for value in event_type)
        if not allowed_types or not allowed_types.issubset({"tick", "candle"}):
            raise HTTPException(400, "event_type must contain tick and/or candle")
        stream_filter = StreamFilter(
            instruments=frozenset(value.strip() for value in instrument if value.strip()),
            providers=frozenset(value.strip() for value in provider if value.strip()),
            event_types=allowed_types,
        )

        async def generate():
            yield "retry: 2000\nevent: ready\ndata: {\"status\":\"connected\"}\n\n"
            iterator = stream_hub.subscribe(stream_filter).__aiter__()
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(iterator.__anext__(), timeout=15.0)
                    yield stream_hub.sse(event)
                except TimeoutError:
                    yield f": heartbeat {datetime.now(UTC).isoformat()}\n\n"
                except StopAsyncIteration:
                    break

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> str:
        return CONTROL_CENTER_HTML

    @app.get("/api/auth/status")
    def auth_status() -> dict[str, object]:
        return {"enabled": security.enabled, "management_roles": ["viewer", "operator", "administrator"], "bridge_authentication": security.enabled}

    @app.get("/api/live")
    def liveness() -> dict[str, object]:
        return build_liveness(__version__, started_utc)

    @app.get("/api/ready")
    def readiness(response: Response) -> dict[str, object]:
        result = build_readiness(store, supervisor, __version__)
        if not result["ready"]:
            response.status_code = 503
        return result

    @app.get("/api/health")
    def health() -> dict[str, object]:
        return build_health(store, supervisor, __version__)

    @app.get("/api/metrics")
    def metrics() -> dict[str, object]:
        return build_metrics(store, supervisor, started_utc)

    @app.get("/metrics", response_class=PlainTextResponse)
    def prometheus_metrics() -> str:
        return prometheus_text(build_metrics(store, supervisor, started_utc))

    @app.get("/api/statistics")
    def statistics() -> dict[str, object]:
        return store.statistics()

    @app.get("/api/market-data/mt5/bridge/status")
    def bridge_status() -> dict[str, object]:
        return {**store.bridge_status(), "queue": bridge.view(), "history_process": history_process.view(), "repair_process": repair_process.view()}

    @app.post("/api/market-data/ingest/mt5/heartbeat")
    def bridge_heartbeat(request: BridgeHeartbeatRequest) -> dict[str, object]:
        try: bridge.heartbeat(request)
        except ValueError as exc: raise HTTPException(400, str(exc)) from exc
        return {"accepted": True, "serverTimeUtc": datetime.now(UTC).isoformat()}

    @app.post("/api/market-data/ingest/mt5/instruments")
    def bridge_instruments(request: BridgeInstrumentsRequest) -> dict[str, object]:
        try: count=bridge.instruments(request)
        except ValueError as exc: raise HTTPException(400, str(exc)) from exc
        return {"accepted": count}

    @app.post("/api/market-data/ingest/mt5/quotes")
    def bridge_quotes(request: BridgeQuotesRequest) -> dict[str, object]:
        try: count=bridge.quotes(request)
        except ValueError as exc: raise HTTPException(400, str(exc)) from exc
        return {"accepted": count, "serverTimeUtc": datetime.now(UTC).isoformat()}

    @app.post("/api/market-data/ingest/mt5/ticks", status_code=202)
    def bridge_ticks(request: BridgeTicksRequest, response: Response) -> dict[str, object]:
        try:
            count = bridge.enqueue_ticks(request)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(
                429,
                str(exc),
                headers={"Retry-After": "5"},
            ) from exc
        queue_view = bridge.view()
        response.headers["X-MT5-Queue-Depth"] = str(queue_view["queue_depth"])
        return {
            "accepted": count,
            "queued": True,
            "coalescedBatches": queue_view["coalesced_batches"],
            "droppedTicks": queue_view["dropped_ticks"],
        }

    @app.post("/api/market-data/ingest/mt5/candles")
    def bridge_candles(request: BridgeCandlesRequest) -> dict[str, object]:
        received = len(request.candles)
        context = full_history.request_context(request.provider_key, request.request_id)
        if context is None and request.request_id:
            context = live_m1.context(request.provider_key, request.request_id)

        recent_authoritative_reset = bool(context) and (
            str(context.get("workflow")) in {"recent_m1", "startup_m1"}
            or str(context.get("phase")) == "recent_m1_download"
        ) and str(context.get("timeframe")) == "1m"

        # A 24-hour repair is a true authoritative reset. Buffer every MT5 chunk
        # first, leaving the existing database window untouched. Only after the
        # complete response has arrived do we replace the whole window atomically.
        if recent_authoritative_reset:
            try:
                assembled = authoritative_refreshes.add(request)
            except ValueError as exc:
                raise HTTPException(400, str(exc)) from exc
            if assembled is None:
                return {
                    "acknowledgement": "buffered",
                    "requestId": request.request_id,
                    "received": received,
                    "stored": received,
                    "skipped": 0,
                    "buffered": True,
                }
            try:
                replaced = bridge.candles(
                    assembled,
                    replace_window=(context["from_utc"], context["to_utc"]),
                    update_runtime_aggregates=str(context.get("workflow")) != "startup_m1",
                )
            except (ValueError, RuntimeError, TimeoutError) as exc:
                authoritative_refreshes.discard(request.provider_key, request.request_id or "")
                raise HTTPException(503, str(exc)) from exc
            return {
                "acknowledgement": "replaced_window",
                "requestId": request.request_id,
                "received": received,
                "stored": received,
                "skipped": 0,
                "buffered": False,
                "windowRowsWritten": replaced,
            }

        replace_all = request.authoritative or (bool(context) and (
            str(context.get("workflow")) == "live_m1"
            or str(context.get("phase")) == "live_m1"
        ) and str(context.get("timeframe")) == "1m")
        try:
            stored = bridge.candles(request, replace_all=replace_all)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        skipped = max(0, received - stored)
        return {
            "acknowledgement": "stored",
            "requestId": request.request_id,
            "received": received,
            "stored": stored,
            "skipped": skipped,
        }

    @app.get("/api/market-data/mt5/discovered-instruments")
    def discovered_instruments(provider_key: str | None=None, terminal_instance_id: str | None=None) -> dict[str, object]:
        items=store.list_bridge_instruments(provider_key, terminal_instance_id)
        return {"count":len(items), "instruments":items}

    @app.get("/api/market-data/mt5/enabled-symbols.txt", response_class=PlainTextResponse)
    def enabled_bridge_symbols(
        provider_key: str = Query(alias="providerKey"),
        terminal_instance_id: str = Query(alias="terminalInstanceId"),
    ) -> str:
        # The Market Data Server is the sole subscription authority. The bridge's
        # discovered/Market Watch selection is diagnostic only and must never decide
        # which instruments are streamed. Return the exact provider configuration.
        _ = terminal_instance_id
        worker = supervisor.get(provider_key)
        if worker is None or worker.config.kind.lower() != "mt5" or not worker.config.enabled:
            return ""
        return ",".join(worker.config.normalized_symbols())

    @app.get("/api/market-data/mt5/repair-request.txt", response_class=PlainTextResponse)
    def bridge_repair_request(
        provider_key: str = Query(alias="providerKey"),
        terminal_instance_id: str = Query(alias="terminalInstanceId"),
    ) -> str:
        _ = terminal_instance_id
        command = full_history.next_request(provider_key)
        worker = supervisor.get(provider_key)
        if worker is None or worker.config.kind.lower() != "mt5" or not worker.config.enabled:
            return ""
        if not command:
            command = live_m1.next_command(provider_key, worker.config.normalized_symbols())
        # Every CopyRates command is preceded by an explicit terminal-time
        # handshake. The server never guesses broker time from quote timestamps.
        return terminal_clock.prepare(provider_key, terminal_instance_id, command)

    @app.post("/api/market-data/mt5/terminal-time")
    def bridge_terminal_time(
        provider_key: str = Query(alias="providerKey"),
        terminal_instance_id: str = Query(alias="terminalInstanceId"),
        request_id: str = Query(alias="requestId"),
        terminal_time: datetime = Query(alias="terminalTime"),
    ) -> dict[str, object]:
        try:
            offset = terminal_clock.record_terminal_time(
                provider_key, terminal_instance_id, request_id, terminal_time
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        return {"accepted": True, "offsetSeconds": int(offset.total_seconds())}

    @app.post("/api/market-data/mt5/repair-result")
    def bridge_repair_result(
        provider_key: str = Query(alias="providerKey"),
        terminal_instance_id: str = Query(alias="terminalInstanceId"),
        provider_symbol: str = Query(alias="providerSymbol"),
        interval: str = Query(),
        completed: bool = Query(),
        request_id: str = Query(alias="requestId"),
        bars_received: int = Query(default=0, alias="barsReceived"),
        bars_inserted: int = Query(default=0, alias="barsInserted"),
        unavailable: bool = Query(default=False),
        error_code: int | None = Query(default=None, alias="errorCode"),
    ) -> dict[str, object]:
        _ = (terminal_instance_id, provider_symbol, interval)
        if live_m1.accepts(provider_key, request_id):
            acknowledgement = live_m1.complete(provider_key, request_id, completed)
        else:
            acknowledgement = full_history.batch_result(
                provider_key, request_id, bars_received, bars_inserted, completed,
                unavailable=unavailable, error_code=error_code,
            )
        accepted = acknowledgement != "IGNORED"
        terminal_clock.complete(request_id)
        return {
            "accepted": accepted,
            "requestId": request_id,
            "acknowledgement": acknowledgement,
        }

    @app.post("/api/market-data/mt5/history-availability")
    def bridge_history_availability(
        provider_key: str = Query(alias="providerKey"),
        request_id: str = Query(alias="requestId"),
        earliest_utc: datetime | None = Query(default=None, alias="earliestUtc"),
        latest_utc: datetime | None = Query(default=None, alias="latestUtc"),
        candle_count: int = Query(default=0, alias="candleCount"),
    ) -> dict[str, object]:
        normalized_earliest = (
            terminal_clock.normalize_returned_timestamp(request_id, earliest_utc)
            if earliest_utc is not None else None
        )
        normalized_latest = (
            terminal_clock.normalize_returned_timestamp(request_id, latest_utc)
            if latest_utc is not None else None
        )
        decision = full_history.availability_result(
            provider_key, request_id, earliest=normalized_earliest,
            latest=normalized_latest, count=candle_count,
        )
        terminal_clock.complete(request_id)
        return PlainTextResponse(decision)

    @app.post("/api/full-history/{provider_key}")
    def start_full_history(provider_key: str) -> dict[str, object]:
        worker = supervisor.get(provider_key)
        if worker is None:
            raise HTTPException(404, "Provider not found")
        if worker.config.kind.lower() != "mt5":
            raise HTTPException(400, "Full-history backfill requires an MT5 provider")
        resolver = SymbolResolver(store)
        symbols = []
        for provider_symbol in worker.config.normalized_symbols():
            resolved = resolver.resolve(provider_key, provider_symbol)
            symbols.append((provider_symbol, normalize_instrument(resolved.canonical_instrument)))
        try:
            result = full_history.start(provider_key, symbols)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        events.record("info", "backfill.full_started", "Full-history background backfill started", provider=provider_key, details={"instruments": len(symbols)})
        return result

    @app.post("/api/full-history/{provider_key}/repair-recent-m1")
    def repair_recent_m1(provider_key: str) -> dict[str, object]:
        worker = supervisor.get(provider_key)
        if worker is None:
            raise HTTPException(404, "Provider not found")
        if worker.config.kind.lower() != "mt5":
            raise HTTPException(400, "Recent M1 repair requires an MT5 provider")
        resolver = SymbolResolver(store)
        symbols: list[tuple[str, str]] = []
        for provider_symbol in worker.config.normalized_symbols():
            resolved = resolver.resolve(provider_key, provider_symbol)
            symbols.append((provider_symbol, normalize_instrument(resolved.canonical_instrument)))
        ranges = recent_m1_missing_ranges(provider_key, [instrument for _, instrument in symbols], hours=24)
        planned = [
            (provider_symbol, instrument, ranges.get(instrument, []))
            for provider_symbol, instrument in symbols if ranges.get(instrument)
        ]
        if not planned:
            events.record("info", "repair.recent_m1_completed",
                          "Last-24-hours official M1 verification found no configured instruments", provider=provider_key,
                          details={"instruments": len(symbols), "ranges": 0})
            return {"provider_key": provider_key, "status": "completed", "ranges": 0}
        try:
            result = full_history.start_targeted(provider_key, planned, workflow="recent_m1")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        return result

    @app.get("/api/full-history")
    def full_history_status(provider_key: str | None = None) -> dict[str, object]:
        return full_history.status(provider_key)

    @app.get("/api/full-history/bad-ranges")
    def full_history_bad_ranges(
        provider_key: str | None = None,
        limit: int = Query(default=1000, ge=1, le=10000),
    ) -> dict[str, object]:
        values = store.list_history_bad_ranges(provider_key, limit)
        return {"count": len(values), "ranges": values}

    @app.post("/api/market-data/mt5/instrument-selection")
    def instrument_selection(request: InstrumentSelectionRequest) -> dict[str, object]:
        changed=store.set_bridge_instrument_selection(request.provider_key,request.terminal_instance_id,request.provider_symbol,request.enabled)
        if not changed: raise HTTPException(404,"Discovered instrument not found")
        return {"updated":True,"enabled":request.enabled}


    @app.get("/api/database/integrity")
    def database_integrity() -> dict[str, object]:
        result = store.integrity_check()
        events.record("info" if result.get("status") == "ok" else "error", "database.integrity",
                      "Database integrity check completed", details=result)
        return result

    @app.get("/api/operational-events")
    def operational_events(
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=50, ge=1, le=500),
        severity: str | None = None, category: str | None = None,
        provider: str | None = None, instrument: str | None = None,
        search: str | None = None, from_utc: datetime | None = None,
        to_utc: datetime | None = None,
    ) -> dict[str, object]:
        return events.list(page=page, page_size=page_size, severity=severity, category=category,
                           provider=provider, instrument=instrument, search=search,
                           from_utc=from_utc, to_utc=to_utc)

    @app.get("/api/operational-events/export")
    def operational_events_export(
        format: str = Query(default="csv", pattern="^(csv|jsonl)$"),
        severity: str | None = None, category: str | None = None,
        provider: str | None = None, instrument: str | None = None,
        search: str | None = None, from_utc: datetime | None = None,
        to_utc: datetime | None = None,
        limit: int = Query(default=50000, ge=1, le=100000),
    ) -> Response:
        items = events.export(
            severity=severity, category=category, provider=provider, instrument=instrument,
            search=search, from_utc=from_utc, to_utc=to_utc, limit=limit,
        )
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        if format == "jsonl":
            body = "".join(json.dumps(item, sort_keys=True, default=str) + "\n" for item in items)
            media_type = "application/x-ndjson"
            suffix = "jsonl"
        else:
            output = io.StringIO(newline="")
            writer = csv.DictWriter(
                output,
                fieldnames=["id", "timestamp_utc", "severity", "category", "provider",
                            "instrument", "message", "details_json"],
            )
            writer.writeheader()
            for item in items:
                writer.writerow({
                    "id": item.get("id"),
                    "timestamp_utc": item.get("timestamp_utc"),
                    "severity": item.get("severity"),
                    "category": item.get("category"),
                    "provider": item.get("provider") or "",
                    "instrument": item.get("instrument") or "",
                    "message": item.get("message"),
                    "details_json": json.dumps(item.get("details", {}), sort_keys=True, default=str),
                })
            body = output.getvalue()
            media_type = "text/csv"
            suffix = "csv"
        events.record(
            "info", "operational.export", "Operational events exported",
            details={"format": format, "row_count": len(items), "limit": limit},
        )
        return Response(
            content=body,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="operational-events-{stamp}.{suffix}"'},
        )

    @app.post("/api/database/retention/preview")
    def retention_preview(request: RetentionRequest) -> dict[str, object]:
        return housekeeping.preview(RetentionPolicy(request.tick_days, request.operational_days))

    @app.post("/api/database/retention/run")
    def retention_run(request: RetentionRequest) -> dict[str, object]:
        return housekeeping.run(
            RetentionPolicy(request.tick_days, request.operational_days), request.vacuum
        )

    @app.get("/api/database/retention/history")
    def retention_history(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, object]:
        values = store.list_cleanup_runs(limit)
        return {"count": len(values), "items": values}

    @app.get("/api/maintenance/schedules")
    def maintenance_schedules() -> dict[str, object]:
        values = store.list_maintenance_schedules()
        return {"count": len(values), "items": values}

    @app.put("/api/maintenance/schedules/{name}")
    def maintenance_schedule_upsert(name: str, request: MaintenanceScheduleRequest) -> dict[str, object]:
        if request.task_type != "retention":
            raise HTTPException(400, "Only retention schedules are currently supported")
        next_run = datetime.now(UTC) if request.run_immediately else datetime.now(UTC) + timedelta(minutes=request.interval_minutes)
        value = store.upsert_maintenance_schedule(
            name, request.task_type, request.enabled, request.interval_minutes, next_run,
            request.tick_days, request.operational_days, request.vacuum,
        )
        events.record("info", "maintenance.schedule.updated", "Maintenance schedule updated",
                      details={"schedule": name, "enabled": request.enabled, "interval_minutes": request.interval_minutes})
        scheduler.wake()
        return value

    @app.post("/api/maintenance/schedules/{name}/run")
    def maintenance_schedule_run(name: str) -> dict[str, object]:
        try:
            return scheduler.execute_by_name(name)
        except KeyError as exc:
            raise HTTPException(404, "Maintenance schedule not found") from exc

    @app.get("/api/maintenance/runs")
    def maintenance_schedule_runs(limit: int = Query(default=100, ge=1, le=1000)) -> dict[str, object]:
        values = store.list_maintenance_schedule_runs(limit)
        return {"count": len(values), "items": values}

    @app.get("/api/calendar/closures")
    def calendar_closures(instrument: str, start: str, end: str) -> dict[str, object]:
        try:
            start_date = datetime.fromisoformat(start).date()
            end_date = datetime.fromisoformat(end).date()
        except ValueError as exc:
            raise HTTPException(400, "start and end must be ISO dates") from exc
        if end_date < start_date:
            raise HTTPException(400, "end must not be earlier than start")
        values = calendar.closures(instrument, start_date, end_date)
        return {"instrument": instrument, "count": len(values), "closures": [
            {"date": item.date.isoformat(), "calendar_key": item.calendar_key, "name": item.name, "source": item.source}
            for item in values
        ]}

    @app.get("/api/providers")
    def providers() -> dict[str, object]:
        return {"providers": supervisor.list_views()}

    @app.get("/api/feed-status")
    def feed_status() -> dict[str, object]:
        items = supervisor.feed_reports()
        overall, state_counts = aggregate_feed_state(items)
        return {
            "overall_state": overall,
            "count": len(items),
            "state_counts": state_counts,
            "items": items,
        }

    @app.get("/api/routing")
    def routing() -> dict[str, object]:
        return {"routes": supervisor.authority.snapshot()}

    @app.get("/api/symbol-policies")
    def symbol_policies(provider_key: str | None = None, instrument: str | None = None) -> dict[str, object]:
        items = store.list_symbol_policies(provider_key, instrument)
        return {"count": len(items), "items": items}

    @app.get("/api/symbol-normalization")
    def symbol_normalization(provider_key: str, provider_symbol: str, reported: str | None = None) -> dict[str, object]:
        resolution = SymbolResolver(store).resolve(provider_key, provider_symbol, reported)
        return {
            "provider_key": resolution.provider_key,
            "provider_symbol": resolution.provider_symbol,
            "canonical_instrument": resolution.canonical_instrument,
            "source": resolution.source,
        }

    @app.get("/api/providers/{provider_key}/symbols")
    def provider_symbols(provider_key: str, refresh: bool = False, search: str | None = None, limit: int = Query(default=1000, ge=1, le=10000)) -> dict[str, object]:
        worker = supervisor.get(provider_key)
        if worker is None:
            raise HTTPException(404, "Provider not found")
        if worker.config.kind.lower() != "mt5":
            raise HTTPException(400, "Symbol discovery is available only for MT5 providers")
        policies = {str(item["provider_symbol"]): item for item in store.list_symbol_policies(provider_key=provider_key)}
        discovered: list[dict[str, object]] = []
        if refresh:
            try:
                discovered = MetaTrader5TickProvider(
                    worker.config.normalized_symbols(), worker.config.terminal_path, provider_key,
                    worker.config.batch_window_seconds, worker.config.batch_limit, worker.config.symbol_aliases, store,
                    worker.config.account_login, worker.config.account_server, worker.config.password_env,
                    secret_store.get(worker.config.provider_key),
                ).discover_symbols(search, limit)
            except RuntimeError as exc:
                raise HTTPException(400, str(exc)) from exc
        else:
            resolver = SymbolResolver(store, worker.config.symbol_aliases)
            discovered = [{
                "provider_symbol": symbol,
                "description": "Configured provider symbol",
                "canonical_instrument": resolver.resolve(provider_key, symbol).canonical_instrument,
                "mapping_source": resolver.resolve(provider_key, symbol).source,
                "visible": None, "selected": symbol in worker.config.normalized_symbols(), "digits": None, "path": None,
            } for symbol in worker.config.normalized_symbols()]
        active_by_canonical = {
            normalize_instrument(str(policy["canonical_instrument"])): str(policy["provider_symbol"])
            for policy in policies.values() if bool(policy.get("enabled"))
        }
        items=[]
        for item in discovered:
            provider_symbol = str(item["provider_symbol"])
            policy=policies.get(provider_symbol)
            if policy:
                item.update({
                    "canonical_instrument": policy["canonical_instrument"],
                    "enabled": policy["enabled"],
                    "allow_live": policy["allow_live"],
                    "allow_history": policy["allow_history"],
                    "priority_override": policy["priority_override"],
                    "mapping_state": "Confirmed" if policy["enabled"] else "Ignored",
                    "mapping_source": "policy",
                })
            else:
                item.update({"enabled": False, "allow_live": True, "allow_history": True, "priority_override": None, "mapping_state": "NeedsReview"})
            canonical = normalize_instrument(str(item["canonical_instrument"]))
            duplicate_of = active_by_canonical.get(canonical)
            if duplicate_of and duplicate_of != provider_symbol and not bool(item.get("enabled")):
                item["duplicate_of"] = duplicate_of
                item["mapping_state"] = "Duplicate"
            else:
                item["duplicate_of"] = None
            items.append(item)
        duplicate_count = sum(1 for item in items if item.get("duplicate_of"))
        return {"provider_key": provider_key, "count": len(items), "duplicate_count": duplicate_count, "items": items}

    @app.put("/api/symbol-policies")
    def upsert_symbol_policy(request: SymbolPolicyRequest) -> dict[str, object]:
        payload = request.model_dump()
        payload["canonical_instrument"] = normalize_instrument(payload["canonical_instrument"])
        if request.enabled:
            conflict = next((
                policy for policy in store.list_symbol_policies(
                    provider_key=request.provider_key,
                    instrument=payload["canonical_instrument"],
                )
                if bool(policy.get("enabled")) and str(policy["provider_symbol"]) != request.provider_symbol
            ), None)
            if conflict is not None:
                raise HTTPException(
                    409,
                    f'{payload["canonical_instrument"]} is already confirmed for provider symbol {conflict["provider_symbol"]}',
                )
        result = store.upsert_symbol_policy(**payload)
        worker = supervisor.get(request.provider_key)
        if worker is not None:
            configured = worker.config.normalized_symbols()
            if request.enabled and request.provider_symbol not in configured:
                worker.config.symbols = [*configured, request.provider_symbol]
                config_store.upsert(worker.config)
            elif not request.enabled and request.provider_symbol in configured:
                worker.config.symbols = [symbol for symbol in configured if symbol != request.provider_symbol]
                config_store.upsert(worker.config)
        supervisor.authority.replace_configs(supervisor._routing_configs(config_store.read_all()))
        events.record("info", "symbol.mapping", "MT5 symbol mapping saved", provider=request.provider_key, instrument=payload["canonical_instrument"], details={"provider_symbol": request.provider_symbol, "enabled": request.enabled, "priority_override": request.priority_override})
        return result

    @app.delete("/api/symbol-policies/{provider_key}/{provider_symbol:path}")
    def delete_symbol_policy(provider_key: str, provider_symbol: str) -> dict[str, object]:
        if not store.delete_symbol_policy(provider_key, provider_symbol):
            raise HTTPException(404, "Symbol policy not found")
        return {"removed": True}

    @app.get("/api/canonical-sources")
    def canonical_sources(instrument: str | None = None) -> dict[str, object]:
        configs = [
            {"provider_key": p.provider_key, "enabled": p.enabled, "priority": p.priority}
            for p in config_store.read_all()
        ]
        policies = store.list_symbol_policies(instrument=instrument)
        instruments = [instrument] if instrument else sorted({str(p["canonical_instrument"]) for p in policies})
        routes = [choose_canonical_source(item, configs, policies) for item in instruments]
        return {"count": len(routes), "routes": routes}

    @app.post("/api/providers/{provider_key}/test")
    def test_provider_connection(provider_key: str) -> dict[str, object]:
        worker = supervisor.get(provider_key)
        if worker is None:
            raise HTTPException(404, "Provider not found")
        config = worker.config
        if config.kind.lower() == "mock":
            result = {"ok": True, "provider_key": provider_key, "kind": "mock", "message": "Mock provider is available."}
            events.record("info", "provider.connection_test", "Provider connection test passed", provider=provider_key, details=result)
            return result
        if config.kind.lower() == "mt5":
            try:
                result = MetaTrader5TickProvider(
                    config.normalized_symbols(), config.terminal_path, config.provider_key,
                    config.batch_window_seconds, config.batch_limit, config.symbol_aliases, store,
                    config.account_login, config.account_server, config.password_env,
                    secret_store.get(config.provider_key),
                ).test_connection()
                payload = {"provider_key": provider_key, "kind": "mt5", **result}
                events.record("info", "provider.connection_test", "Provider connection test completed", provider=provider_key, details=payload)
                return payload
            except RuntimeError as exc:
                events.record("error", "provider.connection_test", "Provider connection test failed", provider=provider_key, details={"error": str(exc)})
                raise HTTPException(400, str(exc)) from exc
        if config.kind.lower() == "yahoo":
            try:
                result = YahooHistoricalProvider(config.provider_key).test_connection(config.normalized_symbols()[0])
                payload = {"provider_key": provider_key, "kind": "yahoo", **result}
                events.record("info", "provider.connection_test", "Provider connection test completed", provider=provider_key, details=payload)
                return payload
            except (RuntimeError, OSError, ValueError) as exc:
                events.record("error", "provider.connection_test", "Provider connection test failed", provider=provider_key, details={"error": str(exc)})
                raise HTTPException(400, str(exc)) from exc
        raise HTTPException(400, f"Unsupported provider kind: {config.kind}")

    @app.get("/api/providers/{provider_key}")
    def provider(provider_key: str) -> dict[str, object]:
        worker = supervisor.get(provider_key)
        if worker is None:
            raise HTTPException(404, "Provider not found")
        return worker.view()

    @app.put("/api/providers/{provider_key}")
    def upsert_provider(provider_key: str, request: ProviderRequest) -> dict[str, object]:
        if provider_key.lower() != request.provider_key.lower():
            raise HTTPException(400, "Route provider key must match request provider key")
        payload = request.model_dump()
        password = payload.pop("mt5_password", None)
        if payload.get("password_env") == "********":
            existing = supervisor.get(provider_key)
            payload["password_env"] = existing.config.password_env if existing is not None else None
        try:
            if password and password != "********":
                secret_store.set(provider_key, password)
        except SecretStoreError as exc:
            raise HTTPException(400, str(exc)) from exc
        return supervisor.upsert(ProviderConfig(**payload))

    @app.post("/api/providers/{provider_key}/{action}")
    def provider_action(provider_key: str, action: str) -> dict[str, object]:
        try:
            return supervisor.action(provider_key, action)
        except KeyError:
            raise HTTPException(404, "Provider not found") from None
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.delete("/api/providers/{provider_key}")
    def remove_provider(provider_key: str) -> dict[str, object]:
        if not supervisor.remove(provider_key):
            raise HTTPException(404, "Provider not found")
        return {"removed": True, "provider_key": provider_key}

    @app.post("/api/backfill")
    def backfill(request: BackfillRequest) -> dict[str, object]:
        worker = supervisor.get(request.provider_key)
        if worker is None:
            raise HTTPException(404, "Provider not found")
        kind = worker.config.kind.lower()
        if kind not in {"mt5", "yahoo"}:
            raise HTTPException(400, "Historical backfill requires an MT5 or Yahoo provider")
        end = datetime.now(UTC)
        start = end - timedelta(days=request.days)
        if kind == "mt5":
            provider = MetaTrader5TickProvider(worker.config.normalized_symbols(), worker.config.terminal_path, request.provider_key, symbol_aliases=worker.config.symbol_aliases, store=store, account_login=worker.config.account_login, account_server=worker.config.account_server, password_env=worker.config.password_env, password=secret_store.get(worker.config.provider_key))
            instrument = normalize_instrument(request.instrument or provider.symbol_resolver.resolve(request.provider_key, request.symbol).canonical_instrument)
        else:
            provider = YahooHistoricalProvider(request.provider_key)
            instrument = request.instrument or request.symbol
        try:
            events.record("info", "backfill.started", "Historical backfill started", provider=request.provider_key, instrument=instrument, details={"symbol": request.symbol, "timeframe": request.timeframe, "days": request.days})
            result = HistoricalBackfillService(store, calendar).run(provider, request.provider_key, request.symbol, instrument, request.timeframe, start, end)
            payload = result.__dict__ if hasattr(result, "__dict__") else {name: getattr(result, name) for name in result.__slots__}
            events.record("info", "backfill.completed", "Historical backfill completed", provider=request.provider_key, instrument=instrument, details=payload)
            return payload
        except (RuntimeError, ValueError) as exc:
            events.record("error", "backfill.failed", "Historical backfill failed", provider=request.provider_key, instrument=instrument, details={"error": str(exc), "timeframe": request.timeframe})
            store.set_ingestion_state(request.provider_key, instrument, request.timeframe, start, end, 0, 0, 0, "Failed", str(exc))
            raise HTTPException(400, str(exc)) from exc


    @app.post("/api/history/rebuild")
    def rebuild_history(request: HistoryRebuildRequest) -> dict[str, object]:
        worker = supervisor.get(request.provider_key)
        if worker is None:
            raise HTTPException(404, "Provider not found")
        if worker.config.kind.lower() != "mt5":
            raise HTTPException(400, "Authoritative rebuild currently requires an MT5 provider")
        provider = MetaTrader5TickProvider(
            worker.config.normalized_symbols(), worker.config.terminal_path, request.provider_key,
            symbol_aliases=worker.config.symbol_aliases, store=store,
            account_login=worker.config.account_login, account_server=worker.config.account_server,
            password_env=worker.config.password_env, password=secret_store.get(worker.config.provider_key),
        )
        end = datetime.now(UTC)
        start = end - timedelta(days=request.days)
        rebuilt: list[dict[str, object]] = []
        symbols = worker.config.normalized_symbols()
        for symbol in symbols:
            resolved = provider.symbol_resolver.resolve(request.provider_key, symbol)
            instrument = normalize_instrument(resolved.canonical_instrument)
            if request.instrument is not None and normalize_instrument(request.instrument) != instrument:
                continue
            try:
                result = AuthoritativeHistoryRebuildService(store).run(
                    provider, request.provider_key, symbol, instrument, start, end
                )
                rebuilt.append({name: getattr(result, name) for name in result.__slots__})
            except (RuntimeError, ValueError) as exc:
                events.record(
                    "error", "history.rebuild_failed", "Authoritative history rebuild failed",
                    provider=request.provider_key, instrument=instrument, details={"error": str(exc)},
                )
                raise HTTPException(400, f"{instrument}: {exc}") from exc
        if request.instrument is not None and not rebuilt:
            raise HTTPException(404, "Instrument is not configured for this provider")
        return {"provider": request.provider_key, "days": request.days, "rebuilt": rebuilt}

    @app.get("/api/backfill/state")
    def backfill_state() -> dict[str, object]:
        values = store.list_ingestion_state()
        return {"count": len(values), "items": values}

    @app.get("/api/gaps")
    def gaps(
        limit: int = Query(default=200, ge=1, le=5000),
        provider: str | None = None,
        instrument: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, object]:
        values = store.list_gaps(limit, provider, instrument, timeframe)
        return {"count": len(values), "items": values}

    @app.post("/api/gaps/scan")
    def scan_gaps(request: GapScanRequest) -> dict[str, object]:
        end = datetime.now(UTC)
        start = end - timedelta(days=request.days)
        count = HistoricalBackfillService(store, calendar).detect_gaps(
            request.provider_key, request.instrument, request.timeframe, start, end
        )
        return {"provider": request.provider_key, "instrument": request.instrument,
                "timeframe": request.timeframe, "gaps": count}

    @app.post("/api/gaps/repair")
    def repair_gaps(request: GapRepairRequest) -> dict[str, object]:
        worker = supervisor.get(request.provider_key)
        if worker is None:
            raise HTTPException(404, "Provider not found")
        kind = worker.config.kind.lower()
        if kind not in {"mt5", "yahoo"}:
            raise HTTPException(400, "Gap repair requires an MT5 or Yahoo provider")
        if kind == "mt5":
            provider = MetaTrader5TickProvider(
                worker.config.normalized_symbols(), worker.config.terminal_path, request.provider_key, store=store,
                account_login=worker.config.account_login, account_server=worker.config.account_server,
                password_env=worker.config.password_env,
                password=secret_store.get(worker.config.provider_key),
            )
            symbol_map = {
                provider._canonical_symbol(symbol): symbol
                for symbol in worker.config.normalized_symbols()
            }
        else:
            provider = YahooHistoricalProvider(request.provider_key)
            policies = store.list_symbol_policies(provider_key=request.provider_key)
            symbol_map = {
                str(policy["canonical_instrument"]): str(policy["provider_symbol"])
                for policy in policies if bool(policy["allow_history"])
            }
            for symbol in worker.config.normalized_symbols():
                symbol_map.setdefault(symbol, YahooHistoricalProvider.resolve_symbol(symbol))
        try:
            result = HistoricalBackfillService(store).repair_gaps(
                provider, request.provider_key, symbol_map,
                request.instrument, request.timeframe, request.limit,
            )
            return {name: getattr(result, name) for name in result.__slots__}
        except (RuntimeError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/api/gaps/repairs")
    def repair_history(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, object]:
        values = store.list_repair_runs(limit)
        return {"count": len(values), "items": values}

    @app.post("/api/quality/scan")
    def quality_scan(request: QualityScanRequest) -> dict[str, object]:
        from decimal import Decimal
        result = quality.scan(request.provider, request.instrument, request.timeframe,
                              request.limit, Decimal(str(request.max_move_percent)))
        return {name: getattr(result, name) for name in result.__slots__}

    @app.get("/api/quality/issues")
    def quality_issues(limit: int = Query(default=500, ge=1, le=5000), action: str | None = None) -> dict[str, object]:
        items = quality.list_issues(limit, action)
        return {"count": len(items), "items": items}

    @app.post("/api/quality/issues/{issue_id}/quarantine")
    def quality_quarantine(issue_id: int) -> dict[str, object]:
        try:
            return quality.quarantine(issue_id)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.post("/api/quality/issues/{issue_id}/rebuild")
    def quality_rebuild(issue_id: int) -> dict[str, object]:
        try:
            return quality.rebuild_one_minute(issue_id)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/api/instruments")
    def instruments() -> dict[str, object]:
        values = store.list_instruments()
        return {"count": len(values), "instruments": values}

    @app.get("/api/candles")
    def candles(
        instrument: str,
        timeframe: str = "1m",
        limit: int = Query(default=200, ge=1, le=5000),
        provider: str | None = None,
        from_utc: datetime | None = None,
        to_utc: datetime | None = None,
    ) -> dict[str, object]:
        if from_utc is not None and to_utc is not None and from_utc > to_utc:
            raise HTTPException(400, "from_utc cannot be later than to_utc")
        active_provider = provider
        if active_provider is None:
            configs = [
                {"provider_key": p.provider_key, "enabled": p.enabled, "priority": p.priority}
                for p in config_store.read_all()
            ]
            route = choose_canonical_source(instrument, configs, store.list_symbol_policies(instrument=instrument))
            preferred = route.get("preferred")
            if isinstance(preferred, dict):
                active_provider = str(preferred["provider_key"])
            if active_provider is None:
                active_provider = store.preferred_candle_provider(instrument, timeframe)
        values = store.read_candles(instrument, timeframe, limit, active_provider, from_utc, to_utc)
        if provider is None and not values:
            available_provider = store.preferred_candle_provider(instrument, timeframe)
            if available_provider is not None and available_provider != active_provider:
                active_provider = available_provider
                values = store.read_candles(instrument, timeframe, limit, active_provider, from_utc, to_utc)
        if active_provider is not None:
            temporary = bridge.temporary_candle(active_provider, instrument, timeframe)
            if temporary is not None:
                in_window = (from_utc is None or temporary.open_time >= ensure_server_local(from_utc)) and (
                    to_utc is None or temporary.open_time <= ensure_server_local(to_utc)
                )
                if in_window:
                    values = [item for item in values if item.open_time != temporary.open_time]
                    values = [*values, temporary]
                    if len(values) > limit:
                        values = values[-limit:]
        return {
            "instrument": instrument,
            "timeframe": timeframe,
            "source": "Axetos Market Data Server",
            "active_provider": active_provider,
            "authority": "Axetos Market Data Server",
            "candle_generation": "server",
            "count": len(values),
            "candles": [
                {
                    "provider": c.provider,
                    "open_time_utc": c.open_time.isoformat(),
                    "open": str(c.open),
                    "high": str(c.high),
                    "low": str(c.low),
                    "close": str(c.close),
                    "tick_count": c.tick_count,
                    "volume": None if c.volume is None else str(c.volume),
                    "complete": c.complete,
                }
                for c in values
            ],
        }

    @app.get("/api/quote")
    def latest_quote_query(instrument: str, provider: str | None = None) -> dict[str, object]:
        return latest_quote(instrument, provider)

    @app.get("/api/quotes/{instrument:path}")
    def latest_quote(instrument: str, provider: str | None = None) -> dict[str, object]:
        # Keep one provider authoritative for a live quote stream. Selecting whichever
        # provider happened to post last made the client oscillate between brokers and
        # produced artificial spread spikes. Explicit provider requests are strict.
        active_provider = provider
        value = None
        if active_provider is None:
            configs = [
                {"provider_key": p.provider_key, "enabled": p.enabled, "priority": p.priority}
                for p in config_store.read_all()
            ]
            route = choose_canonical_source(
                instrument, configs, store.list_symbol_policies(instrument=instrument)
            )
            preferred = route.get("preferred")
            if isinstance(preferred, dict):
                active_provider = str(preferred["provider_key"])

        active_provider_symbol = (
            store.preferred_live_symbol(active_provider, instrument)
            if active_provider is not None else None
        )
        if active_provider is not None:
            value = store.latest_bridge_quote(instrument, active_provider, active_provider_symbol)

        # Availability fallback is allowed only when the canonical provider has no
        # recent quote. It is not evaluated on every tick, preventing provider hopping.
        now = server_now()
        if provider is None and active_provider_symbol is None:
            canonical_is_stale = (
                value is not None and
                now - datetime.fromisoformat(str(value["received_utc"])) > timedelta(seconds=30)
            )
            if value is None or canonical_is_stale:
                fallback = store.latest_bridge_quote(instrument)
                if fallback is not None:
                    fallback_received = datetime.fromisoformat(str(fallback["received_utc"]))
                    # With no canonical quote, return the newest known value and let the
                    # response timestamp communicate staleness. Replace an existing
                    # canonical quote only when the fallback is fresh.
                    if value is None or now - fallback_received <= timedelta(seconds=30):
                        value = fallback
                        active_provider = str(fallback["provider_key"])

        if value is None:
            raise HTTPException(404, "No quote is available for the requested instrument")
        bid = Decimal(str(value["bid"]))
        ask = Decimal(str(value["ask"]))

        # A fresh bridge heartbeat is connection evidence, not proof that the market is
        # trading.  When the newest quote is still equal to the last completed candle
        # after the short-gap window, expose no tradable quote.  This prevents weekend
        # heartbeats from keeping bid/ask and a temporary flat candle alive in clients.
        quote_mid = (bid + ask) / Decimal("2")
        latest_completed = next((
            candle for candle in reversed(store.read_candles(instrument, "1m", 10, active_provider))
            if candle.complete
        ), None)
        feed_report = next((
            report for report in supervisor.feed_reports()
            if report.get("provider") == active_provider and report.get("instrument") == instrument
        ), None)
        unchanged_closed = (
            latest_completed is not None
            and now - latest_completed.open_time >= timedelta(minutes=60)
            and quote_mid == latest_completed.close
        )
        inactive_feed = feed_report is not None and feed_report.get("feed_state") in {"STALLED", "INACTIVE"}
        if unchanged_closed or inactive_feed:
            raise HTTPException(404, "Market is not producing a current tradable quote")

        return {
            "instrument": instrument,
            "provider": value["provider_key"],
            "time_utc": value["source_time_utc"],
            "received_utc": value["received_utc"],
            "bid": str(bid),
            "ask": str(ask),
            "mid": str((bid + ask) / Decimal("2")),
            "source": value["provider_key"],
            "delayed": False,
        }

    @app.get("/api/market-data/quote/{instrument:path}")
    def compatibility_quote(instrument: str, provider: str | None = None) -> dict[str, object]:
        return latest_quote(instrument, provider)

    @app.get("/api/market-data/candles")
    def compatibility_candles(
        instrument: str,
        interval: str = "1m",
        limit: int = Query(default=200, ge=1, le=5000),
        provider: str | None = None,
        from_: datetime | None = Query(default=None, alias="from"),
        to: datetime | None = None,
        trace: bool = False,
    ) -> dict[str, object]:
        payload = candles(instrument, interval, limit, provider, from_, to)
        return {
            "source": payload["source"],
            "active_provider": payload["active_provider"],
            "interval": interval,
            "authority": payload["authority"],
            "candle_generation": payload["candle_generation"],
            "candles": [
                {
                    "time_utc": item["open_time_utc"],
                    "open": item["open"],
                    "high": item["high"],
                    "low": item["low"],
                    "close": item["close"],
                    "is_partial": not item["complete"],
                    "is_synthetic_missing": False,
                }
                for item in payload["candles"]
            ],
        }

    return app


CONTROL_CENTER_HTML = r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Axetos Market Data Server</title><style>
:root{color-scheme:dark;font-family:Inter,Segoe UI,sans-serif;background:#0c111b;color:#edf2f7}body{margin:0}header{padding:20px 28px;border-bottom:1px solid #253047;background:#111827;display:flex;justify-content:space-between;align-items:center}h1{font-size:21px;margin:0}.sub{font-size:12px;color:#93c5fd;margin-top:4px}.wrap{padding:24px;max-width:1280px;margin:auto}.stats,.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px}.grid{grid-template-columns:repeat(auto-fit,minmax(340px,1fr));margin-top:18px}.card{border:1px solid #26334a;background:#121a29;border-radius:12px;padding:17px}.value{font-size:25px;font-weight:800}.label{color:#94a3b8;font-size:12px}.top{display:flex;justify-content:space-between;gap:12px}.name{font-size:18px;font-weight:700}.key{color:#94a3b8;font-size:12px}.status{font-size:12px;font-weight:800;padding:5px 9px;border-radius:999px;background:#334155}.status.live{background:#14532d;color:#bbf7d0}.status.failed{background:#7f1d1d;color:#fecaca}.feed-state{display:inline-block;font-size:11px;font-weight:800;padding:4px 8px;border-radius:999px}.feed-live{background:#14532d;color:#bbf7d0}.feed-quiet{background:#854d0e;color:#fef08a}.feed-stalled{background:#9a3412;color:#fed7aa}.feed-inactive{background:#7f1d1d;color:#fecaca}.feed-partial{background:#854d0e;color:#fef08a}.feed-recovering{background:#1e3a8a;color:#bfdbfe}.feed-initializing{background:#334155;color:#cbd5e1}.rows{margin-top:15px;display:grid;grid-template-columns:1fr 1fr;gap:9px 14px;font-size:13px}button{background:#2563eb;color:white;border:0;border-radius:7px;padding:9px 13px;font-weight:600;cursor:pointer}.secondary{background:#334155}.danger{background:#991b1b}.actions{display:flex;flex-wrap:wrap;gap:7px;margin-top:15px}dialog{background:#111827;color:#fff;border:1px solid #334155;border-radius:12px;width:min(720px,90vw)}form{display:grid;grid-template-columns:1fr 1fr;gap:12px}label{font-size:12px;color:#cbd5e1}input,select{width:100%;box-sizing:border-box;margin-top:5px;padding:9px;border-radius:6px;border:1px solid #334155;background:#0f172a;color:#fff}.wide{grid-column:1/-1}.toolbar{display:flex;justify-content:space-between;align-items:flex-start;gap:14px;margin:20px 0 8px}.toolbar-actions{display:flex;flex-direction:column;align-items:flex-end;gap:7px}.toolbar-row{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:7px}.error{color:#fca5a5;font-size:12px;margin-top:8px}.muted{color:#94a3b8;font-size:12px}.symbol-dialog{width:min(1120px,94vw);max-height:88vh;padding:18px}.symbol-dialog::backdrop{background:rgba(2,6,23,.72)}.symbol-toolbar{display:grid;grid-template-columns:minmax(240px,1fr) auto auto;gap:9px;align-items:end;margin:16px 0 10px}.symbol-toolbar input{margin-top:0}.symbol-list{margin-top:10px;border:1px solid #26334a;border-radius:9px;max-height:62vh;overflow:auto;background:#0d1524}.symbol-row{display:grid;grid-template-columns:minmax(190px,1.25fr) minmax(190px,1fr) minmax(150px,.85fr) 110px auto;gap:12px;align-items:end;padding:13px;border-bottom:1px solid #26334a}.symbol-row:last-child{border-bottom:0}.symbol-info{min-width:0;align-self:center}.symbol-info strong{display:block;font-size:14px;overflow-wrap:anywhere}.symbol-info small{display:block;margin-top:3px;color:#94a3b8;line-height:1.25;overflow-wrap:anywhere}.symbol-field{display:block;min-width:0}.symbol-field>span{display:block;color:#94a3b8;font-size:11px;margin-bottom:5px}.symbol-field input,.symbol-field select{margin-top:0}.symbol-save{align-self:end;white-space:nowrap}.duplicate-note{display:block;margin-top:5px;color:#fbbf24;font-size:11px;font-weight:600}.benchmark-dialog{width:min(760px,92vw)}.benchmark-warning{border:1px solid #92400e;background:#451a03;color:#fde68a;border-radius:8px;padding:11px;margin:12px 0}.benchmark-results{margin-top:14px;border:1px solid #26334a;border-radius:8px;overflow:hidden}.benchmark-result{display:grid;grid-template-columns:90px 1fr 1fr;gap:10px;padding:10px;border-bottom:1px solid #26334a}.benchmark-result:last-child{border-bottom:0}.benchmark-best{background:#12351f}.benchmark-progress{margin-top:12px;color:#93c5fd}@media(max-width:850px){.symbol-dialog{width:min(720px,94vw)}.symbol-toolbar{grid-template-columns:1fr 1fr}.symbol-toolbar input{grid-column:1/-1}.symbol-row{grid-template-columns:1fr 1fr}.symbol-info{grid-column:1/-1}.symbol-save{width:100%}}@media(max-width:560px){.symbol-toolbar,.symbol-row{grid-template-columns:1fr}.symbol-toolbar input,.symbol-info{grid-column:auto}}</style></head><body>
<header><div><h1>Axetos Market Data Server</h1><div class="sub">Market-data infrastructure only · no orders, accounts, positions, or trading logic</div></div><button onclick="openAdd()">+ Add provider</button></header><main class="wrap"><section id="stats" class="stats"></section><div class="toolbar"><strong>Provider control center</strong><div class="toolbar-actions"><div class="toolbar-row"><button class="secondary" onclick="scanQuality()">Scan candle quality</button><button class="secondary" onclick="testAlert()">Test alert</button><button class="secondary" onclick="integrityCheck()">Check database</button><button class="secondary" onclick="createBackup()">Create backup</button><button class="secondary" onclick="openBenchmark()">Run benchmark</button></div><div class="toolbar-row"><button class="secondary" onclick="previewCleanup()">Preview cleanup</button><button class="secondary" onclick="configureRetentionSchedule()">Schedule daily cleanup</button><button class="danger" onclick="runCleanup()">Run cleanup</button><span id="count"></span></div></div></div><section id="providers" class="grid"></section><div class="toolbar"><strong>Operational log</strong><span><select id="eventSeverity" onchange="loadEvents()"><option value="">All severities</option><option>info</option><option>warning</option><option>error</option><option>critical</option></select> <input id="eventSearch" placeholder="Search events" oninput="eventSearchChanged()"> <button class="secondary" onclick="exportEvents('csv')">Export CSV</button> <button class="secondary" onclick="exportEvents('jsonl')">Export JSONL</button></span></div><section class="card"><div id="events"></div><div class="actions"><button class="secondary" onclick="previousEvents()">Previous</button><span id="eventPage" class="label"></span><button class="secondary" onclick="nextEvents()">Next</button></div></section></main>
<dialog id="editor"><h2 id="title">Add provider</h2><form id="form"><label>Provider key<input name="provider_key" required placeholder="ICMarkets.MT5"></label><label>Display name<input name="display_name" required placeholder="IC Markets MT5"></label><label>Provider type<select name="kind"><option value="mock">Mock</option><option value="mt5">MetaTrader 5</option></select></label><label>Polling interval<input name="poll_interval_seconds" type="number" step="0.1" value="1"></label><label id="symbolsField" class="wide">Symbols, comma separated<input name="symbols" value="EUR/USD"><small class="muted">Used by non-MT5 providers. MT5 symbols are selected through Manage symbols after the provider is saved.</small></label><label class="wide">MT5 terminal path<input name="terminal_path" placeholder="Optional terminal64.exe path"></label><label>MT5 account login<input name="account_login" type="number" min="1" placeholder="Optional account number"></label><label>MT5 broker server<input name="account_server" placeholder="Example: OANDA-Demo-1"></label><label class="wide">MT5 password<input name="mt5_password" type="password" autocomplete="new-password" placeholder="Enter password to set or replace"><small class="muted">Stored in the protected local secret store, never in providers.json. Leave masked/blank to preserve the saved password.</small></label><label>Priority<input name="priority" type="number" min="0" value="100"></label><label>Fallback after seconds<input name="fallback_after_seconds" type="number" min="0.1" step="0.1" value="10"></label><label>Batch window seconds<input name="batch_window_seconds" type="number" min="1" value="5"></label><label>Batch limit<input name="batch_limit" type="number" min="100" value="50000"></label><label>Maintenance interval minutes<input name="maintenance_interval_minutes" type="number" min="1" value="60"></label><label>Maintenance backfill days<input name="maintenance_backfill_days" type="number" min="1" value="2"></label><label>Feed quiet after seconds<input name="feed_quiet_seconds" type="number" min="1" value="60"></label><label>Feed stalled after seconds<input name="feed_stalled_seconds" type="number" min="2" value="180"></label><label>Feed inactive after seconds<input name="feed_inactive_seconds" type="number" min="3" value="600"></label><label><input name="maintenance_enabled" type="checkbox" style="width:auto"> Scheduled maintenance</label><label><input name="enabled" type="checkbox" checked style="width:auto"> Enabled</label><label><input name="auto_start" type="checkbox" checked style="width:auto"> Auto-start</label><div id="error" class="error wide"></div><div class="actions wide"><button type="button" class="secondary" onclick="editor.close()">Cancel</button><button type="submit">Save</button></div></form></dialog>
<dialog id="symbolEditor" class="symbol-dialog"><div class="top"><div><h2 id="symbolTitle" style="margin:0">Manage symbols</h2><div class="muted">Review broker symbols and map them to canonical Axetos instruments.</div></div><button class="secondary" onclick="symbolEditor.close()">Close</button></div><div class="symbol-toolbar"><input id="symbolSearch" placeholder="Search symbol or description"><button onclick="discoverSymbols()">Discover from MT5</button><button class="secondary" onclick="showConfiguredSymbols()">Configured only</button></div><div id="symbolSummary" class="muted"></div><div id="symbolList" class="symbol-list"></div></dialog>
<dialog id="benchmarkEditor" class="benchmark-dialog"><div class="top"><div><h2 style="margin:0">Performance benchmark</h2><div class="muted">Administrator diagnostics for synthetic ingestion throughput.</div></div><button class="secondary" onclick="benchmarkEditor.close()">Close</button></div><div class="benchmark-warning">Benchmarking creates heavy synthetic database load and can affect live ingestion. Run it only on a development or maintenance instance.</div><form id="benchmarkForm"><label>Ticks<input name="ticks" type="number" min="1000" max="10000000" value="100000"></label><label>Instruments<input name="instruments" type="number" min="1" max="10000" value="10"></label><label class="wide">Batch sizes, comma separated<input name="batch_sizes" value="1000,5000,10000"><small class="muted">Use one value for a single run or several values to compare them.</small></label><div class="actions wide"><button type="submit">Start benchmark</button></div></form><div id="benchmarkProgress" class="benchmark-progress">No benchmark has been started.</div><div id="benchmarkResults" class="benchmark-results" style="display:none"></div></dialog>
<script>
const root=document.getElementById('providers'),eventsRoot=document.getElementById('events'),eventPageLabel=document.getElementById('eventPage'),stats=document.getElementById('stats'),count=document.getElementById('count'),editor=document.getElementById('editor'),form=document.getElementById('form'),title=document.getElementById('title'),error=document.getElementById('error');let editing=null,editingSymbols=[];const esc=v=>String(v??'').replace(/[&<>\"]/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[x]));const apiError=(payload,fallback)=>{const detail=payload?.detail??payload?.message??fallback;if(Array.isArray(detail))return detail.map(item=>item?.msg||item?.message||JSON.stringify(item)).join('; ');if(detail&&typeof detail==='object')return detail.msg||detail.message||JSON.stringify(detail);return String(detail||fallback)};
let eventPageNumber=1,eventPages=1,eventSearchTimer=null;async function load(){const [pr,sr,hr,mr,br,ar,fr]=await Promise.all([fetch('/api/providers'),fetch('/api/statistics'),fetch('/api/health'),fetch('/api/metrics'),fetch('/api/market-data/mt5/bridge/status'),fetch('/api/alerts/status'),fetch('/api/feed-status')]);const p=await pr.json(),s=await sr.json(),h=await hr.json(),m=await mr.json(),b=await br.json(),a=await ar.json(),f=await fr.json();const configuredInstruments=new Set(),selectedInstruments=new Set(),monitoredInstruments=new Set();for(const provider of p.providers){for(const symbol of (provider.symbols||[])){configuredInstruments.add(symbol.canonical_instrument);if(symbol.selected)selectedInstruments.add(symbol.canonical_instrument)}for(const feed of (provider.feeds||[]))monitoredInstruments.add(feed.instrument)}stats.innerHTML=[['Health',h.status],['Market feed',f.overall_state],['Connected providers',m.providers_live+'/'+m.providers_configured],['Configured instruments',configuredInstruments.size],['Selected instruments',selectedInstruments.size],['Monitored feeds',monitoredInstruments.size],['Stored instruments',s.instruments],['Ticks',s.ticks],['Candles',s.candles],['Database size',(m.database_size_bytes/1048576).toFixed(2)+' MB'],['Latest tick',s.latest_tick_utc?new Date(s.latest_tick_utc).toLocaleString():'-'],['Unresolved gaps',s.unresolved_gaps],['Quality issues',s.unresolved_quality_issues],['MT5 bridge terminals',b.heartbeats.length],['Alerts',a.enabled?'Configured':'Disabled'],['Bridge instruments',b.discovered_instruments],['Bridge queue',b.queue.queue_depth]].map(x=>`<div class="card"><div class="label">${x[0]}</div><div class="value">${x[1]}</div></div>`).join('');count.textContent=`${p.providers.length} configured`;root.innerHTML=p.providers.length?'':'<div class="card">No providers configured.</div>';for(const x of p.providers){const c=x.configuration,r=x.runtime,feeds=Array.isArray(x.feeds)?x.feeds:[],e=document.createElement('article');e.className='card';const feedRows=feeds.length?feeds.map(feed=>`<div><span class="label">${esc(feed.instrument)} feed</span><br><span class="feed-state feed-${String(feed.feed_state).toLowerCase()}">${esc(feed.feed_state)}</span> · ${Math.round(feed.unchanged_seconds)}s unchanged</div>`).join(''):'<div><span class="label">Market feed</span><br><span class="feed-state feed-inactive">INACTIVE</span></div>';e.innerHTML=`<div class="top"><div><div class="name">${esc(c.display_name)}</div><div class="key">${esc(c.provider_key)} · ${esc(c.kind)}</div></div><span class="status ${String(r.status==='Live'?'Connected':r.status).toLowerCase()}">${esc(r.status==='Live'?'Connected':r.status)}</span></div><div class="rows"><div><span class="label">MT5 terminal / broker</span><br>${c.kind==='mt5'?(r.status==='Failed'?`FAILED / DISCONNECTED`:`${r.terminal_running?'RUNNING':'STOPPED'} / ${r.broker_connected?'CONNECTED':'DISCONNECTED'}`):'-'}</div><div><span class="label">MT5 account</span><br>${c.kind==='mt5'?(r.account_logged_in?`${esc(r.account_login||'')} · ${esc(r.account_server||'')}`:'NOT LOGGED IN'):'-'}</div><div><span class="label">Provider observations</span><br>${r.ticks_received}</div><div><span class="label">Accepted / unchanged</span><br>${r.accepted_market_ticks||0} / ${r.ignored_unchanged_updates||0}</div><div><span class="label">Last observation</span><br>${r.last_tick_utc?new Date(r.last_tick_utc).toLocaleString():'-'}</div><div><span class="label">Auto-start</span><br>${c.auto_start?'Yes':'No'}</div><div><span class="label">Priority / fallback</span><br>${c.priority} / ${c.fallback_after_seconds}s</div><div><span class="label">Authoritative / standby observations</span><br>${r.authoritative_ticks} / ${r.standby_ticks}</div><div><span class="label">Recovery attempts / candles</span><br>${r.recovery_attempts||0} / ${r.recovery_candles_written||0}</div>${feedRows}<div><span class="label">Maintenance</span><br>${x.maintenance?`${esc(x.maintenance.status)} · next ${x.maintenance.next_run_utc?new Date(x.maintenance.next_run_utc).toLocaleString():'-'}`:'Disabled'}</div></div><div class="actions"><button onclick="editProvider('${esc(c.provider_key)}')">Edit</button>${c.kind==='mt5'?`<button onclick="manageSymbols('${esc(c.provider_key)}')">Manage symbols</button>`:''}<button onclick="act('${esc(c.provider_key)}','start')">Start</button><button class="secondary" onclick="act('${esc(c.provider_key)}','stop')">Stop</button><button class="secondary" onclick="testProvider('${esc(c.provider_key)}')">Test connection</button>${c.kind==='mt5'?`<button class="secondary" onclick="fullHistory('${esc(c.provider_key)}')">Download full MT5 history</button>`:''}${c.kind==='mt5'&&c.maintenance_enabled?`<button class="secondary" onclick="act('${esc(c.provider_key)}','maintenance')">Run maintenance</button>`:''}<button class="danger" onclick="removeProvider('${esc(c.provider_key)}')">Remove</button></div>${r.last_error?`<div class="error">${esc(r.last_error)}</div>`:''}`;root.appendChild(e)}}
function updateProviderForm(){const isMt5=form.elements.kind.value==='mt5';symbolsField.style.display=isMt5?'none':'';if(!isMt5&&!form.elements.symbols.value.trim())form.elements.symbols.value='EUR/USD'}
function openAdd(){editing=null;editingSymbols=[];title.textContent='Add provider';form.reset();form.elements.poll_interval_seconds.value=1;form.elements.symbols.value='EUR/USD';form.elements.priority.value=100;form.elements.fallback_after_seconds.value=10;form.elements.batch_window_seconds.value=5;form.elements.batch_limit.value=50000;form.elements.maintenance_interval_minutes.value=60;form.elements.maintenance_backfill_days.value=2;form.elements.feed_quiet_seconds.value=60;form.elements.feed_stalled_seconds.value=180;form.elements.feed_inactive_seconds.value=600;form.elements.maintenance_enabled.checked=false;form.elements.enabled.checked=true;form.elements.auto_start.checked=true;form.elements.provider_key.readOnly=false;error.textContent='';updateProviderForm();editor.showModal()}
async function editProvider(key){const r=await fetch('/api/providers/'+encodeURIComponent(key)),x=await r.json(),c=x.configuration;editing=key;editingSymbols=Array.isArray(c.symbols)?c.symbols:[];title.textContent='Edit '+c.display_name;form.elements.provider_key.value=c.provider_key;form.elements.provider_key.readOnly=true;form.elements.display_name.value=c.display_name;form.elements.kind.value=c.kind;form.elements.poll_interval_seconds.value=c.poll_interval_seconds;form.elements.symbols.value=c.symbols.join(',');updateProviderForm();form.elements.terminal_path.value=c.terminal_path||'';form.elements.account_login.value=c.account_login||'';form.elements.account_server.value=c.account_server||'';form.elements.mt5_password.value=c.password_configured?'********':'';form.elements.priority.value=c.priority;form.elements.fallback_after_seconds.value=c.fallback_after_seconds;form.elements.batch_window_seconds.value=c.batch_window_seconds;form.elements.batch_limit.value=c.batch_limit;form.elements.maintenance_interval_minutes.value=c.maintenance_interval_minutes;form.elements.maintenance_backfill_days.value=c.maintenance_backfill_days;form.elements.feed_quiet_seconds.value=c.feed_quiet_seconds??60;form.elements.feed_stalled_seconds.value=c.feed_stalled_seconds??180;form.elements.feed_inactive_seconds.value=c.feed_inactive_seconds??600;form.elements.maintenance_enabled.checked=c.maintenance_enabled;form.elements.enabled.checked=c.enabled;form.elements.auto_start.checked=c.auto_start;editor.showModal()}
async function loadEvents(){const severity=document.getElementById('eventSeverity').value,search=document.getElementById('eventSearch').value;const q=new URLSearchParams({page:String(eventPageNumber),page_size:'25'});if(severity)q.set('severity',severity);if(search)q.set('search',search);const r=await fetch('/api/operational-events?'+q),x=await r.json();eventPages=Math.max(1,x.pages||1);eventPageNumber=Math.min(eventPageNumber,eventPages);eventPageLabel.textContent=`Page ${eventPageNumber} of ${eventPages} · ${x.total} events`;eventsRoot.innerHTML=x.items.length?x.items.map(e=>`<div style="padding:9px 0;border-bottom:1px solid #26334a"><strong>${esc(e.severity.toUpperCase())}</strong> · ${esc(e.category)} · ${new Date(e.timestamp_utc).toLocaleString()}<br><span>${esc(e.message)}</span>${e.provider?`<br><span class="label">${esc(e.provider)}${e.instrument?' · '+esc(e.instrument):''}</span>`:''}</div>`).join(''):'<div class="label">No operational events found.</div>'}function previousEvents(){if(eventPageNumber>1){eventPageNumber--;loadEvents()}}function nextEvents(){if(eventPageNumber<eventPages){eventPageNumber++;loadEvents()}}function eventSearchChanged(){clearTimeout(eventSearchTimer);eventSearchTimer=setTimeout(()=>{eventPageNumber=1;loadEvents()},250)}function exportEvents(format){const severity=document.getElementById('eventSeverity').value,search=document.getElementById('eventSearch').value;const q=new URLSearchParams({format,limit:'50000'});if(severity)q.set('severity',severity);if(search)q.set('search',search);window.location='/api/operational-events/export?'+q.toString()}

let symbolProvider=null,symbolItems=[],symbolDuplicateCount=0,symbolDiscoveryActive=false;
async function manageSymbols(key){symbolProvider=key;symbolItems=[];symbolDuplicateCount=0;symbolDiscoveryActive=false;symbolTitle.textContent='Manage symbols · '+key;symbolSearch.value='';symbolEditor.showModal();await loadSymbols(false)}
async function discoverSymbols(){symbolDiscoveryActive=true;await loadSymbols(true)}
async function showConfiguredSymbols(){symbolDiscoveryActive=false;symbolItems=[];await loadSymbols(false)}
function renderSymbols(){const query=symbolSearch.value.trim().toLowerCase(),items=query?symbolItems.filter(s=>String(s.provider_symbol||'').toLowerCase().includes(query)||String(s.description||'').toLowerCase().includes(query)||String(s.canonical_instrument||'').toLowerCase().includes(query)):symbolItems;symbolSummary.textContent=`${items.length} symbols · ${symbolDuplicateCount} duplicate alternatives · automatic mappings remain disabled until reviewed`;symbolList.innerHTML=items.map((s,i)=>`<div class="symbol-row"><div class="symbol-info"><strong>${esc(s.provider_symbol)}</strong><small>${esc(s.description||'No broker description')}</small>${s.duplicate_of?`<span class="duplicate-note">Alternative to active ${esc(s.duplicate_of)}</span>`:''}</div><label class="symbol-field"><span>Canonical instrument</span><input id="canon_${i}" value="${esc(s.canonical_instrument)}"></label><label class="symbol-field"><span>Mapping state</span><select id="state_${i}"><option value="confirmed" ${s.mapping_state==='Confirmed'?'selected':''}>Confirmed</option><option value="review" ${s.mapping_state==='NeedsReview'?'selected':''}>Needs review</option><option value="ignored" ${s.mapping_state==='Ignored'||s.mapping_state==='Duplicate'?'selected':''}>${s.mapping_state==='Duplicate'?'Duplicate / ignored':'Ignored'}</option></select></label><label class="symbol-field"><span>Priority</span><input id="priority_${i}" type="number" min="0" placeholder="Default" value="${s.priority_override??''}"></label><button class="symbol-save" onclick='saveSymbol(${JSON.stringify(s.provider_symbol)},${i})'>Save</button></div>`).join('')||'<div style="padding:16px">No symbols found.</div>'}
async function loadSymbols(refresh){if(!symbolProvider)return;symbolList.innerHTML='<div style="padding:16px">Loading…</div>';const q=new URLSearchParams({refresh:String(refresh),limit:'5000'});const r=await fetch('/api/providers/'+encodeURIComponent(symbolProvider)+'/symbols?'+q),x=await r.json();if(!r.ok){symbolList.innerHTML='<div class="error" style="padding:16px">'+esc(x.detail||'Could not load symbols')+'</div>';return}symbolItems=Array.isArray(x.items)?x.items:[];symbolDuplicateCount=x.duplicate_count||0;renderSymbols()}
async function saveSymbol(providerSymbol,index){const visibleItems=symbolSearch.value.trim()?symbolItems.filter(s=>String(s.provider_symbol||'').toLowerCase().includes(symbolSearch.value.trim().toLowerCase())||String(s.description||'').toLowerCase().includes(symbolSearch.value.trim().toLowerCase())||String(s.canonical_instrument||'').toLowerCase().includes(symbolSearch.value.trim().toLowerCase())):symbolItems,target=visibleItems[index];const state=document.getElementById('state_'+index).value,canonical=document.getElementById('canon_'+index).value.trim(),priority=document.getElementById('priority_'+index).value;const payload={provider_key:symbolProvider,provider_symbol:providerSymbol,canonical_instrument:canonical,enabled:state==='confirmed',allow_live:state==='confirmed',allow_history:state==='confirmed',priority_override:priority===''?null:Number(priority)};const r=await fetch('/api/symbol-policies',{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(payload)}),x=await r.json();if(!r.ok){alert(x.detail||'Could not save mapping');return}if(symbolDiscoveryActive&&target){target.canonical_instrument=canonical;target.mapping_state=state==='confirmed'?'Confirmed':state==='review'?'NeedsReview':'Ignored';target.priority_override=payload.priority_override;renderSymbols()}else{await loadSymbols(false)}load()}
symbolSearch.addEventListener('input',()=>{if(symbolItems.length)renderSymbols()})
async function fullHistory(key){if(!confirm('Download all available MT5 M1, H1 and D1 source history for every configured instrument? Unavailable ranges are skipped.'))return;const r=await fetch('/api/full-history/'+encodeURIComponent(key),{method:'POST'});const x=await r.json();alert(r.ok?'Full-history backfill started in the background':(x.detail||'Could not start full-history backfill'));load()}
async function backfill(key,symbol){const r=await fetch('/api/backfill',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({provider_key:key,symbol:symbol,timeframe:'1m',days:7})});const x=await r.json();alert(r.ok?`Backfill complete: ${x.written} candles, ${x.gaps} gaps`:(x.detail||'Backfill failed'));load()}
async function repairRecentM1(key){const r=await fetch(`/api/full-history/${encodeURIComponent(key)}/repair-recent-m1`,{method:'POST'});const x=await r.json();alert(r.ok?(x.ranges===0?'No M1 gaps found in the last 24 hours':'Recent M1 repair started'):(x.detail||'Recent M1 repair failed'));load()}
async function repairGaps(key){const r=await fetch('/api/gaps/repair',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({provider_key:key,limit:500})});const x=await r.json();alert(r.ok?`Repair complete: ${x.gaps_resolved} resolved, ${x.gaps_remaining} remaining`:(x.detail||'Repair failed'));load()}
async function rebuildHistory(key){if(!confirm(`Delete and rebuild all stored candle history for ${key} from fresh 1-minute MT5 history?`))return;const r=await fetch('/api/history/rebuild',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({provider_key:key,days:7})});const x=await r.json();alert(r.ok?`Clean rebuild complete for ${(x.rebuilt||[]).length} instruments`:(x.detail||'Rebuild failed'));load()}
async function testProvider(key){const r=await fetch('/api/providers/'+encodeURIComponent(key)+'/test',{method:'POST'}),x=await r.json();alert(r.ok?(x.ok?'Connection test passed.':'Connection test completed with warnings.'):(x.detail||'Connection test failed'))}
async function act(key,action){await fetch('/api/providers/'+encodeURIComponent(key)+'/'+action,{method:'POST'});setTimeout(load,100)}async function removeProvider(key){if(!confirm('Remove '+key+'? Historical data will remain.'))return;await fetch('/api/providers/'+encodeURIComponent(key),{method:'DELETE'});load()}
async function scanQuality(){const r=await fetch('/api/quality/scan',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({limit:10000,max_move_percent:20})}),x=await r.json();alert(r.ok?`Quality scan complete\nCandles scanned: ${x.scanned}\nIssues detected: ${x.issues}\nSevere: ${x.severe}`:(x.detail||'Quality scan failed'));load()}
async function integrityCheck(){const r=await fetch('/api/database/integrity'),x=await r.json();alert(x.status==='ok'?'Database integrity check passed.':'Integrity check failed: '+x.messages.join('\n'))}
async function createBackup(){const r=await fetch('/api/backups',{method:'POST'}),x=await r.json();if(!r.ok){alert(x.detail||'Backup failed');return}alert(`Verified backup created\n${x.filename}\n${x.size_bytes} bytes`);loadEvents()}
async function testAlert(){const r=await fetch('/api/alerts/test',{method:'POST'}),x=await r.json();if(!r.ok){alert(x.detail||'Alert test failed');return}alert(`Test alert submitted as event ${x.event_id}`);loadEvents()}
async function previewCleanup(){const r=await fetch('/api/database/retention/preview',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({tick_days:30,operational_days:90,vacuum:false})}),x=await r.json(),d=x.would_delete;alert(`30/90 day retention preview\nTicks: ${d.ticks}\nResolved gaps: ${d.resolved_gaps}\nIngestion states: ${d.ingestion_states}\nRepair runs: ${d.repair_runs}\nCandles: 0`) }
async function configureRetentionSchedule(){const minutes=Number(prompt('Retention interval in minutes (1440 = daily):','1440'));if(!Number.isFinite(minutes)||minutes<1)return;const r=await fetch('/api/maintenance/schedules/default-retention',{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify({name:'default-retention',task_type:'retention',enabled:true,interval_minutes:minutes,tick_days:30,operational_days:90,vacuum:false,run_immediately:false})}),x=await r.json();if(!r.ok){alert(x.detail||'Could not save schedule');return}alert(`Retention schedule saved\nNext run: ${new Date(x.next_run_utc).toLocaleString()}`)}
async function runCleanup(){if(!confirm('Delete raw ticks older than 30 days and operational history older than 90 days? Candles are never deleted.'))return;const vacuum=confirm('Also compact the SQLite database with VACUUM? This can take time and briefly block writers.');const r=await fetch('/api/database/retention/run',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({tick_days:30,operational_days:90,vacuum})}),x=await r.json();if(!r.ok){alert(x.detail||'Cleanup failed');return}alert(`Cleanup complete\nTicks deleted: ${x.ticks_deleted}\nResolved gaps deleted: ${x.resolved_gaps_deleted}\nIntegrity: ${x.integrity_status}`);load()}

let benchmarkTimer=null;
async function openBenchmark(){benchmarkEditor.showModal();await refreshBenchmark()}
benchmarkForm.onsubmit=async e=>{e.preventDefault();const d=new FormData(benchmarkForm),sizes=String(d.get('batch_sizes')).split(',').map(x=>Number(x.trim())).filter(x=>Number.isInteger(x)&&x>0);if(!sizes.length){alert('Enter at least one valid batch size.');return}if(!confirm('Run a heavy synthetic benchmark now? Live ingestion may be affected.'))return;const r=await fetch('/api/benchmarks/run',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({ticks:Number(d.get('ticks')),instruments:Number(d.get('instruments')),batch_sizes:sizes})}),x=await r.json();if(!r.ok){alert(x.detail||'Benchmark could not start');return}renderBenchmark({status:x.status,job:x});startBenchmarkPolling()}
function startBenchmarkPolling(){clearInterval(benchmarkTimer);benchmarkTimer=setInterval(refreshBenchmark,1000)}
async function refreshBenchmark(){const r=await fetch('/api/benchmarks/status'),x=await r.json();if(!r.ok)return;renderBenchmark(x);if(!x.job||!['queued','running'].includes(x.job.status)){clearInterval(benchmarkTimer);benchmarkTimer=null}}
function renderBenchmark(x){const j=x.job;if(!j){benchmarkProgress.textContent='No benchmark has been started.';benchmarkResults.style.display='none';return}benchmarkProgress.textContent=j.status==='running'?`Running batch ${j.current_batch_size||'-'} · ${j.completed_runs}/${j.total_runs} complete`:j.status==='completed'?`Completed ${j.total_runs} run(s) · recommended batch size ${j.best_batch_size}`:j.status==='failed'?`Benchmark failed: ${j.error}`:'Benchmark queued…';const results=j.results||[];benchmarkResults.style.display=results.length?'block':'none';benchmarkResults.innerHTML=results.map(r=>`<div class="benchmark-result ${j.best_batch_size===r.batch_size?'benchmark-best':''}"><strong>${r.batch_size}</strong><span>${Number(r.ticks_per_second).toLocaleString()} ticks/sec</span><span>${Number(r.elapsed_seconds).toFixed(2)} sec · ${(Number(r.peak_memory_bytes)/1048576).toFixed(2)} MB peak</span></div>`).join('')}
form.elements.kind.addEventListener('change',updateProviderForm);
form.onsubmit=async e=>{e.preventDefault();const d=new FormData(form),key=editing||d.get('provider_key'),kind=String(d.get('kind')),payload={provider_key:key,display_name:d.get('display_name'),kind,poll_interval_seconds:Number(d.get('poll_interval_seconds')),symbols:kind==='mt5'?editingSymbols:String(d.get('symbols')).split(',').map(x=>x.trim()).filter(Boolean),terminal_path:d.get('terminal_path')||null,account_login:d.get('account_login')?Number(d.get('account_login')):null,account_server:d.get('account_server')||null,password_env:null,mt5_password:d.get('mt5_password')||null,priority:Number(d.get('priority')),fallback_after_seconds:Number(d.get('fallback_after_seconds')),batch_window_seconds:Number(d.get('batch_window_seconds')),batch_limit:Number(d.get('batch_limit')),maintenance_enabled:form.elements.maintenance_enabled.checked,maintenance_interval_minutes:Number(d.get('maintenance_interval_minutes')),maintenance_backfill_days:Number(d.get('maintenance_backfill_days')),feed_quiet_seconds:Number(d.get('feed_quiet_seconds')),feed_stalled_seconds:Number(d.get('feed_stalled_seconds')),feed_inactive_seconds:Number(d.get('feed_inactive_seconds')),enabled:form.elements.enabled.checked,auto_start:form.elements.auto_start.checked};const r=await fetch('/api/providers/'+encodeURIComponent(key),{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});if(!r.ok){let payload=null;try{payload=await r.json()}catch{}error.textContent=apiError(payload,r.statusText||'Could not save provider');return}editor.close();load()};load();loadEvents();setInterval(()=>{load();loadEvents()},3000);
</script></body></html>'''

app = create_app(database_path=os.getenv("AXETOS_DATABASE_URL", "data/market_data.sqlite"))
