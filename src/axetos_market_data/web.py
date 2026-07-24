from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field

from .config import ConfigurationStore, ProviderConfig
from .runtime import ProviderSupervisor
from .history import HistoricalBackfillService
from .providers.mt5 import MetaTrader5TickProvider
from datetime import datetime, UTC, timedelta
from .storage import MarketDataStore
from .diagnostics import build_health, build_metrics, prometheus_text
from .housekeeping import HousekeepingService, RetentionPolicy
from . import __version__
from .policies import choose_canonical_source
from .quality import CandleQualityService
from .bridge import (Mt5BridgeService, BridgeHeartbeatRequest, BridgeInstrumentsRequest, BridgeTicksRequest, BridgeQuotesRequest, BridgeCandlesRequest, InstrumentSelectionRequest)


class BackfillRequest(BaseModel):
    provider_key: str
    symbol: str
    instrument: str | None = None
    timeframe: str = "1m"
    days: int = Field(default=7, ge=1, le=3650)


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


class ProviderRequest(BaseModel):
    provider_key: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    kind: str = "mock"
    enabled: bool = True
    auto_start: bool = True
    poll_interval_seconds: float = Field(default=1.0, gt=0)
    symbols: list[str] = Field(default_factory=lambda: ["EUR/USD"])
    terminal_path: str | None = None
    priority: int = Field(default=100, ge=0, le=10000)
    fallback_after_seconds: float = Field(default=10.0, gt=0, le=3600)
    batch_window_seconds: int = Field(default=5, ge=1, le=300)
    batch_limit: int = Field(default=50000, ge=100, le=1000000)
    maintenance_enabled: bool = False
    maintenance_interval_minutes: int = Field(default=60, ge=1, le=10080)
    maintenance_backfill_days: int = Field(default=2, ge=1, le=365)


def create_app(
    database_path: str | Path = "data/market_data.sqlite",
    configuration_path: str | Path = "data/providers.json",
) -> FastAPI:
    store = MarketDataStore(database_path)
    store.initialize()
    config_store = ConfigurationStore(configuration_path)
    supervisor = ProviderSupervisor(config_store, store)
    started_utc = datetime.now(UTC)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        supervisor.load()
        yield
        supervisor.shutdown()
        bridge.shutdown()

    app = FastAPI(
        title="Axetos Market Data Server",
        version=__version__,
        description="Collects market ticks, builds OHLC candles, and stores market data.",
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.supervisor = supervisor
    housekeeping = HousekeepingService(store)
    bridge = Mt5BridgeService(store)
    quality = CandleQualityService(store)
    app.state.bridge = bridge
    app.state.quality = quality

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> str:
        return CONTROL_CENTER_HTML

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
        return {**store.bridge_status(), "queue": bridge.view()}

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
    def bridge_ticks(request: BridgeTicksRequest) -> dict[str, object]:
        try: count=bridge.enqueue_ticks(request)
        except (ValueError,RuntimeError) as exc: raise HTTPException(503 if isinstance(exc,RuntimeError) else 400, str(exc)) from exc
        return {"accepted": count, "queued": True}

    @app.post("/api/market-data/ingest/mt5/candles")
    def bridge_candles(request: BridgeCandlesRequest) -> dict[str, object]:
        try: count=bridge.candles(request)
        except ValueError as exc: raise HTTPException(400, str(exc)) from exc
        return {"accepted": count}

    @app.get("/api/market-data/mt5/discovered-instruments")
    def discovered_instruments(provider_key: str | None=None, terminal_instance_id: str | None=None) -> dict[str, object]:
        items=store.list_bridge_instruments(provider_key, terminal_instance_id)
        return {"count":len(items), "instruments":items}

    @app.post("/api/market-data/mt5/instrument-selection")
    def instrument_selection(request: InstrumentSelectionRequest) -> dict[str, object]:
        changed=store.set_bridge_instrument_selection(request.provider_key,request.terminal_instance_id,request.provider_symbol,request.enabled)
        if not changed: raise HTTPException(404,"Discovered instrument not found")
        return {"updated":True,"enabled":request.enabled}


    @app.get("/api/database/integrity")
    def database_integrity() -> dict[str, object]:
        return store.integrity_check()

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

    @app.get("/api/providers")
    def providers() -> dict[str, object]:
        return {"providers": supervisor.list_views()}

    @app.get("/api/routing")
    def routing() -> dict[str, object]:
        return {"routes": supervisor.authority.snapshot()}

    @app.get("/api/symbol-policies")
    def symbol_policies(provider_key: str | None = None, instrument: str | None = None) -> dict[str, object]:
        items = store.list_symbol_policies(provider_key, instrument)
        return {"count": len(items), "items": items}

    @app.put("/api/symbol-policies")
    def upsert_symbol_policy(request: SymbolPolicyRequest) -> dict[str, object]:
        return store.upsert_symbol_policy(**request.model_dump())

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
            return {"ok": True, "provider_key": provider_key, "kind": "mock", "message": "Mock provider is available."}
        if config.kind.lower() == "mt5":
            try:
                result = MetaTrader5TickProvider(
                    config.normalized_symbols(), config.terminal_path, config.provider_key,
                    config.batch_window_seconds, config.batch_limit,
                ).test_connection()
                return {"provider_key": provider_key, "kind": "mt5", **result}
            except RuntimeError as exc:
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
        return supervisor.upsert(ProviderConfig(**request.model_dump()))

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
        if worker.config.kind.lower() != "mt5":
            raise HTTPException(400, "Historical backfill currently requires an MT5 provider")
        end = datetime.now(UTC)
        start = end - timedelta(days=request.days)
        provider = MetaTrader5TickProvider(worker.config.normalized_symbols(), worker.config.terminal_path, request.provider_key)
        instrument = request.instrument or provider._canonical_symbol(request.symbol)
        try:
            result = HistoricalBackfillService(store).run(provider, request.provider_key, request.symbol, instrument, request.timeframe, start, end)
            return result.__dict__ if hasattr(result, "__dict__") else {name: getattr(result, name) for name in result.__slots__}
        except (RuntimeError, ValueError) as exc:
            store.set_ingestion_state(request.provider_key, instrument, request.timeframe, start, end, 0, 0, 0, "Failed", str(exc))
            raise HTTPException(400, str(exc)) from exc

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
        count = HistoricalBackfillService(store).detect_gaps(
            request.provider_key, request.instrument, request.timeframe, start, end
        )
        return {"provider": request.provider_key, "instrument": request.instrument,
                "timeframe": request.timeframe, "gaps": count}

    @app.post("/api/gaps/repair")
    def repair_gaps(request: GapRepairRequest) -> dict[str, object]:
        worker = supervisor.get(request.provider_key)
        if worker is None:
            raise HTTPException(404, "Provider not found")
        if worker.config.kind.lower() != "mt5":
            raise HTTPException(400, "Gap repair currently requires an MT5 provider")
        provider = MetaTrader5TickProvider(
            worker.config.normalized_symbols(), worker.config.terminal_path, request.provider_key
        )
        symbol_map = {
            provider._canonical_symbol(symbol): symbol
            for symbol in worker.config.normalized_symbols()
        }
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
    ) -> dict[str, object]:
        values = store.read_candles(instrument, timeframe, limit, provider)
        return {
            "instrument": instrument,
            "timeframe": timeframe,
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

    return app


CONTROL_CENTER_HTML = r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Axetos Market Data Server</title><style>
:root{color-scheme:dark;font-family:Inter,Segoe UI,sans-serif;background:#0c111b;color:#edf2f7}body{margin:0}header{padding:20px 28px;border-bottom:1px solid #253047;background:#111827;display:flex;justify-content:space-between;align-items:center}h1{font-size:21px;margin:0}.sub{font-size:12px;color:#93c5fd;margin-top:4px}.wrap{padding:24px;max-width:1280px;margin:auto}.stats,.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px}.grid{grid-template-columns:repeat(auto-fit,minmax(340px,1fr));margin-top:18px}.card{border:1px solid #26334a;background:#121a29;border-radius:12px;padding:17px}.value{font-size:25px;font-weight:800}.label{color:#94a3b8;font-size:12px}.top{display:flex;justify-content:space-between;gap:12px}.name{font-size:18px;font-weight:700}.key{color:#94a3b8;font-size:12px}.status{font-size:12px;font-weight:800;padding:5px 9px;border-radius:999px;background:#334155}.status.live{background:#14532d;color:#bbf7d0}.status.failed{background:#7f1d1d;color:#fecaca}.rows{margin-top:15px;display:grid;grid-template-columns:1fr 1fr;gap:9px 14px;font-size:13px}button{background:#2563eb;color:white;border:0;border-radius:7px;padding:9px 13px;font-weight:600;cursor:pointer}.secondary{background:#334155}.danger{background:#991b1b}.actions{display:flex;flex-wrap:wrap;gap:7px;margin-top:15px}dialog{background:#111827;color:#fff;border:1px solid #334155;border-radius:12px;width:min(720px,90vw)}form{display:grid;grid-template-columns:1fr 1fr;gap:12px}label{font-size:12px;color:#cbd5e1}input,select{width:100%;box-sizing:border-box;margin-top:5px;padding:9px;border-radius:6px;border:1px solid #334155;background:#0f172a;color:#fff}.wide{grid-column:1/-1}.toolbar{display:flex;justify-content:space-between;align-items:center;margin:20px 0 8px}.error{color:#fca5a5;font-size:12px;margin-top:8px}</style></head><body>
<header><div><h1>Axetos Market Data Server</h1><div class="sub">Market-data infrastructure only · no orders, accounts, positions, or trading logic</div></div><button onclick="openAdd()">+ Add provider</button></header><main class="wrap"><section id="stats" class="stats"></section><div class="toolbar"><strong>Provider control center</strong><span><button class="secondary" onclick="scanQuality()">Scan candle quality</button> <button class="secondary" onclick="integrityCheck()">Check database</button> <button class="secondary" onclick="previewCleanup()">Preview cleanup</button> <button class="danger" onclick="runCleanup()">Run cleanup</button> <span id="count"></span></span></div><section id="providers" class="grid"></section></main>
<dialog id="editor"><h2 id="title">Add provider</h2><form id="form"><label>Provider key<input name="provider_key" required placeholder="ICMarkets.MT5"></label><label>Display name<input name="display_name" required placeholder="IC Markets MT5"></label><label>Provider type<select name="kind"><option value="mock">Mock</option><option value="mt5">MetaTrader 5</option></select></label><label>Polling interval<input name="poll_interval_seconds" type="number" step="0.1" value="1"></label><label class="wide">Symbols, comma separated<input name="symbols" value="EUR/USD"></label><label class="wide">MT5 terminal path<input name="terminal_path" placeholder="Optional terminal64.exe path"></label><label>Priority<input name="priority" type="number" min="0" value="100"></label><label>Fallback after seconds<input name="fallback_after_seconds" type="number" min="0.1" step="0.1" value="10"></label><label>Batch window seconds<input name="batch_window_seconds" type="number" min="1" value="5"></label><label>Batch limit<input name="batch_limit" type="number" min="100" value="50000"></label><label>Maintenance interval minutes<input name="maintenance_interval_minutes" type="number" min="1" value="60"></label><label>Maintenance backfill days<input name="maintenance_backfill_days" type="number" min="1" value="2"></label><label><input name="maintenance_enabled" type="checkbox" style="width:auto"> Scheduled maintenance</label><label><input name="enabled" type="checkbox" checked style="width:auto"> Enabled</label><label><input name="auto_start" type="checkbox" checked style="width:auto"> Auto-start</label><div id="error" class="error wide"></div><div class="actions wide"><button type="button" class="secondary" onclick="editor.close()">Cancel</button><button type="submit">Save</button></div></form></dialog>
<script>
const root=document.getElementById('providers'),stats=document.getElementById('stats'),count=document.getElementById('count'),editor=document.getElementById('editor'),form=document.getElementById('form'),title=document.getElementById('title'),error=document.getElementById('error');let editing=null;const esc=v=>String(v??'').replace(/[&<>\"]/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[x]));
async function load(){const [pr,sr,hr,mr,br]=await Promise.all([fetch('/api/providers'),fetch('/api/statistics'),fetch('/api/health'),fetch('/api/metrics'),fetch('/api/market-data/mt5/bridge/status')]);const p=await pr.json(),s=await sr.json(),h=await hr.json(),m=await mr.json(),b=await br.json();stats.innerHTML=[['Health',h.status],['Live providers',m.providers_live+'/'+m.providers_configured],['Ticks',s.ticks],['Candles',s.candles],['Instruments',s.instruments],['Database size',(m.database_size_bytes/1048576).toFixed(2)+' MB'],['Latest tick',s.latest_tick_utc?new Date(s.latest_tick_utc).toLocaleString():'-'],['Unresolved gaps',s.unresolved_gaps],['Quality issues',s.unresolved_quality_issues],['MT5 terminals',b.heartbeats.length],['Bridge instruments',b.discovered_instruments],['Bridge queue',b.queue.queue_depth]].map(x=>`<div class="card"><div class="label">${x[0]}</div><div class="value">${x[1]}</div></div>`).join('');count.textContent=`${p.providers.length} configured`;root.innerHTML=p.providers.length?'':'<div class="card">No providers configured.</div>';for(const x of p.providers){const c=x.configuration,r=x.runtime,e=document.createElement('article');e.className='card';e.innerHTML=`<div class="top"><div><div class="name">${esc(c.display_name)}</div><div class="key">${esc(c.provider_key)} · ${esc(c.kind)}</div></div><span class="status ${String(r.status).toLowerCase()}">${esc(r.status)}</span></div><div class="rows"><div><span class="label">Symbols</span><br>${c.symbols.map(esc).join(', ')}</div><div><span class="label">Ticks received</span><br>${r.ticks_received}</div><div><span class="label">Last tick</span><br>${r.last_tick_utc?new Date(r.last_tick_utc).toLocaleString():'-'}</div><div><span class="label">Auto-start</span><br>${c.auto_start?'Yes':'No'}</div><div><span class="label">Priority / fallback</span><br>${c.priority} / ${c.fallback_after_seconds}s</div><div><span class="label">Authoritative / standby ticks</span><br>${r.authoritative_ticks} / ${r.standby_ticks}</div><div><span class="label">Batch window / limit</span><br>${c.batch_window_seconds}s / ${c.batch_limit}</div><div><span class="label">Maintenance</span><br>${x.maintenance?`${esc(x.maintenance.status)} · next ${x.maintenance.next_run_utc?new Date(x.maintenance.next_run_utc).toLocaleString():'-'}`:'Disabled'}</div></div><div class="actions"><button onclick="editProvider('${esc(c.provider_key)}')">Edit</button><button onclick="act('${esc(c.provider_key)}','start')">Start</button><button class="secondary" onclick="act('${esc(c.provider_key)}','stop')">Stop</button><button onclick="act('${esc(c.provider_key)}','restart')">Restart</button><button class="secondary" onclick="testProvider('${esc(c.provider_key)}')">Test connection</button>${c.kind==='mt5'?`<button class="secondary" onclick="backfill('${esc(c.provider_key)}','${esc(c.symbols[0]||'')}')">Backfill 7d</button><button class="secondary" onclick="repairGaps('${esc(c.provider_key)}')">Repair gaps</button>${c.maintenance_enabled?`<button class="secondary" onclick="act('${esc(c.provider_key)}','maintenance')">Run maintenance</button>`:''}`:''}<button class="danger" onclick="removeProvider('${esc(c.provider_key)}')">Remove</button></div>${r.last_error?`<div class="error">${esc(r.last_error)}</div>`:''}`;root.appendChild(e)}}
function openAdd(){editing=null;title.textContent='Add provider';form.reset();form.elements.poll_interval_seconds.value=1;form.elements.symbols.value='EUR/USD';form.elements.priority.value=100;form.elements.fallback_after_seconds.value=10;form.elements.batch_window_seconds.value=5;form.elements.batch_limit.value=50000;form.elements.maintenance_interval_minutes.value=60;form.elements.maintenance_backfill_days.value=2;form.elements.maintenance_enabled.checked=false;form.elements.enabled.checked=true;form.elements.auto_start.checked=true;form.elements.provider_key.readOnly=false;error.textContent='';editor.showModal()}
async function editProvider(key){const r=await fetch('/api/providers/'+encodeURIComponent(key)),x=await r.json(),c=x.configuration;editing=key;title.textContent='Edit '+c.display_name;form.elements.provider_key.value=c.provider_key;form.elements.provider_key.readOnly=true;form.elements.display_name.value=c.display_name;form.elements.kind.value=c.kind;form.elements.poll_interval_seconds.value=c.poll_interval_seconds;form.elements.symbols.value=c.symbols.join(',');form.elements.terminal_path.value=c.terminal_path||'';form.elements.priority.value=c.priority;form.elements.fallback_after_seconds.value=c.fallback_after_seconds;form.elements.batch_window_seconds.value=c.batch_window_seconds;form.elements.batch_limit.value=c.batch_limit;form.elements.maintenance_interval_minutes.value=c.maintenance_interval_minutes;form.elements.maintenance_backfill_days.value=c.maintenance_backfill_days;form.elements.maintenance_enabled.checked=c.maintenance_enabled;form.elements.enabled.checked=c.enabled;form.elements.auto_start.checked=c.auto_start;editor.showModal()}
async function backfill(key,symbol){const r=await fetch('/api/backfill',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({provider_key:key,symbol:symbol,timeframe:'1m',days:7})});const x=await r.json();alert(r.ok?`Backfill complete: ${x.written} candles, ${x.gaps} gaps`:(x.detail||'Backfill failed'));load()}
async function repairGaps(key){const r=await fetch('/api/gaps/repair',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({provider_key:key,limit:500})});const x=await r.json();alert(r.ok?`Repair complete: ${x.gaps_resolved} resolved, ${x.gaps_remaining} remaining`:(x.detail||'Repair failed'));load()}
async function testProvider(key){const r=await fetch('/api/providers/'+encodeURIComponent(key)+'/test',{method:'POST'}),x=await r.json();alert(r.ok?(x.ok?'Connection test passed.':'Connection test completed with warnings.'):(x.detail||'Connection test failed'))}
async function act(key,action){await fetch('/api/providers/'+encodeURIComponent(key)+'/'+action,{method:'POST'});setTimeout(load,100)}async function removeProvider(key){if(!confirm('Remove '+key+'? Historical data will remain.'))return;await fetch('/api/providers/'+encodeURIComponent(key),{method:'DELETE'});load()}
async function scanQuality(){const r=await fetch('/api/quality/scan',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({limit:10000,max_move_percent:20})}),x=await r.json();alert(r.ok?`Quality scan complete\nCandles scanned: ${x.scanned}\nIssues detected: ${x.issues}\nSevere: ${x.severe}`:(x.detail||'Quality scan failed'));load()}
async function integrityCheck(){const r=await fetch('/api/database/integrity'),x=await r.json();alert(x.status==='ok'?'Database integrity check passed.':'Integrity check failed: '+x.messages.join('\n'))}
async function previewCleanup(){const r=await fetch('/api/database/retention/preview',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({tick_days:30,operational_days:90,vacuum:false})}),x=await r.json(),d=x.would_delete;alert(`30/90 day retention preview\nTicks: ${d.ticks}\nResolved gaps: ${d.resolved_gaps}\nIngestion states: ${d.ingestion_states}\nRepair runs: ${d.repair_runs}\nCandles: 0`) }
async function runCleanup(){if(!confirm('Delete raw ticks older than 30 days and operational history older than 90 days? Candles are never deleted.'))return;const vacuum=confirm('Also compact the SQLite database with VACUUM? This can take time and briefly block writers.');const r=await fetch('/api/database/retention/run',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({tick_days:30,operational_days:90,vacuum})}),x=await r.json();if(!r.ok){alert(x.detail||'Cleanup failed');return}alert(`Cleanup complete\nTicks deleted: ${x.ticks_deleted}\nResolved gaps deleted: ${x.resolved_gaps_deleted}\nIntegrity: ${x.integrity_status}`);load()}
form.onsubmit=async e=>{e.preventDefault();const d=new FormData(form),key=editing||d.get('provider_key'),payload={provider_key:key,display_name:d.get('display_name'),kind:d.get('kind'),poll_interval_seconds:Number(d.get('poll_interval_seconds')),symbols:String(d.get('symbols')).split(',').map(x=>x.trim()).filter(Boolean),terminal_path:d.get('terminal_path')||null,priority:Number(d.get('priority')),fallback_after_seconds:Number(d.get('fallback_after_seconds')),batch_window_seconds:Number(d.get('batch_window_seconds')),batch_limit:Number(d.get('batch_limit')),maintenance_enabled:form.elements.maintenance_enabled.checked,maintenance_interval_minutes:Number(d.get('maintenance_interval_minutes')),maintenance_backfill_days:Number(d.get('maintenance_backfill_days')),enabled:form.elements.enabled.checked,auto_start:form.elements.auto_start.checked};const r=await fetch('/api/providers/'+encodeURIComponent(key),{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});if(!r.ok){error.textContent=(await r.json()).detail||r.statusText;return}editor.close();load()};load();setInterval(load,3000);
</script></body></html>'''

app = create_app()
