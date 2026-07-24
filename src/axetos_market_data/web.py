from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .config import ConfigurationStore, ProviderConfig
from .runtime import ProviderSupervisor
from .history import HistoricalBackfillService
from .providers.mt5 import MetaTrader5TickProvider
from datetime import datetime, UTC, timedelta
from .storage import MarketDataStore


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


class ProviderRequest(BaseModel):
    provider_key: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    kind: str = "mock"
    enabled: bool = True
    auto_start: bool = True
    poll_interval_seconds: float = Field(default=1.0, gt=0)
    symbols: list[str] = Field(default_factory=lambda: ["EUR/USD"])
    terminal_path: str | None = None


def create_app(
    database_path: str | Path = "data/market_data.sqlite",
    configuration_path: str | Path = "data/providers.json",
) -> FastAPI:
    store = MarketDataStore(database_path)
    store.initialize()
    config_store = ConfigurationStore(configuration_path)
    supervisor = ProviderSupervisor(config_store, store)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        supervisor.load()
        yield
        supervisor.shutdown()

    app = FastAPI(
        title="Axetos Market Data Server",
        version="0.4.0",
        description="Collects market ticks, builds OHLC candles, and stores market data.",
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.supervisor = supervisor

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> str:
        return CONTROL_CENTER_HTML

    @app.get("/api/health")
    def health() -> dict[str, object]:
        return {"product": "Axetos Market Data Server", "status": "running", "version": "0.4.0"}

    @app.get("/api/statistics")
    def statistics() -> dict[str, object]:
        return store.statistics()

    @app.get("/api/providers")
    def providers() -> dict[str, object]:
        return {"providers": supervisor.list_views()}

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
<header><div><h1>Axetos Market Data Server</h1><div class="sub">Market-data infrastructure only · no orders, accounts, positions, or trading logic</div></div><button onclick="openAdd()">+ Add provider</button></header><main class="wrap"><section id="stats" class="stats"></section><div class="toolbar"><strong>Provider control center</strong><span id="count"></span></div><section id="providers" class="grid"></section></main>
<dialog id="editor"><h2 id="title">Add provider</h2><form id="form"><label>Provider key<input name="provider_key" required placeholder="ICMarkets.MT5"></label><label>Display name<input name="display_name" required placeholder="IC Markets MT5"></label><label>Provider type<select name="kind"><option value="mock">Mock</option><option value="mt5">MetaTrader 5</option></select></label><label>Polling interval<input name="poll_interval_seconds" type="number" step="0.1" value="1"></label><label class="wide">Symbols, comma separated<input name="symbols" value="EUR/USD"></label><label class="wide">MT5 terminal path<input name="terminal_path" placeholder="Optional terminal64.exe path"></label><label><input name="enabled" type="checkbox" checked style="width:auto"> Enabled</label><label><input name="auto_start" type="checkbox" checked style="width:auto"> Auto-start</label><div id="error" class="error wide"></div><div class="actions wide"><button type="button" class="secondary" onclick="editor.close()">Cancel</button><button type="submit">Save</button></div></form></dialog>
<script>
const root=document.getElementById('providers'),stats=document.getElementById('stats'),count=document.getElementById('count'),editor=document.getElementById('editor'),form=document.getElementById('form'),title=document.getElementById('title'),error=document.getElementById('error');let editing=null;const esc=v=>String(v??'').replace(/[&<>\"]/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[x]));
async function load(){const [pr,sr]=await Promise.all([fetch('/api/providers'),fetch('/api/statistics')]);const p=await pr.json(),s=await sr.json();stats.innerHTML=[['Ticks',s.ticks],['Candles',s.candles],['Instruments',s.instruments],['Latest tick',s.latest_tick_utc?new Date(s.latest_tick_utc).toLocaleString():'-'],['Unresolved gaps',s.unresolved_gaps]].map(x=>`<div class="card"><div class="label">${x[0]}</div><div class="value">${x[1]}</div></div>`).join('');count.textContent=`${p.providers.length} configured`;root.innerHTML=p.providers.length?'':'<div class="card">No providers configured.</div>';for(const x of p.providers){const c=x.configuration,r=x.runtime,e=document.createElement('article');e.className='card';e.innerHTML=`<div class="top"><div><div class="name">${esc(c.display_name)}</div><div class="key">${esc(c.provider_key)} · ${esc(c.kind)}</div></div><span class="status ${String(r.status).toLowerCase()}">${esc(r.status)}</span></div><div class="rows"><div><span class="label">Symbols</span><br>${c.symbols.map(esc).join(', ')}</div><div><span class="label">Ticks received</span><br>${r.ticks_received}</div><div><span class="label">Last tick</span><br>${r.last_tick_utc?new Date(r.last_tick_utc).toLocaleString():'-'}</div><div><span class="label">Auto-start</span><br>${c.auto_start?'Yes':'No'}</div></div><div class="actions"><button onclick="editProvider('${esc(c.provider_key)}')">Edit</button><button onclick="act('${esc(c.provider_key)}','start')">Start</button><button class="secondary" onclick="act('${esc(c.provider_key)}','stop')">Stop</button><button onclick="act('${esc(c.provider_key)}','restart')">Restart</button>${c.kind==='mt5'?`<button class="secondary" onclick="backfill('${esc(c.provider_key)}','${esc(c.symbols[0]||'')}')">Backfill 7d</button><button class="secondary" onclick="repairGaps('${esc(c.provider_key)}')">Repair gaps</button>`:''}<button class="danger" onclick="removeProvider('${esc(c.provider_key)}')">Remove</button></div>${r.last_error?`<div class="error">${esc(r.last_error)}</div>`:''}`;root.appendChild(e)}}
function openAdd(){editing=null;title.textContent='Add provider';form.reset();form.elements.poll_interval_seconds.value=1;form.elements.symbols.value='EUR/USD';form.elements.enabled.checked=true;form.elements.auto_start.checked=true;form.elements.provider_key.readOnly=false;error.textContent='';editor.showModal()}
async function editProvider(key){const r=await fetch('/api/providers/'+encodeURIComponent(key)),x=await r.json(),c=x.configuration;editing=key;title.textContent='Edit '+c.display_name;form.elements.provider_key.value=c.provider_key;form.elements.provider_key.readOnly=true;form.elements.display_name.value=c.display_name;form.elements.kind.value=c.kind;form.elements.poll_interval_seconds.value=c.poll_interval_seconds;form.elements.symbols.value=c.symbols.join(',');form.elements.terminal_path.value=c.terminal_path||'';form.elements.enabled.checked=c.enabled;form.elements.auto_start.checked=c.auto_start;editor.showModal()}
async function backfill(key,symbol){const r=await fetch('/api/backfill',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({provider_key:key,symbol:symbol,timeframe:'1m',days:7})});const x=await r.json();alert(r.ok?`Backfill complete: ${x.written} candles, ${x.gaps} gaps`:(x.detail||'Backfill failed'));load()}
async function repairGaps(key){const r=await fetch('/api/gaps/repair',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({provider_key:key,limit:500})});const x=await r.json();alert(r.ok?`Repair complete: ${x.gaps_resolved} resolved, ${x.gaps_remaining} remaining`:(x.detail||'Repair failed'));load()}
async function act(key,action){await fetch('/api/providers/'+encodeURIComponent(key)+'/'+action,{method:'POST'});setTimeout(load,100)}async function removeProvider(key){if(!confirm('Remove '+key+'? Historical data will remain.'))return;await fetch('/api/providers/'+encodeURIComponent(key),{method:'DELETE'});load()}
form.onsubmit=async e=>{e.preventDefault();const d=new FormData(form),key=editing||d.get('provider_key'),payload={provider_key:key,display_name:d.get('display_name'),kind:d.get('kind'),poll_interval_seconds:Number(d.get('poll_interval_seconds')),symbols:String(d.get('symbols')).split(',').map(x=>x.trim()).filter(Boolean),terminal_path:d.get('terminal_path')||null,enabled:form.elements.enabled.checked,auto_start:form.elements.auto_start.checked};const r=await fetch('/api/providers/'+encodeURIComponent(key),{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});if(!r.ok){error.textContent=(await r.json()).detail||r.statusText;return}editor.close();load()};load();setInterval(load,3000);
</script></body></html>'''

app = create_app()
