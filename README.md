# Axetos Market Data Server

A standalone Python market-data server for collecting financial market ticks, building OHLC candles, aggregating timeframes, validating historical data, and storing the result in SQLite.

This repository contains **market-data infrastructure only**. It does not place, simulate, validate, or manage orders. It has no trading accounts, positions, balances, P&L, strategies, chart renderer, or client trading interface.

## Version 0.9.0

Version 0.9.0 adds compatibility with the original Axetos MT5 bridge contract, including terminal heartbeats, instrument discovery, live quote snapshots, queued tick batches, source-candle ingestion, instrument selection, and bridge diagnostics.

## Operational diagnostics

Version 0.7.0 adds structured health and metrics endpoints, Prometheus-compatible text metrics, database-size reporting, provider health evaluation, and clearer operational status in the management UI. The development dependency set also includes `httpx`, which is required by FastAPI/Starlette `TestClient` in GitHub Actions.

Important endpoints:

```text
GET /api/health
GET /api/metrics
GET /metrics
```

## Current capabilities

- Batched MetaTrader 5 tick retrieval with overlap protection
- Configurable batch window and batch limit
- Optional scheduled backfill and gap repair
- Maintenance status and manual trigger in the management UI

- Continuous provider supervision in background worker threads
- Built-in deterministic mock provider for local development
- Optional direct MetaTrader 5 provider
- Raw tick persistence with duplicate protection
- Deterministic UTC one-minute OHLC candle construction
- Higher-timeframe candle aggregation
- Historical MT5 candle backfill
- Persistent backfill status and failure details
- Weekday candle-gap detection
- Targeted gap rescanning
- Automatic repair of unresolved MT5 gaps
- Contiguous gap grouping to reduce provider requests
- Repair-run audit history
- SQLite storage with WAL mode and indexed lookup paths
- Provider configuration persisted in JSON
- Browser-based provider control center
- REST endpoints and OpenAPI documentation
- Graceful shutdown with active candles stored as incomplete
- Automated tests through GitHub Actions
- Database integrity checks and retention previews
- Safe cleanup of old raw ticks and operational history
- WAL checkpointing, optional SQLite compaction, and cleanup audit history

## MT5 bridge compatibility

Version 0.9.0 implements the original Axetos MT5 bridge ingestion contract: heartbeat, instrument discovery, live quote snapshots, queued tick batches, source candles, and instrument selection. Tick batches are accepted quickly with HTTP 202 and processed by a bounded background queue so the bridge is not blocked by SQLite and candle construction work.

Compatible endpoints:

```text
POST /api/market-data/ingest/mt5/heartbeat
POST /api/market-data/ingest/mt5/instruments
POST /api/market-data/ingest/mt5/quotes
POST /api/market-data/ingest/mt5/ticks
POST /api/market-data/ingest/mt5/candles
GET  /api/market-data/mt5/discovered-instruments
POST /api/market-data/mt5/instrument-selection
GET  /api/market-data/mt5/bridge/status
```

## Architecture

```text
Market-data provider
        |
        v
Provider supervisor and worker
        |
        v
Tick normalization and validation
        |
        +----> SQLite tick store
        |
        v
1-minute candle builder
        |
        v
Higher-timeframe aggregation
        |
        v
SQLite candle store

Historical provider data
        |
        v
Backfill -> validation -> gap detection -> repair -> audit history
```

The server is intentionally separated from any trading platform. A trading platform may read the database or consume the REST endpoints, but order execution remains outside this project.

## Requirements

- Python 3.11 or newer
- MetaTrader 5 terminal and the `MetaTrader5` Python package only when using an MT5 provider

## Installation

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

For MetaTrader 5 support:

```powershell
pip install -e ".[dev,mt5]"
```

## Run the server

```powershell
axetos-market-data-server
```

Or:

```powershell
python -m axetos_market_data.server --host 127.0.0.1 --port 8000
```

Open:

- Control center: `http://127.0.0.1:8000/`
- OpenAPI documentation: `http://127.0.0.1:8000/docs`
- Health endpoint: `http://127.0.0.1:8000/api/health`

The default files are created under `data/`:

```text
data/market_data.sqlite
data/providers.json
```

## Provider control center

The web UI shows database statistics and the live state of each configured provider. Providers can be added, edited, started, stopped, restarted, or removed. MT5 providers expose controls for seven-day backfill and unresolved-gap repair.

## REST endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/api/health` | Server health and version |
| `GET` | `/api/statistics` | Database statistics |
| `GET` | `/api/database/integrity` | Run SQLite integrity diagnostics |
| `POST` | `/api/database/retention/preview` | Preview a retention policy without deleting data |
| `POST` | `/api/database/retention/run` | Apply retention and optionally compact SQLite |
| `GET` | `/api/database/retention/history` | Inspect cleanup audit history |
| `GET` | `/api/providers` | Provider configurations and runtime state |
| `PUT` | `/api/providers/{provider_key}` | Add or update a provider |
| `POST` | `/api/providers/{provider_key}/{action}` | Start, stop, restart, enable, or disable |
| `DELETE` | `/api/providers/{provider_key}` | Remove a provider; historical data remains |
| `POST` | `/api/backfill` | Import MT5 historical candles and detect gaps |
| `GET` | `/api/backfill/state` | Inspect persisted backfill results and failures |
| `GET` | `/api/gaps` | Query unresolved gaps with optional filters |
| `POST` | `/api/gaps/scan` | Rescan a provider/instrument/timeframe window |
| `POST` | `/api/gaps/repair` | Repair unresolved gaps from MT5 history |
| `GET` | `/api/gaps/repairs` | Inspect repair-run audit history |
| `GET` | `/api/instruments` | List instruments currently stored |
| `GET` | `/api/candles` | Query stored candles |

Example candle query:

```text
/api/candles?instrument=EUR%2FUSD&timeframe=1m&limit=200
```

## Command-line collector

The command-line workflow remains available:

```powershell
axetos-market-data mock --instrument EUR/USD --interval 1
```

```powershell
axetos-market-data mt5 --symbol EURUSD --symbol GBPUSD
```

```powershell
axetos-market-data aggregate --provider ICMarkets.MT5 --instrument EUR/USD --timeframe 15m
```

## Tests

```powershell
pytest -q
```

The test suite covers candle creation, storage, provider configuration, historical backfill, gap repair, and the web API. GitHub Actions runs it on every push and pull request.

## Roadmap

- Explicit provider priority and fallback policy
- Richer symbol alias mapping
- Automated scheduled backfill and repair jobs
- Market-session calendars and holiday awareness
- Data-quality diagnostics and operational log export
- PostgreSQL storage option
- Configurable scheduled retention policies

## License

MIT License. See [LICENSE](LICENSE).

## Provider priority and controlled fallback (v0.5.0)

Each provider has an explicit numeric priority (lower values win) and a freshness threshold. The server chooses exactly one authoritative source for each instrument. A lower-priority provider remains on standby and is selected only when every higher-priority source is stale or unavailable.

Incoming ticks from standby feeds are counted for operational visibility but are not blended into the authoritative tick and candle stream. Provider provenance remains attached to every stored record.

The current routing decision can be inspected in the control center or through:

```text
GET /api/routing
```

A typical configuration is:

```text
ICMarkets.MT5  priority 10  primary
Oanda.MT5      priority 20  secondary
Fallback       priority 90  last resort
```
