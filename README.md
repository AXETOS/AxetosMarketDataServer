# Axetos Market Data Server

A standalone Python market-data server for collecting financial market ticks, building OHLC candles, aggregating timeframes, and storing market data in SQLite.

This repository contains **market-data infrastructure only**. It does not place, simulate, validate, or manage orders. It has no trading accounts, positions, balances, P&L, strategies, chart renderer, or client trading interface.

## Current capabilities

- Continuous provider supervision in background worker threads
- Built-in deterministic mock provider for local development
- Optional direct MetaTrader 5 provider
- Raw tick persistence with duplicate protection
- Deterministic UTC one-minute OHLC candle construction
- Higher-timeframe candle aggregation
- SQLite storage with WAL mode and indexed lookup paths
- Provider configuration persisted in JSON
- Provider start, stop, restart, enable, disable, edit, and removal operations
- Browser-based provider control center and database statistics
- REST endpoints and automatically generated OpenAPI documentation
- Graceful shutdown with active candles persisted as incomplete
- Automated tests through GitHub Actions

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
```

The server is intentionally separated from any trading platform. A trading platform may read the database or consume the REST endpoints, but order execution remains outside this project.

## Requirements

- Python 3.11 or newer
- MetaTrader 5 terminal and the `MetaTrader5` Python package only when using the MT5 provider

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

The web UI shows:

- stored tick and candle totals;
- instrument count and latest tick time;
- configured provider state;
- provider type and symbols;
- received tick count and last tick;
- start, stop, restart, edit, enable/disable, and remove operations.

A mock provider can be configured directly from the UI. For an MT5 provider, select **MetaTrader 5**, enter one or more terminal symbols, and optionally provide the path to `terminal64.exe`.

## REST endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/api/health` | Server health and version |
| `GET` | `/api/statistics` | Database statistics |
| `GET` | `/api/providers` | Provider configurations and runtime state |
| `PUT` | `/api/providers/{provider_key}` | Add or update a provider |
| `POST` | `/api/providers/{provider_key}/{action}` | Start, stop, restart, enable, or disable |
| `DELETE` | `/api/providers/{provider_key}` | Remove a provider; historical data remains |
| `GET` | `/api/instruments` | Instruments currently stored |
| `GET` | `/api/candles` | Query stored candles |

Example candle query:

```text
/api/candles?instrument=EUR%2FUSD&timeframe=1m&limit=200
```

## Existing command-line collector

The original command-line workflow remains available:

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

The test suite currently covers candle creation, storage, provider configuration, and the web API. GitHub Actions runs the suite on every push and pull request.

## Project status

Version `0.2.0` is an early but runnable Python conversion of the provider supervision, collection, persistence, and management concepts from the original Axetos market-data server. Planned conversion work includes historical MT5 backfill, gap analysis and repair, richer symbol mapping, provider priority/fallback, operational log export, and more extensive data-quality diagnostics.

## License

MIT License. See [LICENSE](LICENSE).
