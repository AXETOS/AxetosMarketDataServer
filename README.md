# Axetos Market Data Server

A focused Python service for collecting financial market ticks, building OHLC candles, and storing market data in SQLite.

This repository intentionally contains **market-data infrastructure only**. It does not place, simulate, validate, or manage orders. It has no accounts, positions, balances, chart rendering, trading strategies, or user interface.

## Scope

```text
Market-data provider
        ↓
Normalized ticks
        ↓
One-minute candle builder
        ↓
Higher-timeframe aggregation
        ↓
SQLite database
```

The database can be consumed by a separate trading platform, analytics application, or research tool.

## Features

- Strict typed domain models for ticks and candles
- UTC-normalized timestamps
- Duplicate-resistant tick persistence
- Deterministic one-minute OHLC candle construction
- Candle upserts for active/incomplete candles
- Higher-timeframe aggregation from stored one-minute candles
- SQLite WAL mode for concurrent readers
- Optional direct MetaTrader 5 tick provider
- Built-in mock provider for development and demonstrations
- Unit tests for candle construction and persistence

## Requirements

- Python 3.11 or newer
- Windows and an installed MetaTrader 5 terminal only when using the optional MT5 provider

## Installation

```bash
python -m venv .venv
```

Windows:

```powershell
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

Linux/macOS:

```bash
source .venv/bin/activate
pip install -e '.[dev]'
```

For direct MetaTrader 5 collection on Windows:

```powershell
pip install -e ".[dev,mt5]"
```

## Run the local demonstration collector

```bash
axetos-market-data --database data/market_data.sqlite mock --instrument EUR/USD --interval 0.25
```

Stop with `Ctrl+C`. The active minute is persisted with `complete = 0`; finalized minutes are stored with `complete = 1`.

## Run with MetaTrader 5

```powershell
axetos-market-data --database data/market_data.sqlite mt5 `
  --symbol EURUSD.pro `
  --symbol GBPUSD.pro
```

An explicit terminal path can be supplied when several terminals are installed:

```powershell
axetos-market-data --database data/market_data.sqlite mt5 `
  --terminal-path "C:\Program Files\MetaTrader 5\terminal64.exe" `
  --symbol EURUSD.pro
```

## Aggregate stored candles

```bash
axetos-market-data --database data/market_data.sqlite aggregate \
  --provider mt5 \
  --instrument EUR/USD \
  --timeframe 5m
```

Supported target timeframes are `5m`, `15m`, `30m`, `1h`, `4h`, and `1d`.

## Database schema

### `ticks`

Raw normalized provider observations:

- provider
- instrument
- timestamp_utc
- bid
- ask
- volume

### `candles`

Persisted OHLC data:

- provider
- instrument
- timeframe
- open_time_utc
- open
- high
- low
- close
- tick_count
- volume
- complete

Prices are stored as decimal strings rather than binary floating-point values to avoid unnecessary precision loss.

## Design decisions

### One responsibility

The service collects and stores market data. Execution belongs to a separate trading platform.

### Provider normalization

Provider-specific symbol names are normalized before persistence. For example, `EURUSD.pro` becomes `EUR/USD`.

### UTC only

All persisted timestamps are timezone-aware UTC values. Local display conversion belongs to the consuming application.

### SQLite WAL mode

WAL mode allows a trading platform to read the database while the collector writes to it.

## Tests

```bash
pytest
```

## Roadmap

- Batched MT5 tick retrieval instead of quote polling
- Historical backfill adapters
- Gap detection and repair jobs
- Provider priority and failover
- PostgreSQL storage adapter
- Metrics and operational health reporting

## Origin

The project was extracted as a clean, standalone Python implementation from concepts proven in the AxetosOS market-data work. It is intentionally independent of AxetosOS and suitable for public review.

## License

MIT
