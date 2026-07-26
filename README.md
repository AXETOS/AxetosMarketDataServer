# Axetos Market Data Server

A standalone Python market-data server for collecting financial market ticks, building OHLC candles, aggregating timeframes, validating historical data, and storing the result in SQLite or PostgreSQL.

This repository contains **market-data infrastructure only**. It does not place, simulate, validate, or manage orders. It has no trading accounts, positions, balances, P&L, strategies, chart renderer, or client trading interface.

## Version 0.47.0

Version 0.46.0 makes canonical read routing availability-aware. When the configured preferred provider has no candle rows for the requested instrument and timeframe, `/api/candles` falls back to the provider that actually owns the newest stored candle history. Quote reads follow the same rule: `/api/quotes/{instrument}` first respects canonical routing, then falls back to the newest available bridge quote when the preferred provider has no quote. Explicit `provider` requests remain strict and never fall back. This prevents instruments such as Bitcoin from appearing unavailable merely because a higher-priority configured provider does not supply that market.


Version 0.46.0 removes a timing-sensitive benchmark CI race. Benchmark jobs now expose an event-backed completion wait boundary, release tests no longer depend on a fixed ten-second polling window, and API benchmark tests wait for their background worker before application teardown. The HTTP benchmark API remains asynchronous.

Version 0.46.0 makes the Market Data Server the sole candle producer for Trading Platform consumers. Every accepted authoritative tick now persists the current incomplete one-minute candle and refreshes the server-owned canonical 5m, 15m, 30m, 1h, 4h, and 1d candles. The candle API therefore returns both completed history and the current server-built display candle without requiring any client-side OHLC construction or aggregation. Completed and incomplete state remains explicit through the existing `complete` field, and genuine gaps remain absent rather than being synthesized by consumers.

## Version 0.43.0

Version 0.43.0 restores the MT5 bridge control-plane contract. The server now exposes plain-text enabled-symbol and repair-request endpoints plus repair-result acknowledgement, authorizes them with the bridge token, and returns selected provider symbols for the exact terminal instance. Bridge v1.12 distinguishes network/server outages from HTTP application errors: only transport failures and HTTP 5xx responses trigger shared exponential backoff, while 4xx endpoint or configuration responses are reported without suppressing heartbeat, ticks, candles, or other healthy requests.

### MetaTrader 5 bridge

The source EA is included at:

```text
bridges/mt5/Experts/AxetosMarketDataBridge.mq5
```

Compile it in MetaEditor, attach it to one chart in each MT5 terminal, and add the configured server origin (for example `http://127.0.0.1:8000`) under **Tools → Options → Expert Advisors → Allow WebRequest for listed URL**. Use a distinct provider key and bridge token for each configured terminal.

## Version 0.39.0

Version 0.39.0 adds dedicated deployment probes. `GET /api/live` reports process liveness and uptime without depending on provider or database state. `GET /api/ready` verifies that the service can access its database and returns HTTP 503 when the server is not ready to accept traffic. Degraded provider availability remains ready because management, recovery, and historical-data operations are still available. These endpoints are suitable for Docker, Kubernetes, reverse-proxy, and load-balancer health checks.

## Version 0.38.0

Version 0.38.0 fixes the dashboard aggregate market-feed status. A single inactive instrument no longer forces the entire market feed to display `INACTIVE` while other selected instruments are still receiving live observations. Mixed operational and degraded feeds now report `PARTIAL`; all-live or live-and-quiet combinations report `LIVE`; and the feed-status API includes per-state counts for diagnostics.

## Version 0.37.0

Version 0.37.0 adds production-friendly secret-file loading for every API authentication credential. Operators can set `AXETOS_VIEWER_TOKEN_FILE`, `AXETOS_OPERATOR_TOKEN_FILE`, `AXETOS_ADMIN_TOKEN_FILE`, or `AXETOS_BRIDGE_TOKEN_FILE` to read a token from a mounted Docker/Kubernetes secret instead of placing it directly in the process environment. Direct and file-based values are mutually exclusive, unreadable or empty secret files fail startup, and all configured role and bridge tokens must be distinct so one leaked credential cannot silently cross a security boundary.

### Mounted authentication secrets

Append `_FILE` to any token variable and point it at a UTF-8 text file containing one token. A trailing newline is ignored:

```yaml
environment:
  AXETOS_AUTH_ENABLED: "true"
  AXETOS_ADMIN_TOKEN_FILE: /run/secrets/axetos_admin_token
  AXETOS_BRIDGE_TOKEN_FILE: /run/secrets/axetos_bridge_token
```

Do not configure both the direct variable and its `_FILE` counterpart. Administrator, operator, viewer, and bridge token values must be different from one another.

## Version 0.36.0

Version 0.36.0 prevents accidental unauthenticated network exposure. The server now refuses to bind to a non-loopback interface such as `0.0.0.0` or `::` while authentication is disabled. Local development on `127.0.0.1`, `::1`, or `localhost` remains unchanged. Operators behind an external trusted access-control boundary can explicitly acknowledge the risk with `--allow-insecure-public-bind`. Docker Compose now enables authentication by default and requires separate administrator and MT5 bridge tokens before startup.

## Version 0.35.0

Version 0.35.0 hardens machine-local runtime state against interrupted writes. Provider configuration and encrypted MT5 secrets are now written to a temporary file, flushed to durable storage, and atomically replaced, so the active JSON file is never exposed in a partially written state. Failed replacements preserve the previous file and clean up temporary artifacts. Secret files retain owner-only permissions on supported platforms.

Version 0.34.2 fixes the PostgreSQL CI batch-insert path by executing `executemany()` through a Psycopg cursor and retaining SQLite-compatible affected-row tracking. The real PostgreSQL 16 integration test now exercises the same batch tick insertion path used by the server.

Version 0.34.1 fixes the first GitHub Actions run by including NumPy in development dependencies and adding SQLite-compatible affected-row tracking to the PostgreSQL connection adapter. CI continues to validate SQLite across Python 3.11–3.13 and a real PostgreSQL 16 round trip.

Version 0.34.0 added continuous integration for both the default SQLite deployment and the optional PostgreSQL backend. GitHub Actions now runs the complete test suite across supported Python versions and starts an ephemeral PostgreSQL 16 service for a real schema, write, read, operational-event, and restart-initialization round trip. This closes the previous gap where PostgreSQL compatibility was validated only through SQL translation and portable-schema unit tests.

The PostgreSQL integration test remains skipped during ordinary local runs unless `AXETOS_TEST_POSTGRES_URL` is configured.

The current release retains the earlier feed and provider guarantees: normalized midpoint pricing prevents spread-only flicker from being treated as market movement; connected infrastructure keeps system health healthy while an instrument feed may be inactive; the dashboard separates configured, MT5-selected, monitored, and stored instruments; and MT5 startup uses conditional account authentication before resilient IPC recovery.

## Secure network binding

The default command binds only to `127.0.0.1`, so authentication may remain disabled for local development. Binding to another interface requires authentication:

```powershell
$env:AXETOS_AUTH_ENABLED="true"
$env:AXETOS_ADMIN_TOKEN="replace-with-a-long-random-admin-token"
$env:AXETOS_BRIDGE_TOKEN="replace-with-a-different-long-random-bridge-token"
axetos-market-data-server --host 0.0.0.0
```

Viewer and operator tokens remain optional; without them, only the administrator token can access management endpoints. The MT5 ingestion endpoints use the separate bridge token.

For a deployment already protected by a trusted reverse proxy or private network boundary, the safety check can be deliberately bypassed:

```powershell
axetos-market-data-server --host 0.0.0.0 --allow-insecure-public-bind
```

This override disables only the startup guard; it does not add authentication or transport encryption.

## MT5 terminal and account lifecycle

For an MT5 provider, startup follows this sequence:

1. Connect to the configured terminal, starting `terminal64.exe` through `MetaTrader5.initialize()` when needed.
2. Verify that the terminal is connected to the broker.
3. Inspect the currently active account with `account_info()`.
4. If the configured login and server already match, continue without sending a login request.
5. If they do not match, read the password from the configured environment variable, call `mt5.login()`, and verify the resulting account before selecting symbols.

The provider editor accepts the MT5 password through a masked field. On Windows it is protected with DPAPI in a machine-local runtime secret store and is never written to `providers.json` or returned by the API. For headless deployments, an environment-variable reference remains supported. Example:

```powershell
$env:AXETOS_MT5_OANDA_PASSWORD="replace-with-the-account-password"
```

Configure the provider with account login `12345678`, server `OANDA-Demo`, and password environment variable `AXETOS_MT5_OANDA_PASSWORD`. The UI then reports terminal running state, broker connection, active login, and account server separately from the market-feed state.

## Feed-derived market status and candle continuity

The server does not assume that a market is open or closed solely from a calendar. Each provider/instrument feed is classified from received quote behavior:

```text
INITIALIZING → LIVE → QUIET → STALLED → INACTIVE
                              ↓
                         RECOVERING → LIVE
```

The default thresholds are 60 seconds for `QUIET`, 180 seconds for `STALLED`, and 600 seconds for `INACTIVE`. They are configurable per provider. `INACTIVE` never stops the MT5 worker: the terminal remains connected and monitored so genuine price movement is detected immediately when the market or instrument becomes active again.

Identical bid/ask observations are counted diagnostically but are not persisted as genuine market ticks and do not create flat candles. When movement resumes after a stalled or inactive interval, the server requests one-minute history for the missing window. Meaningful historical candles are imported; empty or flat-only history is classified as an inactive interval and is left without synthetic candles.

Candle continuity follows the feed conclusion:

- Continuous feed: the new candle opens at the previous candle close, and that opening value participates in high/low.
- Repaired active interval: recovered candles establish continuity before the live candle resumes.
- Closed, inactive, or otherwise unverified interval: the new candle opens at the first genuine resumed price, preserving real gaps such as stock premarket/regular-session jumps and weekend FX gaps.

Inspect the current report through:

```text
GET /api/feed-status
```

The management UI shows the current state, unchanged duration, accepted market ticks, ignored unchanged observations, recovery attempts, and repaired candle count.

## Storage backends

### SQLite (default)

No configuration is required:

```text
data/market_data.sqlite
```

SQLite uses WAL mode, supports the built-in integrity check, and supports optional compaction through the retention endpoint.

### PostgreSQL

Install the optional driver:

```powershell
pip install -e ".[dev,postgres]"
```

Set a PostgreSQL connection URL before starting the service:

```powershell
$env:AXETOS_DATABASE_URL="postgresql://axetos:replace-me@localhost:5432/axetos_market_data"
axetos-market-data-server
```

The server creates the required tables and indexes when the database user has schema permissions. PostgreSQL maintenance such as `VACUUM`, backup, replication, and point-in-time recovery remains the responsibility of PostgreSQL administration tooling; the API will not attempt SQLite-specific compaction against PostgreSQL.

Inspect the active backend through:

```text
GET /api/storage
```

## Live consumer stream

Trading-platform clients can consume live normalized ticks and stored candle updates through Server-Sent Events:

```text
GET /api/stream/live
GET /api/stream/status
```

Optional repeated query parameters filter the stream:

```text
/api/stream/live?instrument=EUR%2FUSD&provider=ICMarkets.MT5&event_type=tick&event_type=candle
```

The stream emits `ready`, `tick`, and `candle` event types and sends heartbeat comments during quiet periods. When authentication is enabled, a Viewer, Operator, or Administrator management token is required. The endpoint is intended for live display and downstream notification; The configured durable store remains the authoritative history.

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
- Persistent scheduled retention with automatic execution and run history
- Maintenance status and manual trigger in the management UI

- Live SSE tick and candle stream with provider/instrument filtering
- Bounded subscriber queues and stream health counters
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
- Optional PostgreSQL storage through the same market-data store API
- Provider configuration persisted in JSON
- Canonical symbol normalization with provider-specific aliases and policy overrides
- Browser-based provider control center
- REST endpoints and OpenAPI documentation
- Graceful shutdown with active candles stored as incomplete
- Automated tests through GitHub Actions
- Database integrity checks and retention previews
- Verified SQLite backup archives with checksums, manifests, and restore tooling
- Safe cleanup of old raw ticks and operational history, including structured operational events
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

## Market sessions and historical fallback

Version 0.12.0 makes gap detection session-aware. FX uses a Sunday 22:00 UTC to Friday 22:00 UTC trading week, crypto remains 24x7, and major index/metal/energy instruments use deterministic exchange-holiday closures. The calendar can be inspected through `GET /api/calendar/closures`.

A historical-only Yahoo Finance adapter is available as a controlled fallback source. It imports and validates provider candles but never participates in live tick routing and never blends candles with an authoritative provider. Configure it with `kind: yahoo`, disable auto-start, and use the existing backfill endpoint.

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
        +----> Durable tick store (SQLite or PostgreSQL)
        |
        v
1-minute candle builder
        |
        v
Higher-timeframe aggregation
        |
        v
Durable candle store (SQLite or PostgreSQL)

Historical provider data
        |
        v
Backfill -> validation -> gap detection -> repair -> audit history
```

The server is intentionally separated from any trading platform. A trading platform should normally consume the REST or live-stream endpoints, but order execution remains outside this project.

## Requirements

- Python 3.11 or newer
- MetaTrader 5 terminal and the `MetaTrader5` Python package only when using an MT5 provider
- PostgreSQL 14 or newer and `psycopg` only when selecting PostgreSQL storage

### Reference market price

For spot FX, MT5 commonly supplies bid and ask quotes without a reliable centralized last-traded price. Axetos therefore derives a reference market price from the normalized midpoint:

```text
market_price = normalize((bid + ask) / 2, broker_quote_precision)
```

The feed-state engine and live candle builder use this same value. Bid and ask are still retained for spread diagnostics and execution-facing consumers. A spread change that leaves the normalized midpoint unchanged is counted as a provider observation but is not accepted as market movement. Exchange-traded instruments can later prefer a reliable provider-supplied last-trade price when available.

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

For PostgreSQL support:

```powershell
pip install -e ".[dev,postgres]"
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
- Liveness probe: `http://127.0.0.1:8000/api/live`
- Readiness probe: `http://127.0.0.1:8000/api/ready`
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
| `GET` | `/api/live` | Process liveness and uptime |
| `GET` | `/api/ready` | Traffic readiness; returns HTTP 503 when unavailable |
| `GET` | `/api/health` | Server health and version |
| `GET` | `/api/storage` | Active storage backend and backend capabilities |
| `GET` | `/api/operational-events` | Query structured operational events with filtering and pagination |
| `GET` | `/api/symbol-normalization` | Preview canonical resolution for a provider symbol |
| `GET/PUT/DELETE` | `/api/symbol-policies` | Manage explicit provider-symbol mappings and routing policy |
| `GET` | `/api/operational-events/export` | Export filtered operational events as CSV or JSONL |
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

## Docker deployment

Version 0.25.0 introduced a production-oriented container image and a Docker Compose stack with PostgreSQL. Copy the environment template, replace the example password and tokens, then start the stack:

```powershell
Copy-Item .env.example .env
docker compose up --build -d
```

The management UI is available at `http://localhost:8000`, and the health endpoint is available at `http://localhost:8000/api/health`. Both services have readiness health checks and restart policies. PostgreSQL data, application runtime data, and backup archives use named Docker volumes.

```powershell
docker compose ps
docker compose logs -f market-data-server
docker compose down
docker compose down -v  # also removes persisted volumes
```

The Linux container does not provide the Windows-only direct MetaTrader 5 Python terminal integration. Use the MT5 bridge ingestion path for container deployments, or run the direct MT5 provider on Windows outside Docker.

## Management UI benchmark

Administrators can select **Run benchmark** in the management toolbar. Enter the synthetic tick count, instrument count, and one or more comma-separated batch sizes. The job runs in a background thread and the dialog reports progress, throughput, elapsed time, peak Python memory, and the best-performing tested batch size.

Benchmarking creates heavy database load. Use it on a development or maintenance instance rather than during normal live ingestion.

## Ingestion benchmark

Run a repeatable synthetic workload without requiring MT5 or an external market-data provider:

```powershell
axetos-market-data benchmark --ticks 100000 --instruments 10 --batch-size 1000
```

By default, the benchmark uses a temporary SQLite database and removes it afterward. Preserve the benchmark database by supplying the normal database path together with `--keep-database`:

```powershell
axetos-market-data --database data/benchmark.sqlite benchmark --ticks 1000000 --instruments 25 --batch-size 5000 --keep-database --output benchmark-results/v0.34.2.json
```

The JSON result records requested and written ticks, elapsed time, sustained throughput, candle count, peak Python memory, backend name, and database size. Results are intended for comparing releases on the same machine; they are not presented as universal hardware-independent performance claims.

## Endurance and regression checks

Run repeated isolated ingestion cycles for a fixed duration:

```powershell
axetos-market-data endurance --duration-seconds 300 --ticks-per-cycle 100000 --instruments 25 --batch-size 5000 --output benchmark-results/endurance-v0.34.2.json
```

The report includes average, median, minimum, and maximum throughput, total ticks, peak Python memory, and throughput drift from the first cycle to the last. The command exits with code `2` when negative drift exceeds `--regression-threshold-percent`, making it suitable for release checks and CI. Longer multi-hour runs should be performed on dedicated development or staging hosts.

## Roadmap

- Automated schema migrations and deployment upgrade verification
- Multi-provider, multi-subscriber endurance scenarios

## Candle quality and recovery

Version 0.12.0 adds a persistent candle-quality pipeline. The server can scan stored candles for invalid OHLC structure, non-positive prices, negative tick counts, and configurable price discontinuities. Suspect records are logged without silently changing market history. Operators can quarantine an affected candle and rebuild a one-minute candle deterministically from the raw ticks retained for that minute.

Quality endpoints:

```text
POST /api/quality/scan
GET  /api/quality/issues
POST /api/quality/issues/{issue_id}/quarantine
POST /api/quality/issues/{issue_id}/rebuild
```

Original quarantined values remain in `quarantined_candles` for auditability.

## Operational alert webhooks

Version 0.23.0 can deliver structured JSON alerts to a generic webhook when important operational events occur. Configure the destination through environment variables; webhook URLs are never stored in the project files or management database.

```powershell
$env:AXETOS_ALERT_WEBHOOK_URL="https://monitoring.example.com/hooks/axetos"
$env:AXETOS_ALERT_MIN_SEVERITY="error"
$env:AXETOS_ALERT_COOLDOWN_SECONDS="60"
axetos-market-data-server
```

By default, provider failures and recovery, failed connection tests, failed maintenance, quality incidents, failed backfills, and database-integrity failures are eligible for delivery. Override the category set with `AXETOS_ALERT_CATEGORIES` as a comma-separated list. Repeated identical alerts are suppressed during the configured cooldown.

The webhook receives the event ID, version, severity, category, provider, canonical instrument, message, UTC timestamp, and structured details. Delivery success or failure is written back to the operational journal.

```text
GET  /api/alerts/status
POST /api/alerts/test
```

## Backup and restore

Version 0.24.0 provides built-in backup and restore commands for SQLite deployments. The backup operation uses SQLite's online backup API, so it creates a transactionally consistent copy even while the source database uses WAL mode. The resulting ZIP contains the database, optional `providers.json`, and a manifest with SHA-256 checksums.

```powershell
axetos-market-data --database data/market_data.sqlite backup
axetos-market-data --database data/market_data.sqlite verify-backup data/backups/axetos-market-data-YYYYMMDDTHHMMSSZ.zip
axetos-market-data --database restored/market_data.sqlite restore data/backups/axetos-market-data-YYYYMMDDTHHMMSSZ.zip --configuration restored/providers.json
```

Restoring over an existing database requires `--overwrite`. Stop the server before restoring its active database. PostgreSQL deployments must use `pg_dump` and `pg_restore`; the built-in archive format intentionally does not attempt to replace PostgreSQL-native backup and recovery tooling.

Management endpoints:

```text
GET  /api/backups
POST /api/backups
```

The management UI includes a **Create backup** action.

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

## Authentication and role-based access

Authentication is disabled by default for local development. Enable it with environment variables before starting the server:

```powershell
$env:AXETOS_AUTH_ENABLED="true"
$env:AXETOS_VIEWER_TOKEN="replace-with-a-long-random-viewer-token"
$env:AXETOS_OPERATOR_TOKEN="replace-with-a-long-random-operator-token"
$env:AXETOS_ADMIN_TOKEN="replace-with-a-long-random-administrator-token"
$env:AXETOS_BRIDGE_TOKEN="replace-with-a-separate-long-random-bridge-token"
axetos-market-data-server
```

Management clients may send a token as `Authorization: Bearer <token>`, `X-API-Key: <token>`, or as the password in HTTP Basic authentication. HTTP Basic support allows a browser to open the management UI using its normal credential prompt; the username is informational and the password must be a configured management token.

Roles are cumulative:

- **Viewer** can read health, metrics, providers, candles, routing, quality results, and operational history.
- **Operator** includes Viewer access and can start or stop providers, run connection tests, backfills, scans, repairs, rebuilds, and maintenance jobs.
- **Administrator** includes Operator access and can alter provider configuration, symbol policies, retention schedules, and destructive resources.

The MT5 ingestion endpoints use only `AXETOS_BRIDGE_TOKEN`. A management token cannot ingest bridge data, and the bridge token does not grant management access. Send it with `X-API-Key` or `Authorization: Bearer`.

`GET /api/health`, `/metrics`, and API documentation remain available without credentials for monitoring and service discovery. Do not expose the server directly to the public internet; terminate TLS at a trusted reverse proxy and store all tokens in a secret manager or protected environment configuration.

## Managed MT5 symbols

Version 0.20.0 adds a dedicated MT5 symbol-management workflow. The management UI can discover symbols from the configured terminal, show broker descriptions, propose canonical Axetos instruments, and persist each mapping as Confirmed, Needs review, or Ignored. Provider symbols remain separate from canonical instruments: MT5 receives broker names such as `EURUSD.pro`, while storage, routing, candles, APIs, and streams use `EUR/USD`.

Automatic guesses are intentionally disabled until reviewed. This prevents ambiguous broker CFDs from entering the authoritative market-data catalog.

### v0.46.0 Trading Platform read contract

- Resolves the canonical provider server-side when candle or quote requests omit a provider.
- Adds the canonical `/api/quotes/{instrument}` read endpoint.
- Keeps compatibility read endpoints for older Trading Platform clients while `/api/candles` remains authoritative.
- Candle and quote responses identify the active provider and never require client-side candle generation.
