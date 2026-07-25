from __future__ import annotations

import logging
import threading
import time
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from .config import ConfigurationStore, ProviderConfig
from .feed import FeedStateEngine, FeedThresholds
from .providers.mock import MockTickProvider
from .providers.mt5 import MetaTrader5TickProvider
from .service import MarketDataService
from .routing import ProviderAuthorityRegistry
from .maintenance import ProviderMaintenanceWorker
from .storage import MarketDataStore
from .operational import OperationalEventService
from .symbols import SymbolResolver
from .timeframes import bucket_start
from .secrets import SecretStore


@dataclass(slots=True)
class ProviderRuntime:
    provider_key: str
    status: str = "Stopped"
    started_utc: str | None = None
    last_heartbeat_utc: str | None = None
    last_tick_utc: str | None = None
    ticks_received: int = 0
    last_error: str | None = None
    authoritative_ticks: int = 0
    standby_ticks: int = 0
    accepted_market_ticks: int = 0
    ignored_unchanged_updates: int = 0
    recovery_attempts: int = 0
    recovery_candles_written: int = 0
    terminal_running: bool = False
    terminal_connected: bool = False
    broker_connected: bool = False
    account_logged_in: bool = False
    account_login: int | None = None
    account_server: str | None = None
    account_company: str | None = None
    account_name: str | None = None
    login_attempted: bool = False
    reconnecting: bool = False
    reconnect_attempts: int = 0
    last_reconnect_utc: str | None = None


class ProviderWorker:
    def __init__(self, config: ProviderConfig, store: MarketDataStore, authority: ProviderAuthorityRegistry, secret_store: SecretStore | None = None) -> None:
        self.config = config
        self.store = store
        self.authority = authority
        self.secret_store = secret_store
        self.runtime = ProviderRuntime(provider_key=config.provider_key)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.log = logging.getLogger(f"provider.{config.provider_key}")
        self.events = OperationalEventService(store)
        self.feed = FeedStateEngine(FeedThresholds(
            config.feed_quiet_seconds, config.feed_stalled_seconds, config.feed_inactive_seconds,
        ))
        resolver = SymbolResolver(store)
        self._symbol_entries: list[dict[str, object]] = []
        self._provider_instance = None
        for provider_symbol in config.normalized_symbols():
            instrument = resolver.resolve(config.provider_key, provider_symbol).canonical_instrument
            self._symbol_entries.append({"provider_symbol": provider_symbol, "canonical_instrument": instrument, "selection_state": "configured", "selected": False, "error": None})
            latest = store.latest_tick_for(config.provider_key, instrument)
            timestamp = None
            bid = ask = market_price = None
            if latest is not None:
                timestamp = datetime.fromisoformat(str(latest["timestamp_utc"]))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=UTC)
                bid = Decimal(str(latest["bid"]))
                ask = Decimal(str(latest["ask"]))
                market_price = (bid + ask) / Decimal("2")
            self.feed.seed_inactive(config.provider_key, instrument, timestamp, market_price, bid, ask)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name=self.config.provider_key)
        self._thread.start()
        self.events.record("info", "provider.start", "Provider start requested", provider=self.config.provider_key, details={"kind": self.config.kind})

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self.runtime.status = "Stopped"
        self.events.record("info", "provider.stop", "Provider stopped", provider=self.config.provider_key)

    def _provider(self):
        symbols = self.config.normalized_symbols()
        if self.config.kind.lower() == "yahoo":
            raise RuntimeError("Yahoo provider is historical-only; disable auto-start and use backfill/repair actions")
        if self.config.kind.lower() == "mt5":
            return MetaTrader5TickProvider(
                symbols, self.config.terminal_path, self.config.provider_key,
                self.config.batch_window_seconds, self.config.batch_limit, self.config.symbol_aliases, self.store,
                self.config.account_login, self.config.account_server, self.config.password_env,
                self.secret_store.get(self.config.provider_key) if self.secret_store else None,
            )
        return MockTickProvider(symbols[0], self.config.poll_interval_seconds, provider=self.config.provider_key)

    def _provider_symbol(self, instrument: str) -> str | None:
        resolver = SymbolResolver(self.store)
        for symbol in self.config.normalized_symbols():
            if resolver.resolve(self.config.provider_key, symbol).canonical_instrument == instrument:
                return symbol
        return None

    @staticmethod
    def _meaningful_history(candles, reference_price=None) -> bool:
        if not candles:
            return False
        prices = set()
        for candle in candles:
            prices.update((candle.open, candle.high, candle.low, candle.close))
            if candle.high != candle.low or candle.open != candle.close:
                return True
        if reference_price is not None and any(price != reference_price for price in prices):
            return True
        return len(prices) > 1

    def _recover(self, provider, service: MarketDataService, tick, start: datetime, end: datetime) -> tuple[str, bool]:
        self.runtime.recovery_attempts += 1
        reference = service.builder.finalize(tick.provider, tick.instrument, complete=True)
        symbol = self._provider_symbol(tick.instrument)
        if not symbol or not hasattr(provider, "fetch_candles_live"):
            return "history_unavailable_detached", False
        history_start = bucket_start(start, "1m") + timedelta(minutes=1)
        history_end = bucket_start(end, "1m")
        if history_start >= history_end:
            return "no_missing_minutes_connected", True
        try:
            candles = provider.fetch_candles_live(symbol, "1m", history_start, history_end)
        except Exception as exc:
            self.events.record("warning", "feed.recovery", "Feed recovery history request failed", provider=tick.provider, instrument=tick.instrument, details={"error": str(exc), "from_utc": history_start.isoformat(), "to_utc": history_end.isoformat()})
            return "history_request_failed_detached", False
        reference_price = reference.close if reference is not None else None
        meaningful = self._meaningful_history(candles, reference_price)
        if meaningful:
            written = self.store.upsert_candles(candles)
            self.runtime.recovery_candles_written += written
            self.events.record("info", "feed.recovery", "Feed gap repaired from provider history", provider=tick.provider, instrument=tick.instrument, details={"received": len(candles), "written": written, "from_utc": history_start.isoformat(), "to_utc": history_end.isoformat()})
            return f"repaired_{written}_candles", True
        self.events.record("info", "feed.inactive_interval", "Inactive interval contained no meaningful market movement", provider=tick.provider, instrument=tick.instrument, details={"received": len(candles), "from_utc": history_start.isoformat(), "to_utc": history_end.isoformat()})
        return "inactive_interval_no_candles", False

    def _run(self) -> None:
        self.runtime.status = "Starting"
        self.runtime.started_utc = datetime.now(UTC).isoformat()
        self.runtime.last_error = None
        service = MarketDataService(self.store)
        previous_states: dict[str, str] = {}
        try:
            provider = self._provider()
            self._provider_instance = provider
            # stream() initializes/starts MT5 and authenticates before yielding its first tick.
            self.runtime.status = "Live"
            self.events.record("info", "provider.recovery", "Provider is live", provider=self.config.provider_key, details={"kind": self.config.kind})
            for tick in provider.stream():
                if self._stop.is_set():
                    break
                decision = self.feed.observe(tick)
                if decision.accept_tick:
                    self.authority.record_tick(self.config.provider_key, tick.instrument, tick.timestamp)
                authoritative = self.authority.is_authoritative(self.config.provider_key, tick.instrument, tick.timestamp)
                prior_logged = previous_states.get(tick.instrument)
                if decision.state != prior_logged:
                    severity = "warning" if decision.state in {"STALLED", "INACTIVE"} else "info"
                    self.events.record(severity, "feed.state", f"Market feed changed to {decision.state}", provider=tick.provider, instrument=tick.instrument, details={"previous_state": decision.previous_state})
                    previous_states[tick.instrument] = decision.state
                if authoritative:
                    self.runtime.authoritative_ticks += 1
                    if decision.accept_tick:
                        continuity = decision.continuity
                        if decision.recovery_required and decision.recovery_from_utc and decision.recovery_to_utc:
                            result, connected = self._recover(provider, service, tick, decision.recovery_from_utc, decision.recovery_to_utc)
                            self.feed.complete_recovery(tick.provider, tick.instrument, result, connected)
                            continuity = "CONNECTED" if connected else "DETACHED"
                            previous_states[tick.instrument] = "LIVE"
                        service.run([tick], continuity=continuity)
                        self.runtime.accepted_market_ticks += 1
                    else:
                        self.runtime.ignored_unchanged_updates += 1
                else:
                    self.runtime.standby_ticks += 1
                now = datetime.now(UTC).isoformat()
                self.runtime.last_heartbeat_utc = now
                self.runtime.last_tick_utc = tick.timestamp.isoformat()
                self.runtime.ticks_received += 1
            service.builder.flush(complete=False)
        except Exception as exc:
            self.runtime.status = "Failed"
            self.runtime.last_error = str(exc)
            self.log.exception("Provider worker failed")
            self.events.record("error", "provider.failure", "Provider worker failed", provider=self.config.provider_key, details={"error": str(exc), "kind": self.config.kind})
        finally:
            if self.runtime.status != "Failed":
                self.runtime.status = "Stopped"

    def symbol_statuses(self) -> list[dict[str, object]]:
        statuses = getattr(self._provider_instance, "selection_status", {}) if self._provider_instance is not None else {}
        rows: list[dict[str, object]] = []
        for entry in self._symbol_entries:
            status = statuses.get(str(entry["provider_symbol"]), {})
            selected = bool(status.get("selected", False))
            error = status.get("error")
            rows.append({**entry, "selected": selected, "error": error, "selection_state": "selected" if selected else ("failed" if error else "configured")})
        return rows

    def view(self) -> dict[str, object]:
        symbols = self.symbol_statuses()
        session = getattr(self._provider_instance, "session_status", {}) if self._provider_instance is not None else {}
        for field in ("terminal_running", "terminal_connected", "broker_connected", "account_logged_in",
                      "account_login", "account_server", "account_company", "account_name", "login_attempted",
                      "reconnecting", "reconnect_attempts", "last_reconnect_utc"):
            if field in session:
                setattr(self.runtime, field, session[field])
        configuration = asdict(self.config)
        secret_configured = bool(self.secret_store and self.secret_store.configured(self.config.provider_key))
        configuration["password_configured"] = secret_configured or bool(self.config.password_env)
        configuration["password_env_configured"] = bool(self.config.password_env)
        configuration["password_env"] = "********" if self.config.password_env else None
        return {
            "configuration": configuration,
            "runtime": asdict(self.runtime),
            "feeds": self.feed.reports(),
            "symbols": symbols,
            "configured_instruments": len({str(row["canonical_instrument"]) for row in symbols}),
            "selected_instruments": sum(1 for row in symbols if row["selected"]),
            "failed_instruments": sum(1 for row in symbols if row["selection_state"] == "failed"),
        }


class ProviderSupervisor:
    def __init__(self, config_store: ConfigurationStore, data_store: MarketDataStore, secret_store: SecretStore | None = None) -> None:
        self.config_store = config_store
        self.data_store = data_store
        self.secret_store = secret_store
        self._workers: dict[str, ProviderWorker] = {}
        self._maintenance: dict[str, ProviderMaintenanceWorker] = {}
        self.authority = ProviderAuthorityRegistry()
        self._lock = threading.RLock()

    def _routing_configs(self, configs: list[ProviderConfig]) -> list[ProviderConfig]:
        resolver = SymbolResolver(self.data_store)
        return [replace(config, symbols=[resolver.resolve(config.provider_key, symbol).canonical_instrument for symbol in config.normalized_symbols()]) for config in configs]

    def load(self) -> None:
        with self._lock:
            configs = self.config_store.read_all()
            self.authority.replace_configs(self._routing_configs(configs))
            for config in configs:
                self._workers[config.provider_key] = ProviderWorker(config, self.data_store, self.authority, self.secret_store)
                if config.kind.lower() == "mt5" and config.maintenance_enabled:
                    maintenance = ProviderMaintenanceWorker(config, self.data_store, self.secret_store)
                    self._maintenance[config.provider_key] = maintenance
                    maintenance.start()
                if config.enabled and config.auto_start:
                    self._workers[config.provider_key].start()

    def shutdown(self) -> None:
        with self._lock:
            for worker in self._workers.values(): worker.stop()
            for worker in self._maintenance.values(): worker.stop()

    def list_views(self) -> list[dict[str, object]]:
        with self._lock:
            views = []
            for key, worker in self._workers.items():
                view = worker.view()
                view["maintenance"] = self._maintenance[key].view() if key in self._maintenance else None
                views.append(view)
            return views

    def feed_reports(self) -> list[dict[str, object]]:
        with self._lock:
            return [report for worker in self._workers.values() for report in worker.feed.reports()]

    def get(self, provider_key: str) -> ProviderWorker | None:
        with self._lock: return self._workers.get(provider_key)

    def upsert(self, config: ProviderConfig) -> dict[str, object]:
        with self._lock:
            existing = self._workers.pop(config.provider_key, None)
            if existing: existing.stop()
            old_maintenance = self._maintenance.pop(config.provider_key, None)
            if old_maintenance: old_maintenance.stop()
            self.config_store.upsert(config)
            self.authority.replace_configs(self._routing_configs(self.config_store.read_all()))
            worker = ProviderWorker(config, self.data_store, self.authority, self.secret_store)
            self._workers[config.provider_key] = worker
            if config.kind.lower() == "mt5" and config.maintenance_enabled:
                maintenance = ProviderMaintenanceWorker(config, self.data_store, self.secret_store)
                self._maintenance[config.provider_key] = maintenance
                maintenance.start()
            if config.enabled and config.auto_start: worker.start()
            return worker.view()

    def action(self, provider_key: str, action: str) -> dict[str, object]:
        worker = self.get(provider_key)
        if worker is None: raise KeyError(provider_key)
        if action in {"start", "reconnect", "restart"}:
            worker.events.record("info", f"provider.{action}", f"Provider {action} requested", provider=provider_key)
            worker.stop(); worker.start()
        elif action == "stop": worker.stop()
        elif action == "enable":
            worker.config.enabled = True; self.config_store.upsert(worker.config)
            self.authority.replace_configs(self._routing_configs(self.config_store.read_all())); worker.start()
        elif action == "maintenance":
            maintenance = self._maintenance.get(provider_key)
            if maintenance is None: raise ValueError("Scheduled maintenance is not enabled for this provider")
            maintenance.trigger()
        elif action == "disable":
            worker.config.enabled = False; self.config_store.upsert(worker.config)
            self.authority.replace_configs(self._routing_configs(self.config_store.read_all())); worker.stop()
        else: raise ValueError(f"Unsupported action: {action}")
        time.sleep(0.05); return worker.view()

    def remove(self, provider_key: str) -> bool:
        with self._lock:
            worker = self._workers.pop(provider_key, None)
            if worker: worker.stop()
            maintenance = self._maintenance.pop(provider_key, None)
            if maintenance: maintenance.stop()
            removed = self.config_store.delete(provider_key)
            if self.secret_store:
                self.secret_store.delete(provider_key)
            self.authority.replace_configs(self._routing_configs(self.config_store.read_all()))
            return removed
