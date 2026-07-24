from __future__ import annotations

import logging
import threading
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from .config import ConfigurationStore, ProviderConfig
from .providers.mock import MockTickProvider
from .providers.mt5 import MetaTrader5TickProvider
from .service import MarketDataService
from .routing import ProviderAuthorityRegistry
from .maintenance import ProviderMaintenanceWorker
from .storage import MarketDataStore
from .operational import OperationalEventService


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


class ProviderWorker:
    def __init__(self, config: ProviderConfig, store: MarketDataStore, authority: ProviderAuthorityRegistry) -> None:
        self.config = config
        self.store = store
        self.authority = authority
        self.runtime = ProviderRuntime(provider_key=config.provider_key)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.log = logging.getLogger(f"provider.{config.provider_key}")
        self.events = OperationalEventService(store)

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
                self.config.batch_window_seconds, self.config.batch_limit,
            )
        return MockTickProvider(symbols[0], self.config.poll_interval_seconds, provider=self.config.provider_key)

    def _run(self) -> None:
        self.runtime.status = "Starting"
        self.runtime.started_utc = datetime.now(UTC).isoformat()
        self.runtime.last_error = None
        service = MarketDataService(self.store)
        try:
            provider = self._provider()
            self.runtime.status = "Live"
            self.events.record("info", "provider.recovery", "Provider is live", provider=self.config.provider_key, details={"kind": self.config.kind})
            for tick in provider.stream():
                if self._stop.is_set():
                    break
                self.authority.record_tick(self.config.provider_key, tick.instrument, tick.timestamp)
                if self.authority.is_authoritative(self.config.provider_key, tick.instrument, tick.timestamp):
                    service.run([tick])
                    self.runtime.authoritative_ticks += 1
                else:
                    self.runtime.standby_ticks += 1
                now = datetime.now(UTC).isoformat()
                self.runtime.last_heartbeat_utc = now
                self.runtime.last_tick_utc = tick.timestamp.isoformat()
                self.runtime.ticks_received += 1
            service.builder.flush(complete=False)
        except Exception as exc:  # provider boundary
            self.runtime.status = "Failed"
            self.runtime.last_error = str(exc)
            self.log.exception("Provider worker failed")
            self.events.record("error", "provider.failure", "Provider worker failed", provider=self.config.provider_key, details={"error": str(exc), "kind": self.config.kind})
        finally:
            if self.runtime.status != "Failed":
                self.runtime.status = "Stopped"

    def view(self) -> dict[str, object]:
        return {"configuration": asdict(self.config), "runtime": asdict(self.runtime)}


class ProviderSupervisor:
    def __init__(self, config_store: ConfigurationStore, data_store: MarketDataStore) -> None:
        self.config_store = config_store
        self.data_store = data_store
        self._workers: dict[str, ProviderWorker] = {}
        self._maintenance: dict[str, ProviderMaintenanceWorker] = {}
        self.authority = ProviderAuthorityRegistry()
        self._lock = threading.RLock()

    def load(self) -> None:
        with self._lock:
            configs = self.config_store.read_all()
            self.authority.replace_configs(configs)
            for config in configs:
                self._workers[config.provider_key] = ProviderWorker(config, self.data_store, self.authority)
                if config.kind.lower() == "mt5" and config.maintenance_enabled:
                    maintenance = ProviderMaintenanceWorker(config, self.data_store)
                    self._maintenance[config.provider_key] = maintenance
                    maintenance.start()
                if config.enabled and config.auto_start:
                    self._workers[config.provider_key].start()

    def shutdown(self) -> None:
        with self._lock:
            for worker in self._workers.values():
                worker.stop()
            for worker in self._maintenance.values():
                worker.stop()

    def list_views(self) -> list[dict[str, object]]:
        with self._lock:
            views = []
            for key, worker in self._workers.items():
                view = worker.view()
                view["maintenance"] = self._maintenance[key].view() if key in self._maintenance else None
                views.append(view)
            return views

    def get(self, provider_key: str) -> ProviderWorker | None:
        with self._lock:
            return self._workers.get(provider_key)

    def upsert(self, config: ProviderConfig) -> dict[str, object]:
        with self._lock:
            existing = self._workers.pop(config.provider_key, None)
            if existing:
                existing.stop()
            old_maintenance = self._maintenance.pop(config.provider_key, None)
            if old_maintenance:
                old_maintenance.stop()
            self.config_store.upsert(config)
            self.authority.replace_configs(self.config_store.read_all())
            worker = ProviderWorker(config, self.data_store, self.authority)
            self._workers[config.provider_key] = worker
            if config.kind.lower() == "mt5" and config.maintenance_enabled:
                maintenance = ProviderMaintenanceWorker(config, self.data_store)
                self._maintenance[config.provider_key] = maintenance
                maintenance.start()
            if config.enabled and config.auto_start:
                worker.start()
            return worker.view()

    def action(self, provider_key: str, action: str) -> dict[str, object]:
        worker = self.get(provider_key)
        if worker is None:
            raise KeyError(provider_key)
        if action in {"start", "reconnect", "restart"}:
            worker.events.record("info", f"provider.{action}", f"Provider {action} requested", provider=provider_key)
            worker.stop()
            worker.start()
        elif action == "stop":
            worker.stop()
        elif action == "enable":
            worker.config.enabled = True
            self.config_store.upsert(worker.config)
            self.authority.replace_configs(self.config_store.read_all())
            worker.start()
        elif action == "maintenance":
            maintenance = self._maintenance.get(provider_key)
            if maintenance is None:
                raise ValueError("Scheduled maintenance is not enabled for this provider")
            maintenance.trigger()
        elif action == "disable":
            worker.config.enabled = False
            self.config_store.upsert(worker.config)
            self.authority.replace_configs(self.config_store.read_all())
            worker.stop()
        else:
            raise ValueError(f"Unsupported action: {action}")
        time.sleep(0.05)
        return worker.view()

    def remove(self, provider_key: str) -> bool:
        with self._lock:
            worker = self._workers.pop(provider_key, None)
            if worker:
                worker.stop()
            maintenance = self._maintenance.pop(provider_key, None)
            if maintenance:
                maintenance.stop()
            removed = self.config_store.delete(provider_key)
            self.authority.replace_configs(self.config_store.read_all())
            return removed
