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
from .storage import MarketDataStore


@dataclass(slots=True)
class ProviderRuntime:
    provider_key: str
    status: str = "Stopped"
    started_utc: str | None = None
    last_heartbeat_utc: str | None = None
    last_tick_utc: str | None = None
    ticks_received: int = 0
    last_error: str | None = None


class ProviderWorker:
    def __init__(self, config: ProviderConfig, store: MarketDataStore) -> None:
        self.config = config
        self.store = store
        self.runtime = ProviderRuntime(provider_key=config.provider_key)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.log = logging.getLogger(f"provider.{config.provider_key}")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name=self.config.provider_key)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self.runtime.status = "Stopped"

    def _provider(self):
        symbols = self.config.normalized_symbols()
        if self.config.kind.lower() == "mt5":
            return MetaTrader5TickProvider(symbols, self.config.terminal_path)
        return MockTickProvider(symbols[0], self.config.poll_interval_seconds, provider=self.config.provider_key)

    def _run(self) -> None:
        self.runtime.status = "Starting"
        self.runtime.started_utc = datetime.now(UTC).isoformat()
        self.runtime.last_error = None
        service = MarketDataService(self.store)
        try:
            provider = self._provider()
            self.runtime.status = "Live"
            for tick in provider.stream():
                if self._stop.is_set():
                    break
                service.run([tick])
                now = datetime.now(UTC).isoformat()
                self.runtime.last_heartbeat_utc = now
                self.runtime.last_tick_utc = tick.timestamp.isoformat()
                self.runtime.ticks_received += 1
            service.builder.flush(complete=False)
        except Exception as exc:  # provider boundary
            self.runtime.status = "Failed"
            self.runtime.last_error = str(exc)
            self.log.exception("Provider worker failed")
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
        self._lock = threading.RLock()

    def load(self) -> None:
        with self._lock:
            for config in self.config_store.read_all():
                self._workers[config.provider_key] = ProviderWorker(config, self.data_store)
                if config.enabled and config.auto_start:
                    self._workers[config.provider_key].start()

    def shutdown(self) -> None:
        with self._lock:
            for worker in self._workers.values():
                worker.stop()

    def list_views(self) -> list[dict[str, object]]:
        with self._lock:
            return [worker.view() for worker in self._workers.values()]

    def get(self, provider_key: str) -> ProviderWorker | None:
        with self._lock:
            return self._workers.get(provider_key)

    def upsert(self, config: ProviderConfig) -> dict[str, object]:
        with self._lock:
            existing = self._workers.pop(config.provider_key, None)
            if existing:
                existing.stop()
            self.config_store.upsert(config)
            worker = ProviderWorker(config, self.data_store)
            self._workers[config.provider_key] = worker
            if config.enabled and config.auto_start:
                worker.start()
            return worker.view()

    def action(self, provider_key: str, action: str) -> dict[str, object]:
        worker = self.get(provider_key)
        if worker is None:
            raise KeyError(provider_key)
        if action in {"start", "reconnect", "restart"}:
            worker.stop()
            worker.start()
        elif action == "stop":
            worker.stop()
        elif action == "enable":
            worker.config.enabled = True
            self.config_store.upsert(worker.config)
            worker.start()
        elif action == "disable":
            worker.config.enabled = False
            self.config_store.upsert(worker.config)
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
            return self.config_store.delete(provider_key)
