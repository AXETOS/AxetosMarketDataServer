from __future__ import annotations

import logging
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from .history import HistoricalBackfillService
from .providers.mt5 import MetaTrader5TickProvider
from .storage import MarketDataStore


@dataclass(slots=True)
class MaintenanceRuntime:
    provider_key: str
    status: str = "Idle"
    last_started_utc: str | None = None
    last_completed_utc: str | None = None
    next_run_utc: str | None = None
    backfilled_candles: int = 0
    repaired_gaps: int = 0
    last_error: str | None = None


class ProviderMaintenanceWorker:
    def __init__(self, config, store: MarketDataStore) -> None:
        self.config = config
        self.store = store
        self.runtime = MaintenanceRuntime(config.provider_key)
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self.log = logging.getLogger(f"maintenance.{config.provider_key}")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"maintenance-{self.config.provider_key}")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def trigger(self) -> None:
        self._wake.set()

    def view(self) -> dict[str, object]:
        return asdict(self.runtime)

    def _run(self) -> None:
        interval = max(1, int(self.config.maintenance_interval_minutes)) * 60
        while not self._stop.is_set():
            self.runtime.next_run_utc = (datetime.now(UTC) + timedelta(seconds=interval)).isoformat()
            self._wake.wait(interval)
            self._wake.clear()
            if self._stop.is_set():
                break
            self._execute()

    def _execute(self) -> None:
        self.runtime.status = "Running"
        self.runtime.last_started_utc = datetime.now(UTC).isoformat()
        self.runtime.last_error = None
        written = repaired = 0
        try:
            provider = MetaTrader5TickProvider(
                self.config.normalized_symbols(),
                self.config.terminal_path,
                self.config.provider_key,
                self.config.batch_window_seconds,
                self.config.batch_limit,
                account_login=self.config.account_login,
                account_server=self.config.account_server,
                password_env=self.config.password_env,
            )
            history = HistoricalBackfillService(self.store)
            end = datetime.now(UTC)
            start = end - timedelta(days=max(1, int(self.config.maintenance_backfill_days)))
            symbol_map = {provider._canonical_symbol(s): s for s in self.config.normalized_symbols()}
            for instrument, symbol in symbol_map.items():
                result = history.run(provider, self.config.provider_key, symbol, instrument, "1m", start, end)
                written += result.written
            repair = history.repair_gaps(provider, self.config.provider_key, symbol_map, limit=5000)
            repaired += repair.gaps_resolved
            self.runtime.backfilled_candles += written
            self.runtime.repaired_gaps += repaired
            self.runtime.status = "Idle"
            self.runtime.last_completed_utc = datetime.now(UTC).isoformat()
        except Exception as exc:
            self.runtime.status = "Failed"
            self.runtime.last_error = str(exc)
            self.log.exception("Scheduled maintenance failed")
