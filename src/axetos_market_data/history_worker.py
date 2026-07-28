from __future__ import annotations

import multiprocessing as mp
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .domain import Candle
from .storage import MarketDataStore


@dataclass(slots=True)
class HistoryWorkerStats:
    process_id: int | None = None
    running: bool = False
    queued_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    last_job_utc: str | None = None
    last_error: str | None = None


def _serialize_candle(candle: Candle) -> dict[str, object]:
    return {
        "provider": candle.provider,
        "instrument": candle.instrument,
        "timeframe": candle.timeframe,
        "open_time": candle.open_time.isoformat(),
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
        "tick_count": candle.tick_count,
        "volume": None if candle.volume is None else str(candle.volume),
        "complete": candle.complete,
    }


def _deserialize_candle(value: dict[str, Any]) -> Candle:
    return Candle(
        provider=str(value["provider"]),
        instrument=str(value["instrument"]),
        timeframe=str(value["timeframe"]),
        open_time=datetime.fromisoformat(str(value["open_time"])),
        open=Decimal(str(value["open"])),
        high=Decimal(str(value["high"])),
        low=Decimal(str(value["low"])),
        close=Decimal(str(value["close"])),
        tick_count=int(value["tick_count"]),
        volume=None if value.get("volume") is None else Decimal(str(value["volume"])),
        complete=bool(value["complete"]),
    )


def _history_worker_main(
    database_target: str,
    command_queue: mp.Queue,
    result_queue: mp.Queue,
) -> None:
    store = MarketDataStore(database_target)
    store.initialize()
    while True:
        command = command_queue.get()
        if command is None:
            return
        job_id = str(command["job_id"])
        try:
            candles = [_deserialize_candle(item) for item in command["candles"]]
            mode = str(command.get("mode", "missing_only"))
            stored = (
                store.insert_missing_or_replace_flatline(candles)
                if mode == "replace_flatline"
                else store.insert_candles_missing(candles)
            )
            result_queue.put({
                "job_id": job_id,
                "ok": True,
                "received": len(candles),
                "stored": stored,
                "skipped": max(0, len(candles) - stored),
            })
        except BaseException as exc:
            result_queue.put({
                "job_id": job_id,
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            })


class HistoryIngestionProcess:
    """Dedicated process for MT5 historical candle writes.

    The web/live-ingestion process never performs a targeted BACKFILL write. It only
    validates/sanitizes the received bars, submits one bounded job, and waits for the
    worker acknowledgement. The worker owns its own database connection and transaction.
    """

    def __init__(self, database_target: str | Path, *, max_queue_jobs: int = 64) -> None:
        self.database_target = str(database_target)
        self._ctx = mp.get_context("spawn")
        self._commands: mp.Queue = self._ctx.Queue(maxsize=max_queue_jobs)
        self._results: mp.Queue = self._ctx.Queue()
        self._process: mp.Process | None = None
        self._listener: threading.Thread | None = None
        self._lock = threading.RLock()
        self._waiters: dict[str, tuple[threading.Event, dict[str, object]]] = {}
        self._stopping = False
        self.stats = HistoryWorkerStats()

    def start(self) -> None:
        with self._lock:
            if self._process is not None and self._process.is_alive():
                return
            self._stopping = False
            self._process = self._ctx.Process(
                target=_history_worker_main,
                args=(self.database_target, self._commands, self._results),
                name="axetos-history-ingestion",
                daemon=True,
            )
            self._process.start()
            self.stats.process_id = self._process.pid
            self.stats.running = True
            self._listener = threading.Thread(
                target=self._collect_results,
                name="history-result-listener",
                daemon=True,
            )
            self._listener.start()

    def shutdown(self) -> None:
        with self._lock:
            self._stopping = True
            process = self._process
            if process is None:
                return
            try:
                self._commands.put_nowait(None)
            except queue.Full:
                process.terminate()
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
            self.stats.running = False
            self._process = None
            for event, payload in self._waiters.values():
                payload.update({"ok": False, "error": "History worker stopped"})
                event.set()
            self._waiters.clear()

    def insert_missing(
        self, candles: list[Candle], *, timeout_seconds: float = 30.0,
        replace_flatline: bool = False,
    ) -> int:
        if not candles:
            return 0
        self.start()
        job_id = uuid.uuid4().hex
        event = threading.Event()
        payload: dict[str, object] = {}
        with self._lock:
            self._waiters[job_id] = (event, payload)
        command = {
            "job_id": job_id,
            "candles": [_serialize_candle(item) for item in candles],
            "mode": "replace_flatline" if replace_flatline else "missing_only",
        }
        try:
            self._commands.put(command, timeout=2.0)
            self.stats.queued_jobs += 1
        except queue.Full as exc:
            with self._lock:
                self._waiters.pop(job_id, None)
            raise RuntimeError("History ingestion process queue is full") from exc
        if not event.wait(timeout_seconds):
            with self._lock:
                self._waiters.pop(job_id, None)
            raise TimeoutError(
                f"History ingestion process did not acknowledge job {job_id} within {timeout_seconds:g}s"
            )
        if not bool(payload.get("ok")):
            raise RuntimeError(str(payload.get("error") or "History ingestion process failed"))
        return int(payload.get("stored", 0))

    def _collect_results(self) -> None:
        while not self._stopping:
            try:
                result = self._results.get(timeout=0.25)
            except queue.Empty:
                process = self._process
                if process is not None and not process.is_alive() and not self._stopping:
                    self.stats.running = False
                    self.stats.last_error = f"History worker exited with code {process.exitcode}"
                continue
            job_id = str(result.get("job_id", ""))
            with self._lock:
                waiter = self._waiters.pop(job_id, None)
            if waiter is None:
                continue
            event, payload = waiter
            payload.update(result)
            if bool(result.get("ok")):
                self.stats.completed_jobs += 1
                self.stats.last_error = None
            else:
                self.stats.failed_jobs += 1
                self.stats.last_error = str(result.get("error"))
            self.stats.last_job_utc = datetime.now().astimezone().isoformat()
            event.set()

    def view(self) -> dict[str, object]:
        process = self._process
        self.stats.running = bool(process is not None and process.is_alive())
        self.stats.process_id = process.pid if process is not None else None
        return {name: getattr(self.stats, name) for name in self.stats.__slots__}
