from __future__ import annotations

import multiprocessing as mp
import os
import queue
import signal
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .hierarchical_repair import HierarchicalCandleRepair
from .storage import MarketDataStore


@dataclass(slots=True)
class RepairWorkerStats:
    process_id: int | None = None
    running: bool = False
    queued_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    last_job_utc: str | None = None
    last_error: str | None = None


def _repair_worker_main(database_target: str, commands: mp.Queue, results: mp.Queue) -> None:
    # Worker children must not receive the console Ctrl+C intended for Uvicorn.
    # The parent sends an explicit queue sentinel during orderly shutdown.
    if hasattr(signal, "SIGINT"):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    store = MarketDataStore(database_target)
    store.initialize()
    repair = HierarchicalCandleRepair(store)
    while True:
        try:
            command = commands.get()
        except (EOFError, OSError):
            return
        if command is None:
            return
        job_id = str(command["job_id"])
        provider = str(command["provider"])
        instruments = [str(value) for value in command["instruments"]]
        stages: list[dict[str, object]] = []
        try:
            def on_stage(category: str, instrument: str, timeframe: str, details: dict[str, object]) -> None:
                stages.append({
                    "category": category,
                    "instrument": instrument,
                    "timeframe": timeframe,
                    "details": details,
                })
            summary = repair.run(provider, instruments, on_stage=on_stage)
            results.put({
                "job_id": job_id,
                "ok": True,
                "summary": summary,
                "stages": stages,
                "worker_pid": os.getpid(),
            })
        except BaseException as exc:
            results.put({
                "job_id": job_id,
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "stages": stages,
                "worker_pid": os.getpid(),
            })


class CandleRepairProcess:
    """Dedicated OS process for bottom-up candle aggregation and repair."""

    def __init__(self, database_target: str | Path, *, max_queue_jobs: int = 16) -> None:
        self.database_target = str(database_target)
        self._ctx = mp.get_context("spawn")
        self._commands: mp.Queue = self._ctx.Queue(maxsize=max_queue_jobs)
        self._results: mp.Queue = self._ctx.Queue()
        self._process: mp.Process | None = None
        self._listener: threading.Thread | None = None
        self._lock = threading.RLock()
        self._waiters: dict[str, tuple[threading.Event, dict[str, object]]] = {}
        self._stopping = False
        self.stats = RepairWorkerStats()

    def start(self) -> None:
        with self._lock:
            if self._process is not None and self._process.is_alive():
                return
            self._stopping = False
            self._process = self._ctx.Process(
                target=_repair_worker_main,
                args=(self.database_target, self._commands, self._results),
                name="axetos-candle-repair",
                daemon=True,
            )
            self._process.start()
            self.stats.process_id = self._process.pid
            self.stats.running = True
            self._listener = threading.Thread(target=self._collect_results, name="repair-result-listener", daemon=True)
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
            listener = self._listener
            if listener is not None and listener.is_alive():
                listener.join(timeout=1)
            self._listener = None
            for worker_queue in (self._commands, self._results):
                try:
                    worker_queue.close()
                    worker_queue.join_thread()
                except (OSError, ValueError):
                    pass
            self.stats.running = False
            self.stats.process_id = None
            self._process = None
            for event, payload in self._waiters.values():
                payload.update({"ok": False, "error": "Repair worker stopped"})
                event.set()
            self._waiters.clear()

    def run(self, provider: str, instruments: list[str], *, timeout_seconds: float = 1800.0) -> dict[str, object]:
        if not instruments:
            return {"summary": {}, "stages": [], "worker_pid": self.stats.process_id}
        self.start()
        job_id = uuid.uuid4().hex
        event = threading.Event()
        payload: dict[str, object] = {}
        with self._lock:
            self._waiters[job_id] = (event, payload)
        try:
            self._commands.put({"job_id": job_id, "provider": provider, "instruments": instruments}, timeout=2.0)
            self.stats.queued_jobs += 1
        except queue.Full as exc:
            with self._lock:
                self._waiters.pop(job_id, None)
            raise RuntimeError("Candle repair process queue is full") from exc
        if not event.wait(timeout_seconds):
            with self._lock:
                self._waiters.pop(job_id, None)
            raise TimeoutError(f"Candle repair process did not finish job {job_id} within {timeout_seconds:g}s")
        if not bool(payload.get("ok")):
            raise RuntimeError(str(payload.get("error") or "Candle repair process failed"))
        return payload

    def _collect_results(self) -> None:
        while not self._stopping:
            try:
                result = self._results.get(timeout=0.25)
            except queue.Empty:
                process = self._process
                if process is not None and not process.is_alive() and not self._stopping:
                    self.stats.running = False
                    self.stats.last_error = f"Repair worker exited with code {process.exitcode}"
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
