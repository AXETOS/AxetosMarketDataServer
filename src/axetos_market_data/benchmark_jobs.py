from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Iterable

from .benchmarks import IngestionBenchmark


@dataclass(slots=True)
class BenchmarkJob:
    job_id: str
    ticks: int
    instruments: int
    batch_sizes: list[int]
    status: str = "queued"
    created_utc: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    started_utc: str | None = None
    completed_utc: str | None = None
    current_batch_size: int | None = None
    completed_runs: int = 0
    results: list[dict[str, object]] = field(default_factory=list)
    best_batch_size: int | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "ticks": self.ticks,
            "instruments": self.instruments,
            "batch_sizes": list(self.batch_sizes),
            "status": self.status,
            "created_utc": self.created_utc,
            "started_utc": self.started_utc,
            "completed_utc": self.completed_utc,
            "current_batch_size": self.current_batch_size,
            "completed_runs": self.completed_runs,
            "total_runs": len(self.batch_sizes),
            "results": list(self.results),
            "best_batch_size": self.best_batch_size,
            "error": self.error,
        }


class BenchmarkJobManager:
    """Runs one isolated synthetic benchmark job at a time."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._job: BenchmarkJob | None = None
        self._completion = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, ticks: int, instruments: int, batch_sizes: Iterable[int]) -> dict[str, object]:
        sizes = list(dict.fromkeys(int(value) for value in batch_sizes))
        if not sizes:
            raise ValueError("At least one batch size is required")
        with self._lock:
            if self._job is not None and self._job.status in {"queued", "running"}:
                raise RuntimeError("A benchmark is already running")
            job = BenchmarkJob(uuid.uuid4().hex, ticks, instruments, sizes)
            self._job = job
            self._completion.clear()
            thread = threading.Thread(target=self._run, args=(job,), name="axetos-benchmark", daemon=True)
            self._thread = thread
            thread.start()
            return job.to_dict()

    def status(self) -> dict[str, object]:
        with self._lock:
            if self._job is None:
                return {"status": "idle", "job": None}
            return {"status": self._job.status, "job": self._job.to_dict()}

    def wait_for_completion(self, timeout: float | None = None) -> dict[str, object]:
        """Wait for the active benchmark to reach a terminal state.

        This avoids timing-sensitive polling in callers and release tests while
        leaving the HTTP API asynchronous.
        """
        with self._lock:
            if self._job is None:
                return {"status": "idle", "job": None}
            completion = self._completion
        completion.wait(timeout)
        return self.status()

    def _run(self, job: BenchmarkJob) -> None:
        with self._lock:
            job.status = "running"
            job.started_utc = datetime.now(UTC).isoformat()
        try:
            for batch_size in job.batch_sizes:
                with self._lock:
                    job.current_batch_size = batch_size
                result = IngestionBenchmark(
                    ticks=job.ticks,
                    instruments=job.instruments,
                    batch_size=batch_size,
                ).run().to_dict()
                with self._lock:
                    job.results.append(result)
                    job.completed_runs += 1
            with self._lock:
                best = max(job.results, key=lambda item: float(item["ticks_per_second"]))
                job.best_batch_size = int(best["batch_size"])
                job.status = "completed"
        except Exception as exc:  # pragma: no cover - defensive background boundary
            with self._lock:
                job.status = "failed"
                job.error = str(exc)
        finally:
            with self._lock:
                job.current_batch_size = None
                job.completed_utc = datetime.now(UTC).isoformat()
                self._completion.set()
