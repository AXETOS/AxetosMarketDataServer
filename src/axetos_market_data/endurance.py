from __future__ import annotations

import json
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from .benchmarks import IngestionBenchmark


@dataclass(frozen=True, slots=True)
class EnduranceResult:
    duration_seconds: float
    cycles_completed: int
    ticks_per_cycle: int
    instruments: int
    batch_size: int
    total_ticks_written: int
    average_ticks_per_second: float
    median_ticks_per_second: float
    minimum_ticks_per_second: float
    maximum_ticks_per_second: float
    throughput_drift_percent: float
    maximum_peak_memory_bytes: int
    regression_threshold_percent: float
    regression_detected: bool
    started_utc: str
    completed_utc: str
    cycles: list[dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class EnduranceRunner:
    """Repeated isolated benchmark cycles for sustained-performance regression checks."""

    def __init__(
        self,
        *,
        duration_seconds: float = 300.0,
        ticks_per_cycle: int = 100_000,
        instruments: int = 10,
        batch_size: int = 5_000,
        regression_threshold_percent: float = 20.0,
    ) -> None:
        if duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        if ticks_per_cycle <= 0:
            raise ValueError("ticks_per_cycle must be positive")
        if regression_threshold_percent < 0:
            raise ValueError("regression_threshold_percent cannot be negative")
        self.duration_seconds = duration_seconds
        self.ticks_per_cycle = ticks_per_cycle
        self.instruments = instruments
        self.batch_size = batch_size
        self.regression_threshold_percent = regression_threshold_percent

    def run(self) -> EnduranceResult:
        started = datetime.now(UTC)
        deadline = time.monotonic() + self.duration_seconds
        cycles: list[dict[str, object]] = []
        while not cycles or time.monotonic() < deadline:
            result = IngestionBenchmark(
                ticks=self.ticks_per_cycle,
                instruments=self.instruments,
                batch_size=self.batch_size,
            ).run()
            cycles.append(result.to_dict())

        rates = [float(cycle["ticks_per_second"]) for cycle in cycles]
        first = rates[0]
        last = rates[-1]
        drift = ((last - first) / first * 100.0) if first else 0.0
        regression = drift < -self.regression_threshold_percent
        completed = datetime.now(UTC)
        return EnduranceResult(
            duration_seconds=round((completed - started).total_seconds(), 6),
            cycles_completed=len(cycles),
            ticks_per_cycle=self.ticks_per_cycle,
            instruments=self.instruments,
            batch_size=self.batch_size,
            total_ticks_written=sum(int(cycle["ticks_written"]) for cycle in cycles),
            average_ticks_per_second=round(statistics.fmean(rates), 2),
            median_ticks_per_second=round(statistics.median(rates), 2),
            minimum_ticks_per_second=round(min(rates), 2),
            maximum_ticks_per_second=round(max(rates), 2),
            throughput_drift_percent=round(drift, 2),
            maximum_peak_memory_bytes=max(int(cycle["peak_memory_bytes"]) for cycle in cycles),
            regression_threshold_percent=self.regression_threshold_percent,
            regression_detected=regression,
            started_utc=started.isoformat(),
            completed_utc=completed.isoformat(),
            cycles=cycles,
        )


def write_endurance_result(result: EnduranceResult, output: str | None) -> None:
    payload = json.dumps(result.to_dict(), indent=2)
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload + "\n", encoding="utf-8")
    print(payload)
