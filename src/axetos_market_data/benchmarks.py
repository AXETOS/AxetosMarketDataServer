from __future__ import annotations

import json
import math
import os
import tempfile
import time
import tracemalloc
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from .candle_builder import CandleBuilder
from .domain import Tick
from .storage import MarketDataStore


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    backend: str
    ticks_requested: int
    ticks_written: int
    instruments: int
    batch_size: int
    elapsed_seconds: float
    ticks_per_second: float
    candles_written: int
    peak_memory_bytes: int
    database_bytes: int
    started_utc: str
    completed_utc: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class IngestionBenchmark:
    """Deterministic ingestion benchmark for repeatable release comparisons."""

    def __init__(
        self,
        *,
        ticks: int = 100_000,
        instruments: int = 10,
        batch_size: int = 1_000,
        database: str | None = None,
    ) -> None:
        if ticks <= 0:
            raise ValueError("ticks must be positive")
        if instruments <= 0:
            raise ValueError("instruments must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self.ticks = ticks
        self.instruments = instruments
        self.batch_size = batch_size
        self.database = database

    def run(self) -> BenchmarkResult:
        temporary: tempfile.TemporaryDirectory[str] | None = None
        database = self.database
        if database is None:
            temporary = tempfile.TemporaryDirectory(prefix="axetos-benchmark-")
            database = str(Path(temporary.name) / "benchmark.sqlite")

        started = datetime.now(UTC)
        tracemalloc.start()
        start = time.perf_counter()
        try:
            store = MarketDataStore(database)
            store.initialize()
            builder = CandleBuilder(store)
            written = 0
            batch: list[Tick] = []
            for tick in self._generate_ticks():
                batch.append(tick)
                builder.ingest(tick)
                if len(batch) >= self.batch_size:
                    written += store.insert_ticks(batch)
                    batch.clear()
            if batch:
                written += store.insert_ticks(batch)
            builder.flush(complete=True)
            stats = store.statistics()
            elapsed = time.perf_counter() - start
            _, peak_memory = tracemalloc.get_traced_memory()
            completed = datetime.now(UTC)
            database_bytes = self._database_size(database)
            return BenchmarkResult(
                backend=store.backend.kind,
                ticks_requested=self.ticks,
                ticks_written=written,
                instruments=self.instruments,
                batch_size=self.batch_size,
                elapsed_seconds=round(elapsed, 6),
                ticks_per_second=round(written / elapsed if elapsed else math.inf, 2),
                candles_written=int(stats["candles"]),
                peak_memory_bytes=peak_memory,
                database_bytes=database_bytes,
                started_utc=started.isoformat(),
                completed_utc=completed.isoformat(),
            )
        finally:
            tracemalloc.stop()
            if temporary is not None:
                temporary.cleanup()

    def _generate_ticks(self):
        started = datetime(2020, 1, 1, tzinfo=UTC)
        base = Decimal("1.10000")
        spread = Decimal("0.00012")
        for index in range(self.ticks):
            instrument_index = index % self.instruments
            instrument = f"BENCH/{instrument_index:03d}"
            second = index // self.instruments
            timestamp = started + timedelta(milliseconds=250 * second)
            movement = Decimal((index % 41) - 20) * Decimal("0.00001")
            midpoint = base + movement + Decimal(instrument_index) * Decimal("0.001")
            half_spread = spread / Decimal("2")
            yield Tick(
                provider="Benchmark",
                instrument=instrument,
                timestamp=timestamp,
                bid=midpoint - half_spread,
                ask=midpoint + half_spread,
                volume=Decimal("1"),
            )

    @staticmethod
    def _database_size(database: str) -> int:
        if database.startswith("postgresql://") or database.startswith("postgres://"):
            return 0
        return os.path.getsize(database) if os.path.exists(database) else 0


def write_benchmark_result(result: BenchmarkResult, output: str | None) -> None:
    payload = json.dumps(result.to_dict(), indent=2)
    if output is None:
        print(payload)
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload + "\n", encoding="utf-8")
    print(payload)
