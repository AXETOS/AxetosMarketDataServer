from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator

from .domain import Candle, Tick


_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    instrument TEXT NOT NULL,
    timestamp_utc TEXT NOT NULL,
    bid TEXT NOT NULL,
    ask TEXT NOT NULL,
    volume TEXT NULL,
    UNIQUE(provider, instrument, timestamp_utc, bid, ask)
);

CREATE INDEX IF NOT EXISTS ix_ticks_lookup
ON ticks(instrument, timestamp_utc);

CREATE TABLE IF NOT EXISTS candles (
    provider TEXT NOT NULL,
    instrument TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open_time_utc TEXT NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    tick_count INTEGER NOT NULL,
    volume TEXT NULL,
    complete INTEGER NOT NULL,
    PRIMARY KEY(provider, instrument, timeframe, open_time_utc)
);

CREATE INDEX IF NOT EXISTS ix_candles_lookup
ON candles(instrument, timeframe, open_time_utc);
"""


def _iso(value: datetime) -> str:
    return value.isoformat(timespec="microseconds")


class MarketDataStore:
    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database_path, timeout=30)
        try:
            connection.row_factory = sqlite3.Row
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(_SCHEMA)

    def insert_ticks(self, ticks: Iterable[Tick]) -> int:
        rows = [
            (
                tick.provider,
                tick.instrument,
                _iso(tick.timestamp),
                str(tick.bid),
                str(tick.ask),
                None if tick.volume is None else str(tick.volume),
            )
            for tick in ticks
        ]
        if not rows:
            return 0
        with self.connect() as connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO ticks(provider, instrument, timestamp_utc, bid, ask, volume)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return connection.total_changes - before

    def upsert_candle(self, candle: Candle) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO candles(
                    provider, instrument, timeframe, open_time_utc,
                    open, high, low, close, tick_count, volume, complete
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, instrument, timeframe, open_time_utc)
                DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    tick_count=excluded.tick_count,
                    volume=excluded.volume,
                    complete=excluded.complete
                """,
                (
                    candle.provider,
                    candle.instrument,
                    candle.timeframe,
                    _iso(candle.open_time),
                    str(candle.open),
                    str(candle.high),
                    str(candle.low),
                    str(candle.close),
                    candle.tick_count,
                    None if candle.volume is None else str(candle.volume),
                    int(candle.complete),
                ),
            )

    def read_candles(
        self,
        instrument: str,
        timeframe: str,
        limit: int = 500,
        provider: str | None = None,
    ) -> list[Candle]:
        where = "instrument = ? AND timeframe = ?"
        parameters: list[object] = [instrument, timeframe]
        if provider is not None:
            where += " AND provider = ?"
            parameters.append(provider)
        parameters.append(limit)
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM candles
                WHERE {where}
                ORDER BY open_time_utc DESC
                LIMIT ?
                """,
                parameters,
            ).fetchall()
        return [
            Candle(
                provider=row["provider"],
                instrument=row["instrument"],
                timeframe=row["timeframe"],
                open_time=datetime.fromisoformat(row["open_time_utc"]),
                open=Decimal(row["open"]),
                high=Decimal(row["high"]),
                low=Decimal(row["low"]),
                close=Decimal(row["close"]),
                tick_count=int(row["tick_count"]),
                volume=None if row["volume"] is None else Decimal(row["volume"]),
                complete=bool(row["complete"]),
            )
            for row in reversed(rows)
        ]
