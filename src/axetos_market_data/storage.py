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

CREATE TABLE IF NOT EXISTS ingestion_state (
 provider TEXT NOT NULL, instrument TEXT NOT NULL, timeframe TEXT NOT NULL,
 requested_from_utc TEXT NOT NULL, requested_to_utc TEXT NOT NULL,
 received INTEGER NOT NULL, written INTEGER NOT NULL, invalid INTEGER NOT NULL,
 status TEXT NOT NULL, error TEXT NULL, updated_utc TEXT NOT NULL,
 PRIMARY KEY(provider, instrument, timeframe)
);

CREATE TABLE IF NOT EXISTS data_gaps (
 id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, instrument TEXT NOT NULL,
 timeframe TEXT NOT NULL, gap_from_utc TEXT NOT NULL, gap_to_utc TEXT NOT NULL,
 detected_utc TEXT NOT NULL, resolved INTEGER NOT NULL DEFAULT 0,
 UNIQUE(provider, instrument, timeframe, gap_from_utc)
);
CREATE INDEX IF NOT EXISTS ix_gaps_lookup ON data_gaps(resolved, provider, instrument, timeframe);

CREATE TABLE IF NOT EXISTS repair_runs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 provider TEXT NOT NULL,
 instrument TEXT NULL,
 timeframe TEXT NULL,
 gaps_selected INTEGER NOT NULL,
 windows_requested INTEGER NOT NULL,
 candles_received INTEGER NOT NULL,
 candles_written INTEGER NOT NULL,
 invalid_candles INTEGER NOT NULL,
 gaps_resolved INTEGER NOT NULL,
 gaps_remaining INTEGER NOT NULL,
 created_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_repair_runs_provider ON repair_runs(provider, created_utc);
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


    def upsert_candles(self, candles: Iterable[Candle]) -> int:
        count = 0
        for candle in candles:
            self.upsert_candle(candle)
            count += 1
        return count

    def read_candle_times(self, provider: str, instrument: str, timeframe: str, start: datetime, end: datetime) -> list[datetime]:
        with self.connect() as connection:
            rows = connection.execute("SELECT open_time_utc FROM candles WHERE provider=? AND instrument=? AND timeframe=? AND open_time_utc>=? AND open_time_utc<?", (provider, instrument, timeframe, _iso(start), _iso(end))).fetchall()
        return [datetime.fromisoformat(row[0]) for row in rows]

    def set_ingestion_state(self, provider: str, instrument: str, timeframe: str, start: datetime, end: datetime, received: int, written: int, invalid: int, status: str, error: str | None) -> None:
        with self.connect() as connection:
            connection.execute("""INSERT INTO ingestion_state VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(provider,instrument,timeframe) DO UPDATE SET requested_from_utc=excluded.requested_from_utc,requested_to_utc=excluded.requested_to_utc,received=excluded.received,written=excluded.written,invalid=excluded.invalid,status=excluded.status,error=excluded.error,updated_utc=excluded.updated_utc""", (provider,instrument,timeframe,_iso(start),_iso(end),received,written,invalid,status,error,_iso(datetime.now().astimezone())))

    def list_ingestion_state(self) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows=connection.execute("SELECT * FROM ingestion_state ORDER BY updated_utc DESC").fetchall()
        return [dict(row) for row in rows]

    def clear_gaps(self, provider: str, instrument: str, timeframe: str, start: datetime, end: datetime) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM data_gaps WHERE provider=? AND instrument=? AND timeframe=? AND gap_from_utc>=? AND gap_from_utc<?", (provider,instrument,timeframe,_iso(start),_iso(end)))

    def record_gap(self, provider: str, instrument: str, timeframe: str, start: datetime, end: datetime) -> None:
        with self.connect() as connection:
            connection.execute("INSERT OR IGNORE INTO data_gaps(provider,instrument,timeframe,gap_from_utc,gap_to_utc,detected_utc,resolved) VALUES(?,?,?,?,?,?,0)", (provider,instrument,timeframe,_iso(start),_iso(end),_iso(datetime.now().astimezone())))

    def list_gaps(
        self,
        limit: int = 500,
        provider: str | None = None,
        instrument: str | None = None,
        timeframe: str | None = None,
    ) -> list[dict[str, object]]:
        where = ["resolved=0"]
        parameters: list[object] = []
        for column, value in (("provider", provider), ("instrument", instrument), ("timeframe", timeframe)):
            if value is not None:
                where.append(f"{column}=?")
                parameters.append(value)
        parameters.append(limit)
        with self.connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM data_gaps WHERE {' AND '.join(where)} ORDER BY gap_from_utc LIMIT ?",
                parameters,
            ).fetchall()
        return [dict(row) for row in rows]

    def count_gaps(
        self,
        provider: str | None = None,
        instrument: str | None = None,
        timeframe: str | None = None,
    ) -> int:
        where = ["resolved=0"]
        parameters: list[object] = []
        for column, value in (("provider", provider), ("instrument", instrument), ("timeframe", timeframe)):
            if value is not None:
                where.append(f"{column}=?")
                parameters.append(value)
        with self.connect() as connection:
            return int(connection.execute(
                f"SELECT COUNT(*) FROM data_gaps WHERE {' AND '.join(where)}", parameters
            ).fetchone()[0])

    def mark_gap_resolved(self, gap_id: int) -> None:
        with self.connect() as connection:
            connection.execute("UPDATE data_gaps SET resolved=1 WHERE id=?", (gap_id,))

    def candle_exists(self, provider: str, instrument: str, timeframe: str, open_time: datetime) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM candles WHERE provider=? AND instrument=? AND timeframe=? AND open_time_utc=? LIMIT 1",
                (provider, instrument, timeframe, _iso(open_time)),
            ).fetchone()
        return row is not None

    def record_repair_run(self, result: object) -> None:
        with self.connect() as connection:
            connection.execute(
                """INSERT INTO repair_runs(
                    provider,instrument,timeframe,gaps_selected,windows_requested,
                    candles_received,candles_written,invalid_candles,gaps_resolved,
                    gaps_remaining,created_utc
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    getattr(result, "provider"), getattr(result, "instrument"), getattr(result, "timeframe"),
                    getattr(result, "gaps_selected"), getattr(result, "windows_requested"),
                    getattr(result, "candles_received"), getattr(result, "candles_written"),
                    getattr(result, "invalid_candles"), getattr(result, "gaps_resolved"),
                    getattr(result, "gaps_remaining"), _iso(datetime.now().astimezone()),
                ),
            )

    def list_repair_runs(self, limit: int = 100) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM repair_runs ORDER BY created_utc DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

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

    def statistics(self) -> dict[str, object]:
        with self.connect() as connection:
            ticks = int(connection.execute("SELECT COUNT(*) FROM ticks").fetchone()[0])
            candles = int(connection.execute("SELECT COUNT(*) FROM candles").fetchone()[0])
            instruments = int(connection.execute("SELECT COUNT(DISTINCT instrument) FROM candles").fetchone()[0])
            latest_tick = connection.execute("SELECT MAX(timestamp_utc) FROM ticks").fetchone()[0]
            latest_candle = connection.execute("SELECT MAX(open_time_utc) FROM candles").fetchone()[0]
            gaps = int(connection.execute("SELECT COUNT(*) FROM data_gaps WHERE resolved=0").fetchone()[0])
        return {
            "ticks": ticks,
            "candles": candles,
            "instruments": instruments,
            "latest_tick_utc": latest_tick,
            "latest_candle_utc": latest_candle,
            "database_path": str(self.database_path),
            "unresolved_gaps": gaps,
        }

    def list_instruments(self) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT instrument FROM candles ORDER BY instrument"
            ).fetchall()
        return [str(row[0]) for row in rows]
