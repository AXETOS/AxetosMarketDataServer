from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterator

from .domain import Candle, Tick
from .database import DatabaseBackend, ConnectionLike


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

CREATE TABLE IF NOT EXISTS cleanup_runs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 ticks_before_utc TEXT NOT NULL,
 operational_before_utc TEXT NOT NULL,
 ticks_deleted INTEGER NOT NULL,
 resolved_gaps_deleted INTEGER NOT NULL,
 ingestion_states_deleted INTEGER NOT NULL,
 repair_runs_deleted INTEGER NOT NULL,
 database_bytes_before INTEGER NOT NULL,
 database_bytes_after INTEGER NOT NULL,
 integrity_status TEXT NOT NULL,
 vacuumed INTEGER NOT NULL,
 created_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_cleanup_runs_created ON cleanup_runs(created_utc);

CREATE TABLE IF NOT EXISTS maintenance_schedules (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 name TEXT NOT NULL UNIQUE,
 task_type TEXT NOT NULL,
 enabled INTEGER NOT NULL DEFAULT 1,
 interval_minutes INTEGER NOT NULL,
 tick_days INTEGER NOT NULL DEFAULT 30,
 operational_days INTEGER NOT NULL DEFAULT 90,
 vacuum INTEGER NOT NULL DEFAULT 0,
 last_run_utc TEXT NULL,
 next_run_utc TEXT NOT NULL,
 last_status TEXT NULL,
 last_error TEXT NULL,
 updated_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_maintenance_schedules_due
ON maintenance_schedules(enabled, next_run_utc);

CREATE TABLE IF NOT EXISTS maintenance_schedule_runs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 schedule_id INTEGER NOT NULL,
 schedule_name TEXT NOT NULL,
 task_type TEXT NOT NULL,
 status TEXT NOT NULL,
 started_utc TEXT NOT NULL,
 completed_utc TEXT NULL,
 result_json TEXT NOT NULL DEFAULT '{}',
 error TEXT NULL,
 FOREIGN KEY(schedule_id) REFERENCES maintenance_schedules(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_maintenance_schedule_runs_created
ON maintenance_schedule_runs(started_utc DESC);


CREATE TABLE IF NOT EXISTS symbol_policies (
 provider_key TEXT NOT NULL,
 provider_symbol TEXT NOT NULL,
 canonical_instrument TEXT NOT NULL,
 enabled INTEGER NOT NULL DEFAULT 1,
 allow_live INTEGER NOT NULL DEFAULT 1,
 allow_history INTEGER NOT NULL DEFAULT 1,
 priority_override INTEGER NULL,
 updated_utc TEXT NOT NULL,
 PRIMARY KEY(provider_key, provider_symbol)
);
CREATE INDEX IF NOT EXISTS ix_symbol_policies_canonical
ON symbol_policies(canonical_instrument, enabled);


CREATE TABLE IF NOT EXISTS candle_quality_issues (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 provider TEXT NOT NULL,
 instrument TEXT NOT NULL,
 timeframe TEXT NOT NULL,
 open_time_utc TEXT NOT NULL,
 reason TEXT NOT NULL,
 severity TEXT NOT NULL,
 action TEXT NOT NULL DEFAULT 'detected',
 detected_utc TEXT NOT NULL,
 resolved_utc TEXT NULL,
 UNIQUE(provider, instrument, timeframe, open_time_utc, reason)
);
CREATE INDEX IF NOT EXISTS ix_candle_quality_issues
ON candle_quality_issues(action, provider, instrument, timeframe, open_time_utc);

CREATE TABLE IF NOT EXISTS quarantined_candles (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
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
 reason TEXT NOT NULL,
 quarantined_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_quarantined_candles_lookup
ON quarantined_candles(provider, instrument, timeframe, open_time_utc);

CREATE TABLE IF NOT EXISTS operational_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 severity TEXT NOT NULL,
 category TEXT NOT NULL,
 provider TEXT NULL,
 instrument TEXT NULL,
 message TEXT NOT NULL,
 timestamp_utc TEXT NOT NULL,
 details_json TEXT NOT NULL DEFAULT '{}',
 CHECK(severity IN ('debug','info','warning','error','critical'))
);
CREATE INDEX IF NOT EXISTS ix_operational_events_timestamp ON operational_events(timestamp_utc DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_operational_events_filter ON operational_events(category, severity, provider, instrument, timestamp_utc DESC);

CREATE TABLE IF NOT EXISTS mt5_bridge_heartbeats (
 provider_key TEXT NOT NULL, terminal_instance_id TEXT NOT NULL, broker_name TEXT NULL,
 server_name TEXT NULL, account_login INTEGER NULL, source_time_utc TEXT NOT NULL,
 received_utc TEXT NOT NULL, PRIMARY KEY(provider_key, terminal_instance_id)
);
CREATE TABLE IF NOT EXISTS mt5_bridge_instruments (
 provider_key TEXT NOT NULL, terminal_instance_id TEXT NOT NULL, provider_symbol TEXT NOT NULL,
 canonical_instrument TEXT NOT NULL, digits INTEGER NOT NULL, point TEXT NOT NULL,
 is_visible INTEGER NOT NULL, display_name TEXT NULL, description TEXT NULL, path TEXT NULL,
 asset_class TEXT NULL, is_selected INTEGER NOT NULL, observed_utc TEXT NOT NULL,
 PRIMARY KEY(provider_key, terminal_instance_id, provider_symbol)
);
CREATE INDEX IF NOT EXISTS ix_bridge_instruments_canonical ON mt5_bridge_instruments(canonical_instrument);
CREATE TABLE IF NOT EXISTS mt5_bridge_quotes (
 provider_key TEXT NOT NULL, terminal_instance_id TEXT NOT NULL, provider_symbol TEXT NOT NULL,
 canonical_instrument TEXT NOT NULL, source_time_utc TEXT NOT NULL, received_utc TEXT NOT NULL,
 bid TEXT NOT NULL, ask TEXT NOT NULL, last TEXT NULL, volume TEXT NULL,
 PRIMARY KEY(provider_key, terminal_instance_id, provider_symbol)
);
"""



def _postgres_schema() -> str:
    schema = _SCHEMA
    schema = "\n".join(line for line in schema.splitlines() if not line.strip().upper().startswith("PRAGMA "))
    schema = schema.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
    return schema

def _iso(value: datetime) -> str:
    return value.isoformat(timespec="microseconds")


class MarketDataStore:
    def __init__(self, database_path: str | Path) -> None:
        self.database_target = str(database_path)
        self.backend = DatabaseBackend(database_path)
        self.database_path = self.backend.path
        self._tick_publisher: Callable[[Tick], None] | None = None
        self._candle_publisher: Callable[[Candle], None] | None = None

    def set_live_publishers(
        self,
        tick_publisher: Callable[[Tick], None] | None,
        candle_publisher: Callable[[Candle], None] | None,
    ) -> None:
        self._tick_publisher = tick_publisher
        self._candle_publisher = candle_publisher

    @contextmanager
    def connect(self) -> Iterator[ConnectionLike]:
        with self.backend.connect() as connection:
            yield connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(_postgres_schema() if self.backend.kind == "postgresql" else _SCHEMA)


    def record_operational_event(self, severity: str, category: str, provider: str | None,
                                 instrument: str | None, message: str, timestamp: datetime,
                                 details_json: str = "{}") -> int:
        with self.connect() as connection:
            query = """INSERT INTO operational_events(severity,category,provider,instrument,message,timestamp_utc,details_json)
                VALUES(?,?,?,?,?,?,?)"""
            if self.backend.kind == "postgresql":
                query += " RETURNING id"
            cursor = connection.execute(
                query,
                (severity, category, provider, instrument, message, _iso(timestamp), details_json),
            )
            if self.backend.kind == "postgresql":
                return int(cursor.fetchone()[0])
            return int(cursor.lastrowid)

    def list_operational_events(self, *, page: int = 1, page_size: int = 50,
                                severity: str | None = None, category: str | None = None,
                                provider: str | None = None, instrument: str | None = None,
                                search: str | None = None, from_utc: datetime | None = None,
                                to_utc: datetime | None = None) -> dict[str, object]:
        import json
        page = max(1, int(page)); page_size = max(1, min(500, int(page_size)))
        where = ["1=1"]; args: list[object] = []
        for column, value in (("severity", severity), ("category", category),
                              ("provider", provider), ("instrument", instrument)):
            if value:
                where.append(f"{column}=?"); args.append(value)
        if search:
            where.append("(message LIKE ? OR details_json LIKE ?)")
            token = f"%{search}%"; args.extend((token, token))
        if from_utc:
            where.append("timestamp_utc>=?"); args.append(_iso(from_utc))
        if to_utc:
            where.append("timestamp_utc<=?"); args.append(_iso(to_utc))
        clause = " AND ".join(where)
        with self.connect() as connection:
            total = int(connection.execute(f"SELECT COUNT(*) FROM operational_events WHERE {clause}", args).fetchone()[0])
            rows = connection.execute(
                f"SELECT * FROM operational_events WHERE {clause} ORDER BY timestamp_utc DESC,id DESC LIMIT ? OFFSET ?",
                [*args, page_size, (page-1)*page_size],
            ).fetchall()
        items=[]
        for row in rows:
            item=dict(row)
            try: item["details"] = json.loads(str(item.pop("details_json")))
            except json.JSONDecodeError: item["details"] = {"raw": item.pop("details_json", "")}
            items.append(item)
        return {"page": page, "page_size": page_size, "total": total,
                "pages": (total + page_size - 1) // page_size, "items": items}

    def export_operational_events(self, *, severity: str | None = None,
                                  category: str | None = None, provider: str | None = None,
                                  instrument: str | None = None, search: str | None = None,
                                  from_utc: datetime | None = None, to_utc: datetime | None = None,
                                  limit: int = 50000) -> list[dict[str, object]]:
        import json
        limit = max(1, min(100000, int(limit)))
        where = ["1=1"]
        args: list[object] = []
        for column, value in (("severity", severity), ("category", category),
                              ("provider", provider), ("instrument", instrument)):
            if value:
                where.append(f"{column}=?")
                args.append(value)
        if search:
            where.append("(message LIKE ? OR details_json LIKE ?)")
            token = f"%{search}%"
            args.extend((token, token))
        if from_utc:
            where.append("timestamp_utc>=?")
            args.append(_iso(from_utc))
        if to_utc:
            where.append("timestamp_utc<=?")
            args.append(_iso(to_utc))
        clause = " AND ".join(where)
        with self.connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM operational_events WHERE {clause} "
                "ORDER BY timestamp_utc DESC,id DESC LIMIT ?",
                [*args, limit],
            ).fetchall()
        items: list[dict[str, object]] = []
        for row in rows:
            item = dict(row)
            raw_details = str(item.pop("details_json"))
            try:
                item["details"] = json.loads(raw_details)
            except json.JSONDecodeError:
                item["details"] = {"raw": raw_details}
            items.append(item)
        return items

    def delete_operational_events_before(self, cutoff: datetime) -> int:
        with self.connect() as connection:
            before = connection.total_changes
            connection.execute("DELETE FROM operational_events WHERE timestamp_utc<?", (_iso(cutoff),))
            return connection.total_changes - before

    def insert_ticks(self, ticks: Iterable[Tick]) -> int:
        tick_list = list(ticks)
        rows = [
            (
                tick.provider,
                tick.instrument,
                _iso(tick.timestamp),
                str(tick.bid),
                str(tick.ask),
                None if tick.volume is None else str(tick.volume),
            )
            for tick in tick_list
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
            written = connection.total_changes - before
        if self._tick_publisher is not None:
            for tick in tick_list:
                self._tick_publisher(tick)
        return written

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
        if self._candle_publisher is not None:
            self._candle_publisher(candle)


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

    def upsert_maintenance_schedule(self, name: str, task_type: str, enabled: bool, interval_minutes: int,
                                    next_run_utc: datetime, tick_days: int = 30, operational_days: int = 90,
                                    vacuum: bool = False) -> dict[str, object]:
        now = datetime.now(next_run_utc.tzinfo).isoformat()
        with self.connect() as connection:
            connection.execute(
                """INSERT INTO maintenance_schedules
                (name, task_type, enabled, interval_minutes, tick_days, operational_days, vacuum, next_run_utc, updated_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET task_type=excluded.task_type, enabled=excluded.enabled,
                interval_minutes=excluded.interval_minutes, tick_days=excluded.tick_days,
                operational_days=excluded.operational_days, vacuum=excluded.vacuum,
                next_run_utc=excluded.next_run_utc, updated_utc=excluded.updated_utc""",
                (name, task_type, int(enabled), interval_minutes, tick_days, operational_days, int(vacuum),
                 _iso(next_run_utc), now),
            )
        return self.get_maintenance_schedule(name) or {}

    def get_maintenance_schedule(self, name: str) -> dict[str, object] | None:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM maintenance_schedules WHERE name=?", (name,)).fetchone()
        return dict(row) if row else None

    def list_maintenance_schedules(self) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM maintenance_schedules ORDER BY name").fetchall()
        return [dict(row) for row in rows]

    def due_maintenance_schedules(self, now: datetime) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM maintenance_schedules WHERE enabled=1 AND next_run_utc<=? ORDER BY next_run_utc",
                (_iso(now),),
            ).fetchall()
        return [dict(row) for row in rows]

    def begin_maintenance_schedule_run(self, schedule: dict[str, object], started: datetime) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                "INSERT INTO maintenance_schedule_runs(schedule_id,schedule_name,task_type,status,started_utc) VALUES(?,?,?,?,?)",
                (schedule['id'], schedule['name'], schedule['task_type'], 'running', _iso(started)),
            )
            return int(cursor.lastrowid)

    def complete_maintenance_schedule_run(self, run_id: int, schedule_id: int, status: str, completed: datetime,
                                          next_run: datetime, result_json: str = '{}', error: str | None = None) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE maintenance_schedule_runs SET status=?,completed_utc=?,result_json=?,error=? WHERE id=?",
                (status, _iso(completed), result_json, error, run_id),
            )
            connection.execute(
                "UPDATE maintenance_schedules SET last_run_utc=?,next_run_utc=?,last_status=?,last_error=?,updated_utc=? WHERE id=?",
                (_iso(completed), _iso(next_run), status, error, _iso(completed), schedule_id),
            )

    def list_maintenance_schedule_runs(self, limit: int = 100) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM maintenance_schedule_runs ORDER BY started_utc DESC,id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]


    def latest_tick_for(self, provider: str, instrument: str) -> dict[str, object] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT provider,instrument,timestamp_utc,bid,ask FROM ticks WHERE provider=? AND instrument=? ORDER BY timestamp_utc DESC,id DESC LIMIT 1",
                (provider, instrument),
            ).fetchone()
        return None if row is None else dict(row)

    def statistics(self) -> dict[str, object]:
        with self.connect() as connection:
            ticks = int(connection.execute("SELECT COUNT(*) FROM ticks").fetchone()[0])
            candles = int(connection.execute("SELECT COUNT(*) FROM candles").fetchone()[0])
            instruments = int(connection.execute("SELECT COUNT(DISTINCT instrument) FROM candles").fetchone()[0])
            latest_tick = connection.execute("SELECT MAX(timestamp_utc) FROM ticks").fetchone()[0]
            latest_candle = connection.execute("SELECT MAX(open_time_utc) FROM candles").fetchone()[0]
            gaps = int(connection.execute("SELECT COUNT(*) FROM data_gaps WHERE resolved=0").fetchone()[0])
            quality_issues = int(connection.execute("SELECT COUNT(*) FROM candle_quality_issues WHERE action='detected'").fetchone()[0])
        return {
            "ticks": ticks,
            "candles": candles,
            "instruments": instruments,
            "latest_tick_utc": latest_tick,
            "latest_candle_utc": latest_candle,
            "database_path": self.database_target,
            "database_backend": self.backend.kind,
            "unresolved_gaps": gaps,
            "unresolved_quality_issues": quality_issues,
        }

    def list_instruments(self) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT instrument FROM candles ORDER BY instrument"
            ).fetchall()
        return [str(row[0]) for row in rows]


    def integrity_check(self) -> dict[str, object]:
        if self.backend.kind == "postgresql":
            with self.connect() as connection:
                connection.execute("SELECT 1").fetchone()
            return {"status": "ok", "messages": ["PostgreSQL connection and transaction check passed"]}
        with self.connect() as connection:
            rows = connection.execute("PRAGMA integrity_check").fetchall()
        messages = [str(row[0]) for row in rows]
        return {"status": "ok" if messages == ["ok"] else "failed", "messages": messages}

    def retention_preview(
        self,
        ticks_before: datetime,
        operational_before: datetime,
    ) -> dict[str, int]:
        with self.connect() as connection:
            ticks = int(connection.execute(
                "SELECT COUNT(*) FROM ticks WHERE timestamp_utc < ?", (_iso(ticks_before),)
            ).fetchone()[0])
            gaps = int(connection.execute(
                "SELECT COUNT(*) FROM data_gaps WHERE resolved=1 AND detected_utc < ?",
                (_iso(operational_before),),
            ).fetchone()[0])
            states = int(connection.execute(
                "SELECT COUNT(*) FROM ingestion_state WHERE updated_utc < ?",
                (_iso(operational_before),),
            ).fetchone()[0])
            repairs = int(connection.execute(
                "SELECT COUNT(*) FROM repair_runs WHERE created_utc < ?",
                (_iso(operational_before),),
            ).fetchone()[0])
        return {
            "ticks": ticks,
            "resolved_gaps": gaps,
            "ingestion_states": states,
            "repair_runs": repairs,
        }

    def run_retention(
        self,
        ticks_before: datetime,
        operational_before: datetime,
        vacuum: bool = False,
    ) -> dict[str, object]:
        if self.backend.kind == "postgresql" and vacuum:
            raise ValueError("VACUUM from the retention endpoint is SQLite-only; use PostgreSQL maintenance tooling")
        before = (self.database_path.stat().st_size if self.database_path is not None and self.database_path.exists() else 0)
        with self.connect() as connection:
            cursor = connection.execute("DELETE FROM ticks WHERE timestamp_utc < ?", (_iso(ticks_before),))
            ticks_deleted = max(0, cursor.rowcount)
            cursor = connection.execute(
                "DELETE FROM data_gaps WHERE resolved=1 AND detected_utc < ?",
                (_iso(operational_before),),
            )
            gaps_deleted = max(0, cursor.rowcount)
            cursor = connection.execute(
                "DELETE FROM ingestion_state WHERE updated_utc < ?", (_iso(operational_before),)
            )
            states_deleted = max(0, cursor.rowcount)
            cursor = connection.execute(
                "DELETE FROM repair_runs WHERE created_utc < ?", (_iso(operational_before),)
            )
            repairs_deleted = max(0, cursor.rowcount)

        if self.backend.kind == "sqlite":
            assert self.database_path is not None
            checkpoint = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
            try:
                checkpoint.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                checkpoint.close()

            if vacuum:
                connection = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
                try:
                    connection.execute("VACUUM")
                finally:
                    connection.close()

        integrity = self.integrity_check()
        after = (self.database_path.stat().st_size if self.database_path is not None and self.database_path.exists() else 0)
        result: dict[str, object] = {
            "ticks_before_utc": _iso(ticks_before),
            "operational_before_utc": _iso(operational_before),
            "ticks_deleted": ticks_deleted,
            "resolved_gaps_deleted": gaps_deleted,
            "ingestion_states_deleted": states_deleted,
            "repair_runs_deleted": repairs_deleted,
            "database_bytes_before": before,
            "database_bytes_after": after,
            "integrity_status": integrity["status"],
            "vacuumed": vacuum,
            "created_utc": _iso(datetime.now().astimezone()),
        }
        with self.connect() as connection:
            connection.execute(
                """INSERT INTO cleanup_runs(
                    ticks_before_utc, operational_before_utc, ticks_deleted,
                    resolved_gaps_deleted, ingestion_states_deleted, repair_runs_deleted,
                    database_bytes_before, database_bytes_after, integrity_status,
                    vacuumed, created_utc
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    result["ticks_before_utc"], result["operational_before_utc"], ticks_deleted,
                    gaps_deleted, states_deleted, repairs_deleted, before, after,
                    result["integrity_status"], int(vacuum), result["created_utc"],
                ),
            )
        return result

    def list_cleanup_runs(self, limit: int = 50) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM cleanup_runs ORDER BY created_utc DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_bridge_heartbeat(self, value: dict[str, object]) -> None:
        now=datetime.now().astimezone()
        with self.connect() as c:
            c.execute("""INSERT INTO mt5_bridge_heartbeats VALUES(?,?,?,?,?,?,?) ON CONFLICT(provider_key,terminal_instance_id) DO UPDATE SET broker_name=excluded.broker_name,server_name=excluded.server_name,account_login=excluded.account_login,source_time_utc=excluded.source_time_utc,received_utc=excluded.received_utc""", (value['provider_key'],value['terminal_instance_id'],value.get('broker_name'),value.get('server_name'),value.get('account_login'),_iso(value['time_utc']),_iso(now)))

    def upsert_bridge_instruments(self, provider: str, terminal: str, observed: datetime, rows: list[dict[str, object]]) -> None:
        with self.connect() as c:
            c.executemany("""INSERT INTO mt5_bridge_instruments VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(provider_key,terminal_instance_id,provider_symbol) DO UPDATE SET canonical_instrument=excluded.canonical_instrument,digits=excluded.digits,point=excluded.point,is_visible=excluded.is_visible,display_name=excluded.display_name,description=excluded.description,path=excluded.path,asset_class=excluded.asset_class,is_selected=excluded.is_selected,observed_utc=excluded.observed_utc""", [(provider,terminal,r['provider_symbol'],r['canonical_instrument'],r['digits'],str(r['point']),int(r['is_visible']),r.get('display_name'),r.get('description'),r.get('path'),r.get('asset_class'),int(r.get('is_selected',False)),_iso(observed)) for r in rows])

    def list_bridge_instruments(self, provider: str | None=None, terminal: str | None=None) -> list[dict[str, object]]:
        q='SELECT * FROM mt5_bridge_instruments WHERE 1=1'; args=[]
        if provider: q+=' AND provider_key=?'; args.append(provider)
        if terminal: q+=' AND terminal_instance_id=?'; args.append(terminal)
        q+=' ORDER BY provider_key, canonical_instrument, provider_symbol'
        with self.connect() as c: rows=c.execute(q,args).fetchall()
        return [dict(x) for x in rows]

    def set_bridge_instrument_selection(self, provider: str, terminal: str, symbol: str, enabled: bool) -> bool:
        with self.connect() as c:
            cur=c.execute('UPDATE mt5_bridge_instruments SET is_selected=? WHERE provider_key=? AND terminal_instance_id=? AND provider_symbol=?',(int(enabled),provider,terminal,symbol))
            return cur.rowcount>0

    def upsert_bridge_quote(self, provider: str, terminal: str, value: dict[str, object], received: datetime) -> None:
        with self.connect() as c:
            c.execute("""INSERT INTO mt5_bridge_quotes VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(provider_key,terminal_instance_id,provider_symbol) DO UPDATE SET canonical_instrument=excluded.canonical_instrument,source_time_utc=excluded.source_time_utc,received_utc=excluded.received_utc,bid=excluded.bid,ask=excluded.ask,last=excluded.last,volume=excluded.volume""",(provider,terminal,value['provider_symbol'],value['canonical_instrument'],_iso(value['time_utc']),_iso(received),str(value['bid']),str(value['ask']),None if value.get('last') is None else str(value['last']),None if value.get('volume') is None else str(value['volume'])))

    def bridge_status(self) -> dict[str, object]:
        with self.connect() as c:
            heart=[dict(x) for x in c.execute('SELECT * FROM mt5_bridge_heartbeats ORDER BY received_utc DESC').fetchall()]
            instruments=c.execute('SELECT COUNT(*) FROM mt5_bridge_instruments').fetchone()[0]
            selected=c.execute('SELECT COUNT(*) FROM mt5_bridge_instruments WHERE is_selected=1').fetchone()[0]
            quotes=c.execute('SELECT COUNT(*) FROM mt5_bridge_quotes').fetchone()[0]
        return {'heartbeats':heart,'discovered_instruments':instruments,'selected_instruments':selected,'live_quotes':quotes}


    def upsert_symbol_policy(self, provider_key: str, provider_symbol: str, canonical_instrument: str,
                             enabled: bool = True, allow_live: bool = True, allow_history: bool = True,
                             priority_override: int | None = None) -> dict[str, object]:
        from .symbols import normalize_instrument
        canonical_instrument = normalize_instrument(canonical_instrument)
        now = _iso(datetime.now().astimezone())
        with self.connect() as connection:
            connection.execute("""INSERT INTO symbol_policies(provider_key,provider_symbol,canonical_instrument,enabled,allow_live,allow_history,priority_override,updated_utc)
            VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(provider_key,provider_symbol) DO UPDATE SET
            canonical_instrument=excluded.canonical_instrument,enabled=excluded.enabled,allow_live=excluded.allow_live,
            allow_history=excluded.allow_history,priority_override=excluded.priority_override,updated_utc=excluded.updated_utc""",
            (provider_key,provider_symbol,canonical_instrument,int(enabled),int(allow_live),int(allow_history),priority_override,now))
        return self.get_symbol_policy(provider_key, provider_symbol) or {}

    def get_symbol_policy(self, provider_key: str, provider_symbol: str) -> dict[str, object] | None:
        with self.connect() as connection:
            row=connection.execute("SELECT * FROM symbol_policies WHERE provider_key=? AND provider_symbol=?",(provider_key,provider_symbol)).fetchone()
        return None if row is None else self._policy_dict(row)

    def list_symbol_policies(self, provider_key: str | None = None, instrument: str | None = None) -> list[dict[str, object]]:
        where=[]; params=[]
        if provider_key is not None: where.append("provider_key=?"); params.append(provider_key)
        if instrument is not None: where.append("canonical_instrument=?"); params.append(instrument)
        sql="SELECT * FROM symbol_policies"+(" WHERE "+" AND ".join(where) if where else "")+" ORDER BY canonical_instrument,provider_key,provider_symbol"
        with self.connect() as connection: rows=connection.execute(sql,params).fetchall()
        return [self._policy_dict(row) for row in rows]

    def delete_symbol_policy(self, provider_key: str, provider_symbol: str) -> bool:
        with self.connect() as connection:
            before=connection.total_changes
            connection.execute("DELETE FROM symbol_policies WHERE provider_key=? AND provider_symbol=?",(provider_key,provider_symbol))
            return connection.total_changes>before

    @staticmethod
    def _policy_dict(row: object) -> dict[str, object]:
        value=dict(row)
        for key in ("enabled","allow_live","allow_history"): value[key]=bool(value[key])
        return value
