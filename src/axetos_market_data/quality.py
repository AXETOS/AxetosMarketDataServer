from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from .domain import Candle
from .storage import MarketDataStore, _iso


@dataclass(frozen=True, slots=True)
class QualityScanResult:
    scanned: int
    issues: int
    severe: int


class CandleQualityService:
    """Detects, quarantines, and rebuilds structurally invalid or implausible candles."""

    def __init__(self, store: MarketDataStore) -> None:
        self.store = store

    def scan(self, provider: str | None = None, instrument: str | None = None,
             timeframe: str | None = None, limit: int = 10000,
             max_move_percent: Decimal = Decimal("20")) -> QualityScanResult:
        where, args = ["1=1"], []
        for column, value in (("provider", provider), ("instrument", instrument), ("timeframe", timeframe)):
            if value:
                where.append(f"{column}=?")
                args.append(value)
        args.append(limit)
        with self.store.connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM candles WHERE {' AND '.join(where)} ORDER BY provider,instrument,timeframe,open_time_utc LIMIT ?",
                args,
            ).fetchall()
            previous: dict[tuple[str, str, str], Decimal] = {}
            issues = severe = 0
            for row in rows:
                key = (row["provider"], row["instrument"], row["timeframe"])
                o, h, l, c = map(Decimal, (row["open"], row["high"], row["low"], row["close"]))
                reasons: list[tuple[str, str]] = []
                if min(o, h, l, c) <= 0:
                    reasons.append(("non-positive OHLC value", "severe"))
                if h < max(o, c) or l > min(o, c) or h < l:
                    reasons.append(("invalid OHLC range", "severe"))
                if int(row["tick_count"]) < 0:
                    reasons.append(("negative tick count", "severe"))
                prev = previous.get(key)
                if prev and prev > 0:
                    move = abs(o - prev) / prev * Decimal("100")
                    if move > max_move_percent:
                        reasons.append((f"opening jump {move:.4f}% exceeds {max_move_percent}%", "warning"))
                previous[key] = c
                for reason, severity in reasons:
                    connection.execute(
                        """INSERT OR IGNORE INTO candle_quality_issues(
                        provider,instrument,timeframe,open_time_utc,reason,severity,action,detected_utc)
                        VALUES(?,?,?,?,?,?, 'detected', ?)""",
                        (*key, row["open_time_utc"], reason, severity, _iso(datetime.now(UTC))),
                    )
                    issues += 1
                    severe += severity == "severe"
        return QualityScanResult(len(rows), issues, severe)

    def list_issues(self, limit: int = 500, action: str | None = None) -> list[dict[str, object]]:
        query = "SELECT * FROM candle_quality_issues"
        args: list[object] = []
        if action:
            query += " WHERE action=?"
            args.append(action)
        query += " ORDER BY detected_utc DESC LIMIT ?"
        args.append(limit)
        with self.store.connect() as connection:
            return [dict(row) for row in connection.execute(query, args).fetchall()]

    def quarantine(self, issue_id: int) -> dict[str, object]:
        with self.store.connect() as connection:
            issue = connection.execute("SELECT * FROM candle_quality_issues WHERE id=?", (issue_id,)).fetchone()
            if issue is None:
                raise KeyError("quality issue not found")
            candle = connection.execute(
                "SELECT * FROM candles WHERE provider=? AND instrument=? AND timeframe=? AND open_time_utc=?",
                (issue["provider"], issue["instrument"], issue["timeframe"], issue["open_time_utc"]),
            ).fetchone()
            if candle is None:
                raise KeyError("candle not found")
            connection.execute(
                """INSERT INTO quarantined_candles(provider,instrument,timeframe,open_time_utc,open,high,low,close,
                tick_count,volume,complete,reason,quarantined_utc) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (candle["provider"], candle["instrument"], candle["timeframe"], candle["open_time_utc"],
                 candle["open"], candle["high"], candle["low"], candle["close"], candle["tick_count"],
                 candle["volume"], candle["complete"], issue["reason"], _iso(datetime.now(UTC))),
            )
            connection.execute(
                "DELETE FROM candles WHERE provider=? AND instrument=? AND timeframe=? AND open_time_utc=?",
                (candle["provider"], candle["instrument"], candle["timeframe"], candle["open_time_utc"]),
            )
            connection.execute("UPDATE candle_quality_issues SET action='quarantined' WHERE id=?", (issue_id,))
        return {"issue_id": issue_id, "quarantined": True}

    def rebuild_one_minute(self, issue_id: int) -> dict[str, object]:
        with self.store.connect() as connection:
            issue = connection.execute("SELECT * FROM candle_quality_issues WHERE id=?", (issue_id,)).fetchone()
            if issue is None:
                raise KeyError("quality issue not found")
            if issue["timeframe"] != "1m":
                raise ValueError("only one-minute candles can be rebuilt directly from ticks")
            start = datetime.fromisoformat(issue["open_time_utc"])
            end = start + timedelta(minutes=1)
            rows = connection.execute(
                """SELECT * FROM ticks WHERE provider=? AND instrument=? AND timestamp_utc>=? AND timestamp_utc<?
                ORDER BY timestamp_utc,id""",
                (issue["provider"], issue["instrument"], _iso(start), _iso(end)),
            ).fetchall()
        if not rows:
            raise ValueError("no stored ticks are available for this candle")
        prices = [(Decimal(r["bid"]) + Decimal(r["ask"])) / Decimal("2") for r in rows]
        volumes = [Decimal(r["volume"]) for r in rows if r["volume"] is not None]
        candle = Candle(issue["provider"], issue["instrument"], "1m", start,
                        prices[0], max(prices), min(prices), prices[-1], len(prices),
                        sum(volumes, Decimal("0")) if volumes else None, True)
        self.store.upsert_candle(candle)
        with self.store.connect() as connection:
            connection.execute(
                "UPDATE candle_quality_issues SET action='rebuilt',resolved_utc=? WHERE id=?",
                (_iso(datetime.now(UTC)), issue_id),
            )
        return {"issue_id": issue_id, "rebuilt": True, "tick_count": len(prices)}
