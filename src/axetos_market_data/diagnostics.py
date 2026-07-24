from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def build_health(store: Any, supervisor: Any, version: str) -> dict[str, object]:
    now = datetime.now(UTC)
    database_ok = False
    database_error: str | None = None
    try:
        with store.connect() as connection:
            connection.execute("SELECT 1").fetchone()
        database_ok = True
    except Exception as exc:  # diagnostic boundary
        database_error = str(exc)

    providers = supervisor.list_views()
    enabled = [p for p in providers if p["configuration"]["enabled"]]
    live = [p for p in enabled if p["runtime"]["status"] == "Live"]
    failed = [p for p in enabled if p["runtime"]["status"] == "Failed"]
    stale: list[str] = []
    for provider in live:
        runtime = provider["runtime"]
        config = provider["configuration"]
        last = _parse_utc(runtime.get("last_heartbeat_utc"))
        threshold = max(float(config.get("fallback_after_seconds", 10.0)) * 2.0, 15.0)
        if last is None or (now - last).total_seconds() > threshold:
            stale.append(str(config["provider_key"]))

    status = "healthy"
    if not database_ok or failed:
        status = "unhealthy"
    elif stale or (enabled and not live):
        status = "degraded"

    return {
        "product": "Axetos Market Data Server",
        "version": version,
        "status": status,
        "checked_utc": now.isoformat(),
        "database": {"status": "healthy" if database_ok else "unhealthy", "error": database_error},
        "providers": {
            "configured": len(providers),
            "enabled": len(enabled),
            "live": len(live),
            "failed": len(failed),
            "stale": stale,
        },
    }


def build_metrics(store: Any, supervisor: Any, started_utc: datetime) -> dict[str, object]:
    now = datetime.now(UTC)
    statistics = store.statistics()
    providers = supervisor.list_views()
    runtime = [p["runtime"] for p in providers]
    database_path = Path(store.database_path)
    return {
        "generated_utc": now.isoformat(),
        "uptime_seconds": max(0.0, (now - started_utc).total_seconds()),
        "database_size_bytes": database_path.stat().st_size if database_path.exists() else 0,
        "ticks_stored": statistics["ticks"],
        "candles_stored": statistics["candles"],
        "instruments_stored": statistics["instruments"],
        "unresolved_gaps": statistics["unresolved_gaps"],
        "providers_configured": len(providers),
        "providers_live": sum(1 for r in runtime if r["status"] == "Live"),
        "providers_failed": sum(1 for r in runtime if r["status"] == "Failed"),
        "ticks_received": sum(int(r["ticks_received"]) for r in runtime),
        "authoritative_ticks": sum(int(r["authoritative_ticks"]) for r in runtime),
        "standby_ticks": sum(int(r["standby_ticks"]) for r in runtime),
    }


def prometheus_text(metrics: dict[str, object]) -> str:
    lines = [
        "# HELP axetos_market_data_uptime_seconds Server uptime in seconds.",
        "# TYPE axetos_market_data_uptime_seconds gauge",
    ]
    for key, value in metrics.items():
        if key == "generated_utc" or not isinstance(value, (int, float)):
            continue
        metric = "axetos_market_data_" + key
        lines.append(f"# TYPE {metric} gauge")
        lines.append(f"{metric} {value}")
    return "\n".join(lines) + "\n"
