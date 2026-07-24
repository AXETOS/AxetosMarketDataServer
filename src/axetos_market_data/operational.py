from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .storage import MarketDataStore


@dataclass(frozen=True, slots=True)
class OperationalEvent:
    severity: str
    category: str
    message: str
    provider: str | None = None
    instrument: str | None = None
    details: dict[str, Any] | None = None
    timestamp: datetime | None = None


class OperationalEventService:
    """Persistent structured operational event journal."""

    def __init__(self, store: MarketDataStore) -> None:
        self.store = store

    def record(
        self,
        severity: str,
        category: str,
        message: str,
        *,
        provider: str | None = None,
        instrument: str | None = None,
        details: dict[str, Any] | None = None,
        timestamp: datetime | None = None,
    ) -> int:
        normalized_severity = severity.strip().lower()
        if normalized_severity not in {"debug", "info", "warning", "error", "critical"}:
            raise ValueError(f"Unsupported event severity: {severity}")
        if not category.strip():
            raise ValueError("Event category is required")
        if not message.strip():
            raise ValueError("Event message is required")
        return self.store.record_operational_event(
            normalized_severity,
            category.strip().lower(),
            provider,
            instrument,
            message.strip(),
            timestamp or datetime.now(UTC),
            json.dumps(details or {}, sort_keys=True, separators=(",", ":"), default=str),
        )

    def list(self, **filters: object) -> dict[str, object]:
        return self.store.list_operational_events(**filters)

    def export(self, **filters: object) -> list[dict[str, object]]:
        return self.store.export_operational_events(**filters)
