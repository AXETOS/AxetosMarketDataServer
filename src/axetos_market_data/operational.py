from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .storage import MarketDataStore
from .alerts import WebhookAlertDispatcher, build_alert_payload


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

    def __init__(self, store: MarketDataStore, dispatcher: WebhookAlertDispatcher | None = None) -> None:
        self.store = store
        self.dispatcher = dispatcher or WebhookAlertDispatcher()

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
        normalized_category = category.strip().lower()
        normalized_message = message.strip()
        event_timestamp = timestamp or datetime.now(UTC)
        event_id = self.store.record_operational_event(
            normalized_severity,
            normalized_category,
            provider,
            instrument,
            normalized_message,
            event_timestamp,
            json.dumps(details or {}, sort_keys=True, separators=(",", ":"), default=str),
        )
        try:
            result = self.dispatcher.deliver(build_alert_payload(
                event_id, normalized_severity, normalized_category, normalized_message,
                event_timestamp, provider, instrument, details,
            ))
            if result.get("delivered"):
                self.store.record_operational_event(
                    "info", "alert.delivered", provider, instrument,
                    "Operational alert delivered", datetime.now(UTC),
                    json.dumps({"source_event_id": event_id, **result}, sort_keys=True, separators=(",", ":")),
                )
        except Exception as exc:
            self.store.record_operational_event(
                "error", "alert.delivery_failed", provider, instrument,
                "Operational alert delivery failed", datetime.now(UTC),
                json.dumps({"source_event_id": event_id, "error": str(exc)}, sort_keys=True, separators=(",", ":")),
            )
        return event_id

    def list(self, **filters: object) -> dict[str, object]:
        return self.store.list_operational_events(**filters)

    def export(self, **filters: object) -> list[dict[str, object]]:
        return self.store.export_operational_events(**filters)
