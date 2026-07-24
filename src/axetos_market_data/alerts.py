from __future__ import annotations

import json
import os
import threading
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any


_SEVERITY_RANK = {"debug": 10, "info": 20, "warning": 30, "error": 40, "critical": 50}
_DEFAULT_CATEGORIES = {
    "alert.test",
    "provider.failure",
    "provider.recovery",
    "provider.connection_test",
    "maintenance.schedule.failed",
    "quality.scan",
    "quality.quarantine",
    "database.integrity",
    "backfill.failed",
}


@dataclass(frozen=True, slots=True)
class AlertSettings:
    webhook_url: str | None
    minimum_severity: str = "error"
    categories: frozenset[str] = frozenset(_DEFAULT_CATEGORIES)
    timeout_seconds: float = 5.0
    cooldown_seconds: float = 60.0

    @classmethod
    def from_environment(cls) -> "AlertSettings":
        url = os.getenv("AXETOS_ALERT_WEBHOOK_URL", "").strip() or None
        minimum = os.getenv("AXETOS_ALERT_MIN_SEVERITY", "error").strip().lower()
        if minimum not in _SEVERITY_RANK:
            raise ValueError("AXETOS_ALERT_MIN_SEVERITY must be debug, info, warning, error, or critical")
        raw_categories = os.getenv("AXETOS_ALERT_CATEGORIES", "").strip()
        categories = (
            frozenset(value.strip().lower() for value in raw_categories.split(",") if value.strip())
            if raw_categories
            else frozenset(_DEFAULT_CATEGORIES)
        )
        return cls(
            webhook_url=url,
            minimum_severity=minimum,
            categories=categories,
            timeout_seconds=max(0.5, float(os.getenv("AXETOS_ALERT_TIMEOUT_SECONDS", "5"))),
            cooldown_seconds=max(0.0, float(os.getenv("AXETOS_ALERT_COOLDOWN_SECONDS", "60"))),
        )

    @property
    def enabled(self) -> bool:
        return bool(self.webhook_url)

    def should_deliver(self, severity: str, category: str) -> bool:
        return category.lower() in self.categories or _SEVERITY_RANK[severity.lower()] >= _SEVERITY_RANK[self.minimum_severity]


class WebhookAlertDispatcher:
    """Sends structured operational alerts to a generic JSON webhook."""

    _cooldowns: dict[str, float] = {}
    _lock = threading.Lock()

    def __init__(self, settings: AlertSettings | None = None) -> None:
        self.settings = settings or AlertSettings.from_environment()

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.settings.enabled,
            "minimum_severity": self.settings.minimum_severity,
            "categories": sorted(self.settings.categories),
            "timeout_seconds": self.settings.timeout_seconds,
            "cooldown_seconds": self.settings.cooldown_seconds,
        }

    def deliver(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.settings.enabled:
            return {"delivered": False, "reason": "disabled"}
        severity = str(payload["severity"]).lower()
        category = str(payload["category"]).lower()
        if not self.settings.should_deliver(severity, category):
            return {"delivered": False, "reason": "filtered"}

        key = "|".join(
            str(payload.get(name) or "")
            for name in ("category", "provider", "instrument", "message")
        )
        now = time.monotonic()
        with self._lock:
            last = self._cooldowns.get(key)
            if last is not None and now - last < self.settings.cooldown_seconds:
                return {"delivered": False, "reason": "cooldown"}
            self._cooldowns[key] = now

        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        request = urllib.request.Request(
            self.settings.webhook_url or "",
            data=body,
            headers={"Content-Type": "application/json", "User-Agent": "AxetosMarketDataServer/0.25.0"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.settings.timeout_seconds) as response:
                status = int(getattr(response, "status", 200))
            if status < 200 or status >= 300:
                raise RuntimeError(f"Webhook returned HTTP {status}")
            return {"delivered": True, "status_code": status}
        except Exception:
            with self._lock:
                self._cooldowns.pop(key, None)
            raise


def build_alert_payload(
    event_id: int,
    severity: str,
    category: str,
    message: str,
    timestamp: datetime,
    provider: str | None,
    instrument: str | None,
    details: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "source": "Axetos Market Data Server",
        "version": "0.25.0",
        "event_id": event_id,
        "severity": severity,
        "category": category,
        "message": message,
        "timestamp_utc": timestamp.isoformat(),
        "provider": provider,
        "instrument": instrument,
        "details": details or {},
    }
