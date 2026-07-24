from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from axetos_market_data.alerts import AlertSettings, WebhookAlertDispatcher
from axetos_market_data.operational import OperationalEventService
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.web import create_app


class RecordingDispatcher:
    def __init__(self) -> None:
        self.payloads: list[dict[str, object]] = []

    def deliver(self, payload: dict[str, object]) -> dict[str, object]:
        self.payloads.append(payload)
        return {"delivered": True, "status_code": 204}


def test_operational_event_dispatches_structured_alert(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    dispatcher = RecordingDispatcher()
    service = OperationalEventService(store, dispatcher=dispatcher)  # type: ignore[arg-type]
    event_id = service.record(
        "error", "provider.failure", "Provider failed",
        provider="Oanda.MT5", instrument="EUR/USD", details={"error": "boom"},
        timestamp=datetime(2026, 7, 24, 20, 0, tzinfo=UTC),
    )
    payload = dispatcher.payloads[0]
    assert payload["event_id"] == event_id
    assert payload["category"] == "provider.failure"
    assert payload["provider"] == "Oanda.MT5"
    assert payload["details"] == {"error": "boom"}
    events = store.list_operational_events(page=1, page_size=20)
    assert any(item["category"] == "alert.delivered" for item in events["items"])


def test_alert_filter_and_cooldown():
    settings = AlertSettings(
        webhook_url="https://example.invalid/hook",
        minimum_severity="error",
        categories=frozenset({"provider.recovery"}),
        cooldown_seconds=60,
    )
    dispatcher = WebhookAlertDispatcher(settings)
    assert settings.should_deliver("info", "provider.recovery")
    assert settings.should_deliver("critical", "anything")
    assert not settings.should_deliver("info", "backfill.completed")
    assert dispatcher.status()["enabled"] is True


def test_alert_status_endpoint_disabled_by_default(tmp_path, monkeypatch):
    monkeypatch.delenv("AXETOS_ALERT_WEBHOOK_URL", raising=False)
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        response = client.get("/api/alerts/status")
        assert response.status_code == 200
        assert response.json()["enabled"] is False
        test_response = client.post("/api/alerts/test")
        assert test_response.status_code == 400


def test_alert_payload_is_json_serializable():
    settings = AlertSettings(webhook_url=None)
    dispatcher = WebhookAlertDispatcher(settings)
    result = dispatcher.deliver({
        "severity": "error", "category": "provider.failure", "message": "x",
        "provider": None, "instrument": None,
    })
    assert json.dumps(result)
    assert result["reason"] == "disabled"
