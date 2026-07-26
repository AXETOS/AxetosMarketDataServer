import csv
import io
import json

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.web import create_app


def test_version_014():
    assert __version__ == "0.58.0"


def test_operational_event_csv_export_respects_filters(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    app.state.events.record("info", "provider.start", "Started", provider="ICMarkets.MT5")
    app.state.events.record("error", "provider.failure", "Offline", provider="Oanda.MT5", details={"code": 503})

    with TestClient(app) as client:
        response = client.get(
            "/api/operational-events/export",
            params={"format": "csv", "severity": "error"},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=" in response.headers["content-disposition"]
    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert len(rows) == 1
    assert rows[0]["category"] == "provider.failure"
    assert json.loads(rows[0]["details_json"])["code"] == 503


def test_operational_event_jsonl_export(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    app.state.events.record("warning", "quality.scan", "Issue found", instrument="EUR/USD")

    with TestClient(app) as client:
        response = client.get("/api/operational-events/export", params={"format": "jsonl"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    items = [json.loads(line) for line in response.text.splitlines() if line]
    assert len(items) == 1
    assert items[0]["instrument"] == "EUR/USD"
    assert items[0]["details"] == {}
