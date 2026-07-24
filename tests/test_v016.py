from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from axetos_market_data.scheduler import MaintenanceScheduler
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.web import create_app


def test_persistent_retention_schedule_executes_and_records_history(tmp_path):
    store = MarketDataStore(tmp_path / "data.sqlite")
    store.initialize()
    now = datetime.now(UTC)
    store.upsert_maintenance_schedule("nightly-retention", "retention", True, 60, now - timedelta(seconds=1), 30, 90, False)
    scheduler = MaintenanceScheduler(store)
    results = scheduler.run_due(now)
    assert results[0]["status"] == "completed"
    schedule = store.get_maintenance_schedule("nightly-retention")
    assert schedule is not None
    assert schedule["last_status"] == "completed"
    assert schedule["last_run_utc"] is not None
    assert store.list_maintenance_schedule_runs(10)[0]["status"] == "completed"


def test_maintenance_schedule_api(tmp_path):
    app = create_app(tmp_path / "data.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        response = client.put("/api/maintenance/schedules/daily-retention", json={
            "name": "daily-retention", "task_type": "retention", "enabled": True,
            "interval_minutes": 1440, "tick_days": 30, "operational_days": 90,
            "vacuum": False, "run_immediately": False,
        })
        assert response.status_code == 200
        assert response.json()["name"] == "daily-retention"
        listing = client.get("/api/maintenance/schedules").json()
        assert listing["count"] == 1
        run = client.post("/api/maintenance/schedules/daily-retention/run")
        assert run.status_code == 200
        assert run.json()["status"] == "completed"
        history = client.get("/api/maintenance/runs").json()
        assert history["count"] == 1
