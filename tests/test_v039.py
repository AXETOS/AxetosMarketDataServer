from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.diagnostics import build_liveness, build_readiness
from axetos_market_data.web import create_app


class HealthyStore:
    def connect(self):
        class Connection:
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def execute(self, _query):
                return SimpleNamespace(fetchone=lambda: (1,))
        return Connection()


class BrokenStore:
    def connect(self):
        raise RuntimeError("database unavailable")


class EmptySupervisor:
    def list_views(self):
        return []


def test_liveness_is_independent_of_provider_state() -> None:
    from datetime import UTC, datetime, timedelta
    started = datetime.now(UTC) - timedelta(seconds=2)
    payload = build_liveness(__version__, started)
    assert payload["status"] == "alive"
    assert payload["uptime_seconds"] >= 1


def test_readiness_accepts_healthy_or_degraded_service() -> None:
    payload = build_readiness(HealthyStore(), EmptySupervisor(), __version__)
    assert payload["ready"] is True
    assert payload["status"] == "ready"


def test_readiness_fails_closed_when_database_is_unavailable() -> None:
    payload = build_readiness(BrokenStore(), EmptySupervisor(), __version__)
    assert payload["ready"] is False
    assert payload["status"] == "not_ready"


def test_probe_endpoints_return_expected_http_status(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "market.sqlite")
    with TestClient(app) as client:
        assert client.get("/api/live").status_code == 200
        ready = client.get("/api/ready")
        assert ready.status_code == 200
        assert ready.json()["ready"] is True


def test_v039_release_metadata() -> None:
    root = Path(__file__).resolve().parents[1]
    assert __version__ == "0.61.4"
    assert 'version = "0.61.4"' in (root / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.61.4" in (root / "README.md").read_text(encoding="utf-8")
