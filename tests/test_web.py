from fastapi.testclient import TestClient

from axetos_market_data.web import create_app


def test_health_and_provider_configuration(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        assert client.get("/api/health").status_code == 200
        response = client.put(
            "/api/providers/Mock.Test",
            json={
                "provider_key": "Mock.Test",
                "display_name": "Test Provider",
                "kind": "mock",
                "enabled": False,
                "auto_start": False,
                "poll_interval_seconds": 0.1,
                "symbols": ["EUR/USD"],
                "terminal_path": None,
            },
        )
        assert response.status_code == 200
        providers = client.get("/api/providers").json()["providers"]
        assert providers[0]["configuration"]["provider_key"] == "Mock.Test"
