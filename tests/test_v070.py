from fastapi.testclient import TestClient

from axetos_market_data.web import create_app


def test_health_metrics_and_prometheus_endpoint(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        health = client.get("/api/health")
        assert health.status_code == 200
        assert health.json()["version"] == "0.36.0"
        assert health.json()["database"]["status"] == "healthy"

        metrics = client.get("/api/metrics")
        assert metrics.status_code == 200
        assert metrics.json()["providers_configured"] == 0
        assert metrics.json()["database_size_bytes"] > 0

        prometheus = client.get("/metrics")
        assert prometheus.status_code == 200
        assert "axetos_market_data_uptime_seconds" in prometheus.text
