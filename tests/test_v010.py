from fastapi.testclient import TestClient
from axetos_market_data.policies import choose_canonical_source
from axetos_market_data.web import create_app


def test_symbol_policy_and_canonical_selection(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        for key, priority in (("primary", 10), ("secondary", 20)):
            response = client.put(f"/api/providers/{key}", json={
                "provider_key": key, "display_name": key.title(), "kind": "mock",
                "enabled": True, "auto_start": False, "symbols": ["EUR/USD"],
                "priority": priority,
            })
            assert response.status_code == 200
        for key, symbol in (("primary", "EURUSD.raw"), ("secondary", "EURUSD.pro")):
            response = client.put("/api/symbol-policies", json={
                "provider_key": key, "provider_symbol": symbol,
                "canonical_instrument": "EUR/USD", "enabled": True,
                "allow_live": True, "allow_history": True,
            })
            assert response.status_code == 200
        routes = client.get("/api/canonical-sources", params={"instrument": "EUR/USD"}).json()
        assert routes["routes"][0]["preferred"]["provider_key"] == "primary"
        assert client.post("/api/providers/primary/test").json()["ok"] is True


def test_priority_override():
    providers = [
        {"provider_key": "a", "enabled": True, "priority": 10},
        {"provider_key": "b", "enabled": True, "priority": 20},
    ]
    policies = [
        {"provider_key": "a", "provider_symbol": "A", "canonical_instrument": "X", "enabled": True, "allow_live": True, "allow_history": True, "priority_override": None},
        {"provider_key": "b", "provider_symbol": "B", "canonical_instrument": "X", "enabled": True, "allow_live": True, "allow_history": True, "priority_override": 5},
    ]
    assert choose_canonical_source("X", providers, policies)["preferred"]["provider_key"] == "b"
