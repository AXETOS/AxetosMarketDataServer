from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.web import create_app


def _provider_payload() -> dict[str, object]:
    return {
        "provider_key": "Oanda.MT5",
        "display_name": "Oanda.MT5",
        "kind": "mt5",
        "enabled": False,
        "auto_start": False,
        "symbols": [],
        "terminal_path": None,
    }


def test_version_022() -> None:
    assert __version__ == "0.34.1"


def test_duplicate_canonical_mapping_is_rejected_and_annotated(tmp_path) -> None:
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        assert client.put("/api/providers/Oanda.MT5", json=_provider_payload()).status_code == 200
        first = {
            "provider_key": "Oanda.MT5",
            "provider_symbol": "EURUSD.pro",
            "canonical_instrument": "EUR/USD",
            "enabled": True,
            "allow_live": True,
            "allow_history": True,
            "priority_override": None,
        }
        assert client.put("/api/symbol-policies", json=first).status_code == 200
        duplicate = {**first, "provider_symbol": "EURUSD"}
        response = client.put("/api/symbol-policies", json=duplicate)
        assert response.status_code == 409
        assert "EURUSD.pro" in response.json()["detail"]

        symbols = client.get("/api/providers/Oanda.MT5/symbols").json()
        configured = {item["provider_symbol"]: item for item in symbols["items"]}
        assert configured["EURUSD.pro"]["mapping_state"] == "Confirmed"


def test_ignoring_mapping_removes_provider_subscription(tmp_path) -> None:
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        assert client.put("/api/providers/Oanda.MT5", json=_provider_payload()).status_code == 200
        mapping = {
            "provider_key": "Oanda.MT5",
            "provider_symbol": "EURUSD.pro",
            "canonical_instrument": "EUR/USD",
            "enabled": True,
            "allow_live": True,
            "allow_history": True,
            "priority_override": None,
        }
        assert client.put("/api/symbol-policies", json=mapping).status_code == 200
        provider = client.get("/api/providers/Oanda.MT5").json()["configuration"]
        assert provider["symbols"] == ["EURUSD.pro"]
        mapping.update({"enabled": False, "allow_live": False, "allow_history": False})
        assert client.put("/api/symbol-policies", json=mapping).status_code == 200
        provider = client.get("/api/providers/Oanda.MT5").json()["configuration"]
        assert provider["symbols"] == []
