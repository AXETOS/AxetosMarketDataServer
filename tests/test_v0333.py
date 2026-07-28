import json
from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.secrets import SecretStore
from axetos_market_data.web import create_app


def test_real_mt5_password_is_stored_outside_provider_configuration(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("AXETOS_SECRET_KEY", "test-only-key")
    config_path = tmp_path / "data" / "providers.json"
    app = create_app(tmp_path / "data" / "market.sqlite", config_path)
    payload = {
        "provider_key": "Oanda.MT5", "display_name": "Oanda.MT5", "kind": "mt5",
        "enabled": False, "auto_start": False, "symbols": [],
        "account_login": 62373999, "account_server": "OANDATMS-MT5",
        "mt5_password": "real-password!", "password_env": None,
    }
    with TestClient(app) as client:
        response = client.put("/api/providers/Oanda.MT5", json=payload)
        assert response.status_code == 200, response.text
        configuration = client.get("/api/providers/Oanda.MT5").json()["configuration"]
        assert configuration["password_configured"] is True
        assert configuration["password_env"] is None
        assert "real-password!" not in json.dumps(configuration)

    raw = config_path.read_text(encoding="utf-8")
    assert "real-password!" not in raw
    assert "mt5_password" not in raw
    assert SecretStore(config_path.parent / "mt5_secrets.json").get("Oanda.MT5") == "real-password!"


def test_masked_password_preserves_existing_secret(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("AXETOS_SECRET_KEY", "test-only-key")
    config_path = tmp_path / "data" / "providers.json"
    app = create_app(tmp_path / "data" / "market.sqlite", config_path)
    base = {
        "provider_key": "Oanda.MT5", "display_name": "Oanda.MT5", "kind": "mt5",
        "enabled": False, "auto_start": False, "symbols": [], "mt5_password": "first-secret",
    }
    with TestClient(app) as client:
        assert client.put("/api/providers/Oanda.MT5", json=base).status_code == 200
        base["display_name"] = "Updated"
        base["mt5_password"] = "********"
        assert client.put("/api/providers/Oanda.MT5", json=base).status_code == 200
    assert SecretStore(config_path.parent / "mt5_secrets.json").get("Oanda.MT5") == "first-secret"


def test_v0333_release_metadata_and_ui():
    assert __version__ == "0.61.3"
    html = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert "MT5 password" in html
    assert "Password environment variable name" not in html
