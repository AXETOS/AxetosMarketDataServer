import json
from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data.config import ConfigurationStore, ProviderConfig
from axetos_market_data.web import create_app


def test_invalid_secret_like_password_env_is_removed_from_configuration_file(tmp_path: Path):
    path = tmp_path / "providers.json"
    path.write_text(json.dumps({"providers": [{
        "provider_key": "Oanda.MT5",
        "display_name": "OANDA",
        "kind": "mt5",
        "password_env": "actual-password!",
    }]}), encoding="utf-8")

    configs = ConfigurationStore(path).read_all()

    assert configs[0].password_env is None
    assert "actual-password!" not in path.read_text(encoding="utf-8")


def test_provider_api_masks_password_environment_reference(tmp_path: Path):
    db = tmp_path / "market.sqlite"
    cfg = tmp_path / "providers.json"
    ConfigurationStore(cfg).write_all([ProviderConfig(
        provider_key="Oanda.MT5",
        display_name="OANDA",
        kind="mt5",
        enabled=False,
        password_env="AXETOS_MT5_OANDA_PASSWORD",
    )])
    app = create_app(db, configuration_path=cfg)
    with TestClient(app) as client:
        response = client.get("/api/providers/Oanda.MT5")
        assert response.status_code == 200
        configuration = response.json()["configuration"]
        assert configuration["password_env"] == "********"
        assert configuration["password_env_configured"] is True


def test_masked_password_environment_reference_is_preserved_on_edit(tmp_path: Path):
    db = tmp_path / "market.sqlite"
    cfg = tmp_path / "providers.json"
    ConfigurationStore(cfg).write_all([ProviderConfig(
        provider_key="Oanda.MT5",
        display_name="OANDA",
        kind="mt5",
        enabled=False,
        password_env="AXETOS_MT5_OANDA_PASSWORD",
    )])
    app = create_app(db, configuration_path=cfg)
    with TestClient(app) as client:
        current = client.get("/api/providers/Oanda.MT5").json()["configuration"]
        current.pop("password_env_configured", None)
        current["password_env"] = "********"
        response = client.put("/api/providers/Oanda.MT5", json=current)
        assert response.status_code == 200

    assert ConfigurationStore(cfg).read_all()[0].password_env == "AXETOS_MT5_OANDA_PASSWORD"


def test_password_environment_reference_requires_uppercase_variable_name(tmp_path: Path):
    app = create_app(tmp_path / "market.sqlite", configuration_path=tmp_path / "providers.json")
    payload = {
        "provider_key": "Oanda.MT5",
        "display_name": "OANDA",
        "kind": "mt5",
        "enabled": False,
        "password_env": "my-real-password!",
    }
    with TestClient(app) as client:
        response = client.put("/api/providers/Oanda.MT5", json=payload)
        assert response.status_code == 422
