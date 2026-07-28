from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.config import ConfigurationStore, ProviderConfig
from axetos_market_data.web import create_app

ROOT = Path(__file__).resolve().parents[1]
BRIDGE = ROOT / "bridges" / "mt5" / "Experts" / "AxetosMarketDataBridge.mq5"


def _discover(client: TestClient) -> None:
    response = client.post("/api/market-data/ingest/mt5/instruments", json={
        "providerKey": "ICMarkets.MT5",
        "terminalInstanceId": "terminal-1",
        "timeUtc": datetime.now(UTC).isoformat(),
        "instruments": [
            {"providerSymbol": "EURUSD", "canonicalInstrument": "EUR/USD", "digits": 5, "point": 0.00001, "isVisible": True, "isSelected": True},
            {"providerSymbol": "GBPUSD", "canonicalInstrument": "GBP/USD", "digits": 5, "point": 0.00001, "isVisible": True, "isSelected": False},
        ],
    })
    assert response.status_code == 200, response.text


def test_release_metadata() -> None:
    assert __version__ == "0.62.1"
    assert 'version = "0.62.1"' in (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.62.1" in (ROOT / "README.md").read_text(encoding="utf-8")


def test_enabled_symbols_plain_text_contract(tmp_path) -> None:
    config_path = tmp_path / "providers.json"
    ConfigurationStore(config_path).upsert(ProviderConfig(
        provider_key="ICMarkets.MT5", display_name="IC Markets", kind="mt5", symbols=["EURUSD"]
    ))
    app = create_app(f"sqlite:///{tmp_path / 'symbols.sqlite'}", config_path)
    with TestClient(app) as client:
        _discover(client)
        response = client.get("/api/market-data/mt5/enabled-symbols.txt", params={"providerKey": "ICMarkets.MT5", "terminalInstanceId": "terminal-1"})
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")
        assert response.text == "EURUSD"


def test_optional_bridge_control_endpoints_exist(tmp_path) -> None:
    app = create_app(f"sqlite:///{tmp_path / 'control.sqlite'}")
    with TestClient(app) as client:
        repair = client.get("/api/market-data/mt5/repair-request.txt", params={"providerKey": "ICMarkets.MT5", "terminalInstanceId": "terminal-1"})
        assert repair.status_code == 200
        assert repair.text == ""
        result = client.post("/api/market-data/mt5/repair-result", params={"providerKey": "ICMarkets.MT5", "terminalInstanceId": "terminal-1", "providerSymbol": "EURUSD", "interval": "1m", "completed": "true", "requestId": "r1"}, json={})
        assert result.status_code == 200
        assert result.json()["accepted"] is False
        assert result.json()["acknowledgement"] == "IGNORED"


def test_bridge_http_4xx_does_not_trigger_transport_backoff() -> None:
    source = BRIDGE.read_text(encoding="utf-8")
    assert '#property version   "1.30"' in source
    assert "void RecordHttpApplicationFailure" in source
    assert "if(status < 0 || status >= 500)" in source
    assert "continuing other requests" in source
