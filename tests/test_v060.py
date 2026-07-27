from axetos_market_data.config import ProviderConfig
from axetos_market_data.providers.mt5 import MetaTrader5TickProvider
from axetos_market_data.web import create_app
from fastapi.testclient import TestClient


def test_batch_and_maintenance_defaults():
    config = ProviderConfig(provider_key="x", display_name="X")
    assert config.batch_window_seconds == 5
    assert config.batch_limit == 50_000
    assert config.maintenance_enabled is False
    assert config.maintenance_interval_minutes == 60


def test_mt5_symbol_normalization():
    assert MetaTrader5TickProvider._canonical_symbol("EURUSD.pro") == "EUR/USD"
    assert MetaTrader5TickProvider._canonical_symbol("GBPUSD.raw") == "GBP/USD"


def test_health_reports_v070(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        assert client.get("/api/health").json()["version"] == "0.59.0"
