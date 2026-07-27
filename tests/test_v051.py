from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data.config import ConfigurationStore, ProviderConfig
from axetos_market_data.web import create_app


def test_enabled_bridge_symbols_comes_from_server_provider_configuration(tmp_path: Path) -> None:
    config_path = tmp_path / "providers.json"
    ConfigurationStore(config_path).upsert(ProviderConfig(
        provider_key="ICMarkets.MT5",
        display_name="IC Markets",
        kind="mt5",
        symbols=["SOLUSD", "BTCUSD", "LTCUSD", "EURUSD"],
    ))
    app = create_app(tmp_path / "market.sqlite", config_path)
    with TestClient(app) as client:
        response = client.get(
            "/api/market-data/mt5/enabled-symbols.txt",
            params={"providerKey": "ICMarkets.MT5", "terminalInstanceId": "terminal-a"},
        )
    assert response.status_code == 200
    assert response.text == "SOLUSD,BTCUSD,LTCUSD,EURUSD"


def test_enabled_bridge_symbols_ignores_bridge_market_watch_selection(tmp_path: Path) -> None:
    config_path = tmp_path / "providers.json"
    ConfigurationStore(config_path).upsert(ProviderConfig(
        provider_key="Oanda.MT5",
        display_name="Oanda",
        kind="mt5",
        symbols=["EURUSD.pro"],
    ))
    app = create_app(tmp_path / "market.sqlite", config_path)
    store = app.state.store
    store.upsert_bridge_instruments("Oanda.MT5", "terminal-a", __import__('datetime').datetime.now(__import__('datetime').UTC), [
        {"provider_symbol": "BTCUSD", "canonical_instrument": "BTC/USD", "digits": 2, "point": "0.01", "is_visible": True, "is_selected": True},
        {"provider_symbol": "EURUSD.pro", "canonical_instrument": "EUR/USD", "digits": 5, "point": "0.00001", "is_visible": True, "is_selected": False},
    ])
    with TestClient(app) as client:
        response = client.get(
            "/api/market-data/mt5/enabled-symbols.txt",
            params={"providerKey": "Oanda.MT5", "terminalInstanceId": "terminal-a"},
        )
    assert response.text == "EURUSD.pro"


def test_bridge_applies_empty_server_selection_as_stop() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert 'server selection is empty; streaming stopped' in source
    assert 'retaining the current stream' not in source
    assert '#property version   "1.23"' in source
