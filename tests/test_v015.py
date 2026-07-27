from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.symbols import SymbolResolver, normalize_instrument
from axetos_market_data.web import create_app


def test_version_015():
    assert __version__ == "0.60.7"


def test_common_broker_symbol_variants_share_identity():
    assert normalize_instrument("EURUSD") == "EUR/USD"
    assert normalize_instrument("eurusd.pro") == "EUR/USD"
    assert normalize_instrument("EURUSD_raw") == "EUR/USD"
    assert normalize_instrument("XAUUSD.a") == "XAU/USD"
    assert normalize_instrument("BTCUSDT") == "BTC/USDT"


def test_explicit_policy_overrides_reported_and_automatic_names(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    app.state.store.upsert_symbol_policy("Broker.MT5", "GER40.cash", "DAX/EUR")
    resolution = SymbolResolver(app.state.store).resolve("Broker.MT5", "GER40.cash", "GER40")
    assert resolution.canonical_instrument == "DAX/EUR"
    assert resolution.source == "policy"
    app.state.bridge.shutdown()


def test_symbol_normalization_endpoint_reports_resolution_source(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        auto = client.get("/api/symbol-normalization", params={"provider_key": "IC", "provider_symbol": "EURUSD.pro"})
        assert auto.status_code == 200
        assert auto.json()["canonical_instrument"] == "EUR/USD"
        assert auto.json()["source"] == "automatic"

        client.put("/api/symbol-policies", json={
            "provider_key": "IC", "provider_symbol": "US500.cash", "canonical_instrument": "SPX/USD"
        })
        explicit = client.get("/api/symbol-normalization", params={"provider_key": "IC", "provider_symbol": "US500.cash"})
        assert explicit.json()["canonical_instrument"] == "SPX/USD"
        assert explicit.json()["source"] == "policy"
