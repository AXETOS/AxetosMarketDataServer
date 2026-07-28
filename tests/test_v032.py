from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.config import ProviderConfig
from axetos_market_data.runtime import ProviderWorker
from axetos_market_data.routing import ProviderAuthorityRegistry
from axetos_market_data.storage import MarketDataStore


def test_release_metadata_and_readme():
    assert __version__ == "0.67.1"
    readme = Path("README.md").read_text(encoding="utf-8")
    assert "## Version 0.67.1" in readme
    assert "configured, MT5-selected, monitored, and stored instruments" in readme


def test_configured_symbols_register_feed_monitors(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    config = ProviderConfig(provider_key="Oanda.MT5", display_name="Oanda", kind="mt5", symbols=["EURUSD.pro", "BTCUSD", "SOLUSD"])
    worker = ProviderWorker(config, store, ProviderAuthorityRegistry())
    reports = worker.feed.reports()
    assert {row["instrument"] for row in reports} == {"EUR/USD", "BTC/USD", "SOL/USD"}
    view = worker.view()
    assert view["configured_instruments"] == 3
    assert view["selected_instruments"] == 0
    assert all(row["selection_state"] == "configured" for row in view["symbols"])


def test_symbol_selection_diagnostics_are_exposed(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    config = ProviderConfig(provider_key="Oanda.MT5", display_name="Oanda", kind="mt5", symbols=["EURUSD.pro", "BTCUSD"])
    worker = ProviderWorker(config, store, ProviderAuthorityRegistry())
    class Provider:
        selection_status = {
            "EURUSD.pro": {"selected": True, "error": None},
            "BTCUSD": {"selected": False, "error": "symbol unavailable"},
        }
    worker._provider_instance = Provider()
    view = worker.view()
    assert view["selected_instruments"] == 1
    assert view["failed_instruments"] == 1
    states = {row["provider_symbol"]: row["selection_state"] for row in view["symbols"]}
    assert states == {"EURUSD.pro": "selected", "BTCUSD": "failed"}


def test_management_ui_distinguishes_instrument_counts():
    source = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    for label in ("Configured instruments", "Selected instruments", "Monitored feeds", "Stored instruments"):
        assert label in source
    assert "Configured / selected / failed" not in source
