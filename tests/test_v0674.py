from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.bridge import BridgeQuotesRequest
from axetos_market_data.clock import server_now
from axetos_market_data.web import create_app


def test_only_confirmed_live_provider_symbol_drives_quote_and_temporary_candle(tmp_path) -> None:
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    store = app.state.store
    bridge = app.state.bridge
    store.upsert_symbol_policy("ICMarkets.MT5", "BTCUSD", "BTC/USD", True, True, True)
    store.upsert_symbol_policy("ICMarkets.MT5", "BTCUSD.alt", "BTC/USD", False, False, False)
    now = server_now().replace(second=10, microsecond=0)

    rejected = bridge.quotes(BridgeQuotesRequest.model_validate({
        "ProviderKey": "ICMarkets.MT5",
        "TerminalInstanceId": "terminal",
        "Quotes": [{
            "ProviderSymbol": "BTCUSD.alt", "CanonicalInstrument": "BTC/USD",
            "TimeUtc": now.isoformat(), "Bid": "64000", "Ask": "64010",
        }],
    }))
    assert rejected == 0
    assert bridge.temporary_minute("ICMarkets.MT5", "BTC/USD") is None

    accepted = bridge.quotes(BridgeQuotesRequest.model_validate({
        "ProviderKey": "ICMarkets.MT5",
        "TerminalInstanceId": "terminal",
        "Quotes": [{
            "ProviderSymbol": "BTCUSD", "CanonicalInstrument": "BTC/USD",
            "TimeUtc": (now + timedelta(seconds=1)).isoformat(), "Bid": "64500", "Ask": "64510",
        }],
    }))
    assert accepted == 1
    quote = store.latest_bridge_quote("BTC/USD", "ICMarkets.MT5", "BTCUSD")
    assert quote is not None
    assert quote["bid"] == "64500"
    temporary = bridge.temporary_minute("ICMarkets.MT5", "BTC/USD")
    assert temporary is not None
    assert temporary.close == Decimal("64505")


def test_v0674_metadata() -> None:
    assert __version__ == "0.68.11"
    assert 'version = "0.68.11"' in Path("pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.68.11" in Path("README.md").read_text(encoding="utf-8")
