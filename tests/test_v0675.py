from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from axetos_market_data import __version__
from axetos_market_data.bridge import BridgeCandlesRequest
from axetos_market_data.clock import server_now
from axetos_market_data.web import create_app


def _request(symbol: str, price: str) -> BridgeCandlesRequest:
    now = server_now().replace(second=0, microsecond=0) - timedelta(minutes=1)
    return BridgeCandlesRequest.model_validate({
        "ProviderKey": "ICMarkets.MT5",
        "TerminalInstanceId": "terminal",
        "ProviderSymbol": symbol,
        "CanonicalInstrument": "ETH/USD",
        "Interval": "1m",
        "Candles": [{
            "TimeUtc": now.isoformat(),
            "Open": price, "High": price, "Low": price, "Close": price,
            "TickVolume": 1,
        }],
    })


def test_only_confirmed_history_symbol_can_write_candles(tmp_path) -> None:
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    store = app.state.store
    bridge = app.state.bridge
    store.upsert_symbol_policy("ICMarkets.MT5", "ETHUSD", "ETH/USD", True, True, True, 1)
    store.upsert_symbol_policy("ICMarkets.MT5", "ETHUSD.alt", "ETH/USD", True, False, True, 2)

    with pytest.raises(ValueError, match="not the confirmed history symbol"):
        bridge.candles(_request("ETHUSD.alt", "1915"))
    assert store.read_candles("ETH/USD", "1m", 10, "ICMarkets.MT5") == []

    assert bridge.candles(_request("ETHUSD", "1906")) == 1
    candles = store.read_candles("ETH/USD", "1m", 10, "ICMarkets.MT5")
    assert len(candles) == 1
    assert candles[0].close == Decimal("1906")


def test_v0675_metadata() -> None:
    assert __version__ == "0.67.8"
    assert 'version = "0.67.8"' in Path("pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.67.8" in Path("README.md").read_text(encoding="utf-8")
