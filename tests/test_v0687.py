from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.authoritative_refresh import AuthoritativeRefreshBuffer
from axetos_market_data.bridge import BridgeCandle, BridgeCandlesRequest


def test_bridge_uploads_one_based_chunk_indexes() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert "chunk_index + 1, chunk_count, items" in source
    assert '#property version   "3.05"' in source


def test_single_chunk_startup_refresh_is_accepted() -> None:
    request = BridgeCandlesRequest.model_validate({
        "providerKey": "ICMarkets.MT5",
        "terminalInstanceId": "mt5",
        "providerSymbol": "EURUSD",
        "canonicalInstrument": "EUR/USD",
        "interval": "1m",
        "requestId": "startup-one-chunk",
        "chunkIndex": 1,
        "chunkCount": 1,
        "candles": [{
            "timeUtc": "2026-07-30T09:00:00+00:00",
            "open": "1.1", "high": "1.2", "low": "1.0", "close": "1.15",
            "tickVolume": 10,
        }],
    })
    assembled = AuthoritativeRefreshBuffer().add(request)
    assert assembled is not None
    assert assembled.chunk_index == 1
    assert assembled.chunk_count == 1
    assert assembled.authoritative is True
    assert len(assembled.candles) == 1


def test_release_metadata() -> None:
    assert __version__ == "0.68.7"
    assert 'version = "0.68.7"' in Path("pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.68.7" in Path("README.md").read_text(encoding="utf-8")
