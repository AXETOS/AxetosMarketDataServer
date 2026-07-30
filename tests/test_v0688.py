from pathlib import Path

from axetos_market_data.authoritative_refresh import AuthoritativeRefreshBuffer
from axetos_market_data.bridge import BridgeCandlesRequest


def _request(index: int, count: int = 1) -> BridgeCandlesRequest:
    return BridgeCandlesRequest.model_validate({
        "providerKey": "ICMarkets.MT5",
        "terminalInstanceId": "mt5",
        "providerSymbol": "EURUSD",
        "canonicalInstrument": "EUR/USD",
        "interval": "1m",
        "requestId": "startup-one",
        "chunkIndex": index,
        "chunkCount": count,
        "candles": [],
    })


def test_authoritative_refresh_accepts_legacy_zero_based_single_chunk() -> None:
    assembled = AuthoritativeRefreshBuffer().add(_request(0))
    assert assembled is not None
    assert assembled.chunk_index == 1
    assert assembled.chunk_count == 1
    assert assembled.authoritative is True


def test_authoritative_refresh_accepts_current_one_based_single_chunk() -> None:
    assembled = AuthoritativeRefreshBuffer().add(_request(1))
    assert assembled is not None


def test_bridge_logs_http_error_response_body() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert "response=%s" in source
    assert '#property version   "3.06"' in source
