from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.authoritative_refresh import AuthoritativeRefreshBuffer
from axetos_market_data.bridge import BridgeCandlesRequest


def request(index: int, count: int, minute: int) -> BridgeCandlesRequest:
    return BridgeCandlesRequest.model_validate({
        "providerKey": "ICMarkets.MT5",
        "terminalInstanceId": "terminal",
        "providerSymbol": "EURUSD",
        "canonicalInstrument": "EUR/USD",
        "interval": "1m",
        "requestId": "repair-window",
        "chunkIndex": index,
        "chunkCount": count,
        "candles": [{
            "timeUtc": datetime(2026, 7, 29, 0, minute, tzinfo=UTC).isoformat(),
            "open": Decimal("1.1000") + Decimal(minute) / Decimal("10000"),
            "high": Decimal("1.1002") + Decimal(minute) / Decimal("10000"),
            "low": Decimal("1.0998") + Decimal(minute) / Decimal("10000"),
            "close": Decimal("1.1001") + Decimal(minute) / Decimal("10000"),
            "tickVolume": 10,
        }],
    })


def test_authoritative_refresh_buffers_all_chunks_before_replacement() -> None:
    buffer = AuthoritativeRefreshBuffer()
    assert buffer.add(request(1, 2, 0)) is None
    assembled = buffer.add(request(2, 2, 1))
    assert assembled is not None
    assert assembled.authoritative is True
    assert assembled.chunk_index == 1
    assert assembled.chunk_count == 1
    assert [item.time_utc.minute for item in assembled.candles] == [0, 1]


def test_authoritative_refresh_accepts_duplicate_chunk_delivery() -> None:
    buffer = AuthoritativeRefreshBuffer()
    assert buffer.add(request(1, 2, 0)) is None
    assert buffer.add(request(1, 2, 0)) is None
    assembled = buffer.add(request(2, 2, 1))
    assert assembled is not None
    assert len(assembled.candles) == 2


def test_v0672_metadata_and_authoritative_reset_source() -> None:
    assert __version__ == "0.67.4"
    assert 'version = "0.67.4"' in Path("pyproject.toml").read_text(encoding="utf-8")
    readme = Path("README.md").read_text(encoding="utf-8")
    assert "## Version 0.67.4" in readme
    source = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert "authoritative_refreshes.add(request)" in source
    assert '"skipped": 0' in source
    assert "replace_window=(context[\"from_utc\"], context[\"to_utc\"])" in source
