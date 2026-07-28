from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_bridge_channels_are_independent() -> None:
    source = (ROOT / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.34"' in source
    assert "g_http_retry_after" not in source
    assert "g_http_suppressed_requests" not in source
    assert "HttpAttemptAllowed" not in source
    assert "HttpChannelForPath" in source
    assert "HttpTimeoutForPath" in source
    assert "other channels continue" in source
    assert "InpControlTimeoutMs" in source
    assert "InpLiveTimeoutMs" in source


def test_release_metadata() -> None:
    from axetos_market_data import __version__
    assert __version__ == "0.64.1"
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert readme.index("## Version 0.64.1") < readme.index("## Version 0.62.2")
