from pathlib import Path

from axetos_market_data import __version__


def test_v0622_metadata_and_bridge_priority() -> None:
    root = Path(__file__).resolve().parents[1]
    bridge = (root / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    readme = (root / "README.md").read_text(encoding="utf-8")
    assert __version__ == "0.66.0"
    assert '#property version   "2.00"' in bridge
    timer = bridge[bridge.index("void OnTimer()"):bridge.index("string ResolveProviderSymbol") ]
    assert timer.index("SendHeartbeat()") < timer.index("RefreshRepairRequest()")
    assert timer.index("RefreshRepairRequest()") < timer.index("SendPreviousCompletedM1()")
    assert "if(RefreshRepairRequest())\n      return;" in timer
    assert 'path == "/api/market-data/ingest/mt5/heartbeat"' in bridge
    assert 'StringFind(path, "/api/market-data/mt5/repair-request.txt") == 0' in bridge
    assert readme.startswith("# Axetos Market Data Server")
    assert readme.index("## Version 0.66.0") < readme.index("## Version 0.62.2")
