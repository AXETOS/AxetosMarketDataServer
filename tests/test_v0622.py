from pathlib import Path

from axetos_market_data import __version__


def test_v0622_metadata_and_bridge_priority() -> None:
    root = Path(__file__).resolve().parents[1]
    bridge = (root / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    readme = (root / "README.md").read_text(encoding="utf-8")
    assert __version__ == "1.0.0"
    assert '#property version   "3.06"' in bridge
    timer = bridge[bridge.index("void OnTimer()"):bridge.index("void SendHeartbeat")]
    assert timer.index("SendHeartbeat();") < timer.index("PollCommand();")
    assert readme.index("## Version 1.0.0") < readme.index("## Version 0.62.2")
