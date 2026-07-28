from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_repair_cannot_starve_heartbeat() -> None:
    bridge = (ROOT / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    timer = bridge[bridge.index("void OnTimer()") : bridge.index("string ResolveProviderSymbol") ]
    assert timer.index("SendHeartbeat();") < timer.index("RefreshRepairRequest()")
    assert "Keep provider liveness independent from long bulk uploads" in bridge
    assert "Axetos MT5 Bridge v1.34" in bridge
