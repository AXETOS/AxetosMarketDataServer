from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_server_command_loop_keeps_heartbeat_first() -> None:
    bridge = (ROOT / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    timer = bridge[bridge.index("void OnTimer()") : bridge.index("void SendHeartbeat")]
    assert timer.index("SendHeartbeat();") < timer.index("PollCommand();")
    assert 'Axetos MT5 Bridge v3.06' in bridge
