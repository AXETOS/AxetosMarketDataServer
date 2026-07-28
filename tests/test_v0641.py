from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_server_command_loop_keeps_heartbeat_first() -> None:
    bridge = (ROOT / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    timer = bridge[bridge.index("void OnTimer()") : bridge.index("bool ExecuteServerCommand")]
    assert timer.index("SendHeartbeat();") < timer.index("ExecuteServerCommand();")
    assert 'Axetos MT5 Bridge v2.00' in bridge
