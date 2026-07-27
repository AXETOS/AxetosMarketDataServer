from pathlib import Path


def test_bridge_retries_copyrates_history_sync_without_blocking():
    root = Path(__file__).resolve().parents[1]
    source = (root / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.24"' in source
    assert "g_pending_probe_request_id" in source
    assert "waiting for MT5 history sync" in source
    assert "availability result %s %s" in source
    assert "g_pending_probe_attempts < 10" in source
