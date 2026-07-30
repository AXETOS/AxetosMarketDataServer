from pathlib import Path


def test_bridge_retries_copyrates_history_sync_without_blocking() -> None:
    root = Path(__file__).resolve().parents[1]
    source = (root / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "3.05"' in source
    assert "g_pending_probe_request_id" not in source
    assert "waiting for MT5 history sync" not in source
    assert "CopyRates" in source
    assert "ReportAvailability" in source
