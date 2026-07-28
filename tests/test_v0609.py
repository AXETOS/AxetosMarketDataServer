from pathlib import Path

def test_bridge_limits_history_sync_probe_to_three_attempts() -> None:
    source = Path('bridges/mt5/Experts/AxetosMarketDataBridge.mq5').read_text(encoding='utf-8')
    assert '#property version   "3.01"' in source
    assert 'g_pending_probe_attempts' not in source
    assert 'CopyRates' in source
    assert 'ReportAvailability' in source
