from pathlib import Path

def test_bridge_limits_history_sync_probe_to_three_attempts() -> None:
    source = Path('bridges/mt5/Experts/AxetosMarketDataBridge.mq5').read_text(encoding='utf-8')
    assert '#property version   "1.29"' in source
    assert 'g_pending_probe_attempts < 3' in source
    assert 'attempt %d/3' in source
    assert 'attempt %d/10' not in source
