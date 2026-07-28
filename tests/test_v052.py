from pathlib import Path


def test_bridge_has_server_controlled_symbols_and_tiered_history_timeframes() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.31"' in source
    assert "InpSymbols" not in source
    assert "InpUseServerSelection" not in source
    assert 'g_intervals[1] = { "1m" }' in source
    assert 'return PERIOD_M1' in source
    assert 'return PERIOD_H1' in source
    assert 'return PERIOD_D1' in source
    assert '"&interval=" + interval' in source
