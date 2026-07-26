from pathlib import Path


def test_bridge_has_no_local_symbol_subscription_or_higher_timeframe_backfill() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.16"' in source
    assert "InpSymbols" not in source
    assert "InpUseServerSelection" not in source
    assert 'g_intervals[1] = { "1m" }' in source
    assert 'PERIOD_D1' not in source
    assert 'PERIOD_H1' not in source
    assert 'PERIOD_M15' not in source
    assert '&interval=1m' in source
