from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_completed_m1_delay_uses_supported_mql5_time_api() -> None:
    source = (ROOT / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.30"' in source
    assert "TimeSeconds(now)" not in source
    assert "MqlDateTime now_parts;" in source
    assert "TimeToStruct(now, now_parts);" in source
    assert "if(now_parts.sec < delay)" in source
