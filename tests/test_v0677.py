from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_mt5_copyrates_uses_server_utc_timestamp_without_broker_offset():
    source = (ROOT / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "3.06"' in source
    assert "return StringToTime(normalized);" in source
    assert "UtcToBroker" not in source
    assert "BrokerToUtc" not in source
    assert "BrokerUtcOffsetSeconds" not in source
    assert 'IsoUtc(rates[i].time)' in source


def test_bridge_documents_utc_storage_invariant():
    source = (ROOT / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert "database remain UTC" in source
    assert "Never add the broker offset" in source
