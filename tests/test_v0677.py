from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def test_mt5_utc_commands_are_converted_to_broker_clock():
    source = (ROOT / "bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "3.02"' in source
    assert "return UtcToBroker(utc_value);" in source
    assert "datetime UtcToBroker(datetime value)" in source
    assert "datetime BrokerToUtc(datetime value)" in source
    assert "BrokerUtcOffsetSeconds()" in source
