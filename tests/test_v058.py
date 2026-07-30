from datetime import UTC, datetime
from pathlib import Path
from axetos_market_data.full_history import FullHistoryBackfillManager

def test_unavailable_discovery_advances_to_next_timeframe():
    m=FullHistoryBackfillManager(lambda *_:0,now_factory=lambda:datetime(2026,7,27,tzinfo=UTC)); m.start('P',[('SOLUSD','SOL/USD')])
    first=m.next_request('P').split('|')
    assert first[:3] == ['DISCOVER','SOLUSD','1m']
    ack=m.availability_result('P', first[5], earliest=None, latest=None, count=0)
    assert ack == 'UNAVAILABLE|0|0'
    assert m.next_request('P').startswith('DISCOVER|SOLUSD|1h|')

def test_bridge_direct_backfill_support():
    source=Path('bridges/mt5/Experts/AxetosMarketDataBridge.mq5').read_text(); assert 'action != "FETCH" && action != "BACKFILL"' in source and 'CopyRates' in source
