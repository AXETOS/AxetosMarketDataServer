from datetime import UTC, datetime
from pathlib import Path
from axetos_market_data.full_history import FullHistoryBackfillManager

def test_unavailable_download_advances():
    m=FullHistoryBackfillManager(lambda *_:0,now_factory=lambda:datetime(2026,7,27,tzinfo=UTC)); m.start('P',[('SOLUSD','SOL/USD')])
    first=m.next_request('P'); ack=m.batch_result('P',first.split('|')[-1],0,0,True,unavailable=True); assert ack.startswith('UNAVAILABLE|')
    assert m.next_request('P').startswith('BACKFILL|')

def test_bridge_direct_backfill_support():
    source=Path('bridges/mt5/Experts/AxetosMarketDataBridge.mq5').read_text(); assert 'command != "BACKFILL"' in source and 'CopyRates returned' in source
