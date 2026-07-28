from datetime import UTC, datetime
from pathlib import Path
from axetos_market_data.full_history import FullHistoryBackfillManager

def test_direct_download_acknowledgement() -> None:
    m=FullHistoryBackfillManager(lambda *_:0,now_factory=lambda:datetime(2026,7,27,tzinfo=UTC)); m.start('P',[('SOLUSD','SOL/USD')])
    req=m.next_request('P').split('|'); assert req[:3]==['BACKFILL','SOLUSD','1m']
    assert m.batch_result('P',req[5],1435,1,True)=='STORED|1435|1|1434'

def test_bridge_and_server_expose_explicit_storage_handshake() -> None:
    bridge=Path('bridges/mt5/Experts/AxetosMarketDataBridge.mq5').read_text(); manager=Path('src/axetos_market_data/full_history.py').read_text()
    assert 'CopyRates returned' in bridge and 'server acknowledged' in bridge
    assert 'return f"STORED|{received}|{inserted}|{skipped}"' in manager
