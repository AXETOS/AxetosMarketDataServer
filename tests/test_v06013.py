from datetime import UTC, datetime
from axetos_market_data.full_history import FullHistoryBackfillManager

def test_no_discovery_or_planning_probe_is_required():
    m=FullHistoryBackfillManager(lambda *_:0,now_factory=lambda:datetime(2026,7,28,tzinfo=UTC)); m.start('P',[('BTCUSD','BTC/USD')]); assert m.next_request('P').startswith('BACKFILL|')
