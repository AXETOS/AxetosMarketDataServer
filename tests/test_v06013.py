from datetime import UTC, datetime
from axetos_market_data.full_history import FullHistoryBackfillManager
from history_test_helpers import complete_history_discovery

def test_availability_discovery_precedes_planned_download():
    m=FullHistoryBackfillManager(lambda *_:0,now_factory=lambda:datetime(2026,7,28,tzinfo=UTC)); m.start('P',[('BTCUSD','BTC/USD')]); assert complete_history_discovery(m, 'P').startswith('BACKFILL|')
