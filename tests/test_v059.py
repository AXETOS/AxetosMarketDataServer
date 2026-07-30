from datetime import UTC, datetime
from axetos_market_data.full_history import FullHistoryBackfillManager
from history_test_helpers import complete_history_discovery

def test_direct_backfill_not_blocked_by_live_pressure():
    m=FullHistoryBackfillManager(lambda *_:0,pressure_probe=lambda:False,now_factory=lambda:datetime(2026,7,27,tzinfo=UTC)); m.start('P',[('EURUSD','EUR/USD')]); assert complete_history_discovery(m, 'P').startswith('BACKFILL|')
