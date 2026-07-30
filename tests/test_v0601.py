from datetime import UTC, datetime
from axetos_market_data.full_history import FullHistoryBackfillManager
from history_test_helpers import complete_history_discovery

def test_backfill_request_stays_in_flight_until_ack():
    m=FullHistoryBackfillManager(lambda *_:0,now_factory=lambda:datetime(2026,7,27,tzinfo=UTC)); m.start('P',[('EURUSD','EUR/USD')]); first=complete_history_discovery(m, 'P'); assert first.startswith('BACKFILL|'); assert m.next_request('P')==''
