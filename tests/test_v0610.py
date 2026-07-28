from datetime import UTC, datetime
from axetos_market_data.full_history import FullHistoryBackfillManager

def test_simple_planner_starts_direct_download() -> None:
    m=FullHistoryBackfillManager(lambda *_:0,now_factory=lambda:datetime(2026,7,28,12,tzinfo=UTC)); m.start('P',[('EURUSD','EUR/USD')])
    assert m.next_request('P').startswith('BACKFILL|EURUSD|1m|')

def test_unavailable_range_is_marked_bad_and_next_range_continues() -> None:
    bad=[]; m=FullHistoryBackfillManager(lambda *_:0,bad_range_recorder=lambda *args: bad.append(args),now_factory=lambda:datetime(2026,7,28,12,tzinfo=UTC)); m.start('P',[('EURUSD','EUR/USD')])
    req=m.next_request('P').split('|'); m.batch_result('P',req[5],0,0,False,unavailable=True,error_code=4401)
    assert bad and m.next_request('P').startswith('BACKFILL|')
