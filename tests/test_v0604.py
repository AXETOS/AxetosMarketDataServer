from datetime import UTC, datetime
from axetos_market_data.full_history import FullHistoryBackfillManager

def test_direct_download_is_reported_without_planning_probes() -> None:
    m=FullHistoryBackfillManager(lambda *_:0,now_factory=lambda:datetime(2026,7,27,tzinfo=UTC)); m.start('P',[('SOLUSD','SOL/USD')])
    assert m.next_request('P').startswith('BACKFILL|SOLUSD|1m|')

def test_local_counts_do_not_prevent_authoritative_source_download() -> None:
    m=FullHistoryBackfillManager(lambda *_:1000,now_factory=lambda:datetime(2026,7,27,tzinfo=UTC)); m.start('P',[('SOLUSD','SOL/USD')])
    assert m.next_request('P').startswith('BACKFILL|')
