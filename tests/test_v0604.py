from datetime import UTC, datetime
from axetos_market_data.full_history import FullHistoryBackfillManager
from history_test_helpers import complete_history_discovery

def test_download_is_reported_after_availability_planning() -> None:
    m=FullHistoryBackfillManager(lambda *_:0,now_factory=lambda:datetime(2026,7,27,tzinfo=UTC)); m.start('P',[('SOLUSD','SOL/USD')])
    assert complete_history_discovery(m, 'P').startswith('BACKFILL|SOLUSD|1m|')

def test_local_counts_do_not_prevent_authoritative_source_download() -> None:
    m=FullHistoryBackfillManager(lambda *_:1000,now_factory=lambda:datetime(2026,7,27,tzinfo=UTC)); m.start('P',[('SOLUSD','SOL/USD')])
    assert complete_history_discovery(m, 'P').startswith('BACKFILL|')
