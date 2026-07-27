from datetime import UTC, datetime, timedelta
from axetos_market_data.full_history import FullHistoryBackfillManager


def test_each_timeframe_gets_one_ten_year_discovery_probe() -> None:
    now = datetime(2026, 7, 27, 22, 0, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_: 0, now_factory=lambda: now)
    manager.start('ICMarkets.MT5', [('SOLUSD', 'SOL/USD')])
    for timeframe in ('1m', '1h', '1d'):
        parts = manager.next_request('ICMarkets.MT5').split('|')
        assert parts[:3] == ['DISCOVER', 'SOLUSD', timeframe]
        assert datetime.fromisoformat(parts[4]) - datetime.fromisoformat(parts[3]) >= timedelta(days=3650)
        manager.availability_result('ICMarkets.MT5', parts[5], earliest=None, latest=None, count=0)
    assert manager.next_request('ICMarkets.MT5') == ''
