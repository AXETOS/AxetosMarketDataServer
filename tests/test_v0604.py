from datetime import UTC, datetime, timedelta
from axetos_market_data.full_history import FullHistoryBackfillManager


def _discover(manager, provider, now):
    boundaries = {'1m': now - timedelta(days=30), '1h': now - timedelta(days=700), '1d': now - timedelta(days=2500)}
    for tf in ('1m','1h','1d'):
        req = manager.next_request(provider).split('|')
        manager.availability_result(provider, req[5], earliest=boundaries[tf], latest=now, count=1000)


def test_availability_decision_is_reported_before_next_range():
    now=datetime(2026,7,27,tzinfo=UTC)
    manager=FullHistoryBackfillManager(lambda *args: 0, now_factory=lambda: now)
    manager.start('P', [('SOLUSD','SOL/USD')])
    _discover(manager,'P',now)
    req=manager.next_request('P').split('|')
    decision=manager.availability_result('P', req[5], earliest=datetime.fromisoformat(req[3]), latest=datetime.fromisoformat(req[4]), count=1436)
    assert decision=='BACKFILL|1436|0'
    assert manager.next_request('P').startswith('BACKFILL|SOLUSD|1m|')
    assert manager.next_request('P')==''


def test_availability_skip_is_explicit():
    now=datetime(2026,7,27,tzinfo=UTC)
    manager=FullHistoryBackfillManager(lambda *args: 1436, now_factory=lambda: now)
    manager.start('P', [('SOLUSD','SOL/USD')])
    _discover(manager,'P',now)
    req=manager.next_request('P').split('|')
    decision=manager.availability_result('P', req[5], earliest=datetime.fromisoformat(req[3]), latest=datetime.fromisoformat(req[4]), count=1436)
    assert decision=='SKIP|1436|1436'
