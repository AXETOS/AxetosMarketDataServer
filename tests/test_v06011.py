from datetime import UTC, datetime, timedelta
from pathlib import Path

from axetos_market_data.full_history import FullHistoryBackfillManager


def _answer(manager, provider, request, earliest, count):
    parts = request.split('|')
    latest = datetime.fromisoformat(parts[4]) if earliest is not None else None
    return manager.availability_result(provider, parts[5], earliest=earliest, latest=latest, count=count)


def test_discovers_all_three_boundaries_before_planning_downloads() -> None:
    now = datetime(2026, 7, 27, 23, 0, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_: 0, now_factory=lambda: now)
    manager.start('ICMarkets.MT5', [('SOLUSD', 'SOL/USD')])

    m1 = manager.next_request('ICMarkets.MT5')
    p = m1.split('|')
    assert p[:3] == ['DISCOVER', 'SOLUSD', '1m']
    assert datetime.fromisoformat(p[4]) - datetime.fromisoformat(p[3]) >= timedelta(days=3650)
    m1_boundary = now - timedelta(days=40)
    assert _answer(manager, 'ICMarkets.MT5', m1, m1_boundary, 50000).startswith('DISCOVERED_MISSING|50000|0|')

    h1 = manager.next_request('ICMarkets.MT5')
    assert h1.split('|')[:3] == ['DISCOVER', 'SOLUSD', '1h']
    h1_boundary = now - timedelta(days=800)
    _answer(manager, 'ICMarkets.MT5', h1, h1_boundary, 19000)

    d1 = manager.next_request('ICMarkets.MT5')
    assert d1.split('|')[:3] == ['DISCOVER', 'SOLUSD', '1d']
    d1_boundary = now - timedelta(days=3000)
    _answer(manager, 'ICMarkets.MT5', d1, d1_boundary, 3000)

    first_download_probe = manager.next_request('ICMarkets.MT5').split('|')
    assert first_download_probe[:3] == ['AVAILABILITY', 'SOLUSD', '1m']
    assert datetime.fromisoformat(first_download_probe[3]) == m1_boundary

    status = manager.status('ICMarkets.MT5')['jobs'][0]['instruments'][0]
    assert status['discovery_complete'] is True
    assert status['discovery_boundaries']['1m'] == m1_boundary.isoformat()
    assert status['discovery_boundaries']['1h'] == h1_boundary.isoformat()
    assert status['discovery_boundaries']['1d'] == d1_boundary.isoformat()


def test_unavailable_timeframe_is_omitted_from_plan() -> None:
    now = datetime(2026, 7, 27, 23, 0, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_: 0, now_factory=lambda: now)
    manager.start('ICMarkets.MT5', [('SOLUSD', 'SOL/USD')])
    m1 = manager.next_request('ICMarkets.MT5')
    _answer(manager, 'ICMarkets.MT5', m1, None, 0)
    h1 = manager.next_request('ICMarkets.MT5')
    h1_boundary = now - timedelta(days=700)
    _answer(manager, 'ICMarkets.MT5', h1, h1_boundary, 15000)
    d1 = manager.next_request('ICMarkets.MT5')
    _answer(manager, 'ICMarkets.MT5', d1, now - timedelta(days=2500), 2500)

    request = manager.next_request('ICMarkets.MT5').split('|')
    assert request[:3] == ['AVAILABILITY', 'SOLUSD', '1h']
    assert datetime.fromisoformat(request[3]) == h1_boundary


def test_bridge_v126_understands_discovery_command_and_logs_boundaries() -> None:
    source = Path('bridges/mt5/Experts/AxetosMarketDataBridge.mq5').read_text(encoding='utf-8')
    assert '#property version   "1.31"' in source
    assert 'command == "AVAILABILITY" || command == "DISCOVER"' in source
    assert 'discovering available %s history' in source
    assert 'history discovery for %s completed' in source
