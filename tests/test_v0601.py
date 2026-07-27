from datetime import UTC, datetime, timedelta
from axetos_market_data.full_history import FullHistoryBackfillManager


def _complete_discovery(manager, provider, now):
    for tf, days in [('1m',30),('1h',700),('1d',2500)]:
        req=manager.next_request(provider).split('|')
        manager.availability_result(provider, req[5], earliest=now-timedelta(days=days), latest=now, count=1000)


def test_discovery_probe_is_not_blocked_by_live_queue_pressure() -> None:
    now=datetime(2026,7,27,tzinfo=UTC)
    manager=FullHistoryBackfillManager(lambda *_:0, pressure_probe=lambda:False, now_factory=lambda:now)
    manager.start('ICMarkets.MT5',[('EURUSD','EUR/USD')])
    assert manager.next_request('ICMarkets.MT5').startswith('DISCOVER|EURUSD|1m|')


def test_backfill_command_dispatch_is_not_stranded_by_live_queue_pressure() -> None:
    now=datetime(2026,7,27,tzinfo=UTC)
    manager=FullHistoryBackfillManager(lambda *_:0, pressure_probe=lambda:False, now_factory=lambda:now)
    manager.start('ICMarkets.MT5',[('EURUSD','EUR/USD')])
    _complete_discovery(manager,'ICMarkets.MT5',now)
    probe=manager.next_request('ICMarkets.MT5').split('|')
    manager.availability_result('ICMarkets.MT5',probe[5],earliest=datetime.fromisoformat(probe[3]),latest=datetime.fromisoformat(probe[4]),count=1440)
    assert manager.next_request('ICMarkets.MT5').startswith('BACKFILL|EURUSD|1m|')
    assert manager.next_request('ICMarkets.MT5')==''
