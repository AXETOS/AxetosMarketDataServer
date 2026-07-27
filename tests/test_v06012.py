from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from queue import Queue
from threading import RLock

from axetos_market_data.bridge import BridgeTick, BridgeTicksRequest, Mt5BridgeService, QueueStats


def _request(second: int) -> BridgeTicksRequest:
    return BridgeTicksRequest(
        provider_key="ICMarkets.MT5",
        terminal_instance_id="terminal-1",
        ticks=[BridgeTick(
            provider_symbol="EURUSD",
            canonical_instrument="EUR/USD",
            time_utc=datetime(2026, 7, 28, 0, 0, second, tzinfo=UTC),
            bid=Decimal("1.10000") + Decimal(second) / Decimal("100000"),
            ask=Decimal("1.10002") + Decimal(second) / Decimal("100000"),
        )],
    )


def test_full_live_queue_discards_oldest_snapshot_and_accepts_newest() -> None:
    bridge = Mt5BridgeService.__new__(Mt5BridgeService)
    bridge._queue = Queue(maxsize=1)
    bridge._queue_lock = RLock()
    bridge._stopping = False
    bridge.stats = QueueStats()
    old = _request(1)
    newest = _request(2)
    bridge._queue.put_nowait(old)

    assert bridge.enqueue_ticks(newest) == 1
    retained = bridge._queue.get_nowait()
    assert retained.ticks[0].time_utc == newest.ticks[0].time_utc
    assert bridge.stats.coalesced_batches == 1
    assert bridge.stats.dropped_batches == 1
    assert bridge.stats.dropped_ticks == 1


def test_bridge_v127_treats_tick_backpressure_separately() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.27"' in source
    assert 'IsTickBackpressure' in source
    assert 'live ingestion congested; pausing tick submissions for 5s' in source
    assert 'live ingestion resumed' in source
    assert 'g_tick_suppressed_batches++' in source
