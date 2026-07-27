from __future__ import annotations

import inspect
import time
from datetime import datetime, timezone
from decimal import Decimal

from axetos_market_data.bridge import BridgeCandle, BridgeCandlesRequest, Mt5BridgeService
from axetos_market_data.storage import MarketDataStore


def _history_request() -> BridgeCandlesRequest:
    return BridgeCandlesRequest(
        provider_key="ICMarkets.MT5",
        terminal_instance_id="terminal-1",
        provider_symbol="SOLUSD",
        canonical_instrument="SOL/USD",
        interval="1m",
        candles=[
            BridgeCandle(
                time_utc=datetime(2026, 7, 27, 0, 0, tzinfo=timezone.utc),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.5"),
                tick_volume=10,
            )
        ],
        request_id="repair-1434-1433",
    )


def test_dispatched_history_post_does_not_wait_for_live_queue_pressure(tmp_path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    bridge = Mt5BridgeService(store)
    try:
        # Reproduce the old starvation condition deterministically. The completed
        # dispatch must be stored even when the live-pressure probe says "busy".
        bridge.can_run_background_write = lambda: False  # type: ignore[method-assign]

        started = time.monotonic()
        stored = bridge.candles(_history_request())
        elapsed = time.monotonic() - started

        assert stored == 1
        assert elapsed < 1.0
    finally:
        bridge.shutdown()


def test_root_cause_removed_from_synchronous_upload_path() -> None:
    source = inspect.getsource(Mt5BridgeService._insert_historical_low_priority)
    assert "live queue" in source
    assert "while not self.can_run_background_write()" not in source
