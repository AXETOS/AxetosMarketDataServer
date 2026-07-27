from datetime import datetime, timedelta, timezone
from pathlib import Path

from axetos_market_data.full_history import FullHistoryBackfillManager

UTC = timezone.utc


def test_history_request_is_delivered_once_until_acknowledged() -> None:
    now = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_: 0, now_factory=lambda: now)
    manager.start("ICMarkets.MT5", [("EURUSD", "EUR/USD")])
    first = manager.next_request("ICMarkets.MT5")
    assert first.startswith("AVAILABILITY|")
    assert manager.next_request("ICMarkets.MT5") == ""


def test_history_request_redelivers_after_lease_expiry() -> None:
    current = [datetime(2026, 7, 27, 12, 0, tzinfo=UTC)]
    manager = FullHistoryBackfillManager(
        lambda *_: 0,
        now_factory=lambda: current[0],
        request_lease=timedelta(seconds=30),
    )
    manager.start("ICMarkets.MT5", [("EURUSD", "EUR/USD")])
    first = manager.next_request("ICMarkets.MT5")
    assert manager.next_request("ICMarkets.MT5") == ""
    current[0] += timedelta(seconds=31)
    assert manager.next_request("ICMarkets.MT5") == first


def test_bridge_reports_stored_batch_acknowledgement() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.22"' in source
    assert "backfill batch stored for %s %s" in source
    assert "server acknowledged" in source
