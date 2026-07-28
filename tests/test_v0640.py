from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from axetos_market_data.domain import Candle
from axetos_market_data.full_history import FullHistoryBackfillManager
from axetos_market_data.hierarchical_repair import HierarchicalCandleRepair
from axetos_market_data.history_worker import _compress_flat_m1


def test_simple_history_starts_with_direct_m1_backfill() -> None:
    now = datetime(2026, 7, 28, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_: 0, now_factory=lambda: now)
    manager.start("P", [("EURUSD", "EUR/USD")])
    request = manager.next_request("P").split("|")
    assert request[:3] == ["BACKFILL", "EURUSD", "1m"]
    assert datetime.fromisoformat(request[4]) <= now
    assert manager.next_request("P") == ""


def test_simple_plan_contains_m1_h1_and_d1() -> None:
    now = datetime(2026, 7, 28, tzinfo=UTC)
    ranges = FullHistoryBackfillManager._simple_source_ranges(now)
    assert {value.timeframe for value in ranges} == {"1m", "1h", "1d"}
    assert len([value for value in ranges if value.timeframe == "1m"]) >= 120
    assert len([value for value in ranges if value.timeframe == "1h"]) >= 10
    assert len([value for value in ranges if value.timeframe == "1d"]) == 1


def _candle(when: datetime, value: str, *, flat: bool = True) -> Candle:
    price = Decimal(value)
    return Candle("P", "EUR/USD", "1m", when, price, price if flat else price + Decimal("1"), price, price, 1, None, True)


def test_repeated_flat_minutes_collapse_to_last_boundary() -> None:
    start = datetime(2026, 7, 25, tzinfo=UTC)
    pending: dict[tuple[str, str], Candle] = {}
    assert _compress_flat_m1([_candle(start, "1"), _candle(start + timedelta(minutes=1), "1")], pending) == []
    output = _compress_flat_m1([_candle(start + timedelta(minutes=2), "2", flat=False)], pending)
    assert [value.open_time for value in output] == [start + timedelta(minutes=1), start + timedelta(minutes=2)]


def test_repair_stages_are_only_required_hierarchy() -> None:
    assert HierarchicalCandleRepair.STAGES == (
        ("1m", "15m", 520),
        ("1m", "1h", 500),
        ("1h", "1d", 450),
        ("1d", "1w", 400),
        ("1d", "1mo", 400),
    )


def test_bridge_polls_two_completed_minutes() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "3.00"' in source
    assert "newest_completed - 60" not in source
    assert "InpCompletedM1DelaySeconds" not in source
    assert "PollCommand" in source
