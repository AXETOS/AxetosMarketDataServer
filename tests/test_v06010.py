from datetime import UTC, datetime, timedelta

from axetos_market_data.full_history import FullHistoryBackfillManager


def test_empty_hourly_tier_is_probed_once_then_advances_to_daily() -> None:
    now = datetime(2026, 7, 27, 22, 0, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_args: 0, now_factory=lambda: now)
    manager.start("ICMarkets.MT5", [("SOLUSD", "SOL/USD")])

    m1 = manager.next_request("ICMarkets.MT5").split("|")
    manager.availability_result("ICMarkets.MT5", m1[5], earliest=None, latest=None, count=0)

    h1 = manager.next_request("ICMarkets.MT5").split("|")
    assert h1[:3] == ["AVAILABILITY", "SOLUSD", "1h"]
    assert datetime.fromisoformat(h1[4]) - datetime.fromisoformat(h1[3]) > timedelta(days=1000)

    decision = manager.availability_result(
        "ICMarkets.MT5", h1[5], earliest=None, latest=None, count=0
    )
    assert decision == "UNAVAILABLE|0|0"

    d1 = manager.next_request("ICMarkets.MT5").split("|")
    assert d1[:3] == ["AVAILABILITY", "SOLUSD", "1d"]


def test_hourly_tier_with_history_drills_into_month_sized_ranges() -> None:
    now = datetime(2026, 7, 27, 22, 0, tzinfo=UTC)

    def local_count(_provider: str, _instrument: str, timeframe: str, start: datetime, end: datetime) -> int:
        if timeframe == "1m":
            return 0
        return 100 if end - start > timedelta(days=1000) else 10

    manager = FullHistoryBackfillManager(local_count, now_factory=lambda: now)
    manager.start("ICMarkets.MT5", [("SOLUSD", "SOL/USD")])
    m1 = manager.next_request("ICMarkets.MT5").split("|")
    manager.availability_result("ICMarkets.MT5", m1[5], earliest=None, latest=None, count=0)

    h1 = manager.next_request("ICMarkets.MT5").split("|")
    decision = manager.availability_result(
        "ICMarkets.MT5",
        h1[5],
        earliest=datetime.fromisoformat(h1[3]),
        latest=datetime.fromisoformat(h1[4]),
        count=5000,
    )
    assert decision == "DRILLDOWN|5000|100"

    monthly = manager.next_request("ICMarkets.MT5").split("|")
    assert monthly[:3] == ["AVAILABILITY", "SOLUSD", "1h"]
    span = datetime.fromisoformat(monthly[4]) - datetime.fromisoformat(monthly[3])
    assert timedelta(days=29) <= span <= timedelta(days=30)


def test_daily_tier_uses_one_broad_availability_probe() -> None:
    now = datetime(2026, 7, 27, 22, 0, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_args: 0, now_factory=lambda: now)
    manager.start("ICMarkets.MT5", [("SOLUSD", "SOL/USD")])

    m1 = manager.next_request("ICMarkets.MT5").split("|")
    manager.availability_result("ICMarkets.MT5", m1[5], earliest=None, latest=None, count=0)
    h1 = manager.next_request("ICMarkets.MT5").split("|")
    manager.availability_result("ICMarkets.MT5", h1[5], earliest=None, latest=None, count=0)

    d1 = manager.next_request("ICMarkets.MT5").split("|")
    assert d1[:3] == ["AVAILABILITY", "SOLUSD", "1d"]
    assert datetime.fromisoformat(d1[4]) - datetime.fromisoformat(d1[3]) > timedelta(days=365 * 50)
