from datetime import UTC, datetime, timedelta

from axetos_market_data.full_history import FullHistoryBackfillManager


def test_discovery_marks_complete_timeframe_without_range_checks() -> None:
    now = datetime(2026, 7, 28, tzinfo=UTC)
    boundary = now - timedelta(days=365)

    def count(_provider, _instrument, _timeframe, _start, _end):
        return 10_000

    def bounds(_provider, _instrument, _timeframe, _start, _end):
        return boundary, now

    manager = FullHistoryBackfillManager(count, bounds, now_factory=lambda: now)
    manager.start("ICMarkets.MT5", [("BTCUSD", "BTC/USD")])
    for timeframe in ("1m", "1h", "1d"):
        request = manager.next_request("ICMarkets.MT5")
        parts = request.split("|")
        assert parts[:3] == ["DISCOVER", "BTCUSD", timeframe]
        decision = manager.availability_result(
            "ICMarkets.MT5", parts[5], earliest=boundary, latest=now, count=10_000
        )
        assert decision.startswith("DISCOVERED_COMPLETE|10000|10000|")

    assert manager.next_request("ICMarkets.MT5") == ""
    job = manager.status("ICMarkets.MT5")["jobs"][0]
    item = job["instruments"][0]
    assert item["discovery_complete_timeframes"] == ["1d", "1h", "1m"]
    assert item["ranges_probed"] == 3
    assert item["batches_completed"] == 0


def test_incomplete_discovery_still_builds_targeted_range_plan() -> None:
    now = datetime(2026, 7, 28, tzinfo=UTC)
    boundary = now - timedelta(days=30)
    manager = FullHistoryBackfillManager(
        lambda *_: 90,
        lambda *_: (boundary, now),
        now_factory=lambda: now,
    )
    manager.start("ICMarkets.MT5", [("SOLUSD", "SOL/USD")])

    first = manager.next_request("ICMarkets.MT5").split("|")
    decision = manager.availability_result(
        "ICMarkets.MT5", first[5], earliest=boundary, latest=now, count=100
    )
    assert decision.startswith("DISCOVERED_MISSING|100|90|")

    for timeframe in ("1h", "1d"):
        request = manager.next_request("ICMarkets.MT5").split("|")
        manager.availability_result(
            "ICMarkets.MT5", request[5], earliest=None, latest=None, count=0
        )

    planned = manager.next_request("ICMarkets.MT5")
    assert planned.startswith("AVAILABILITY|SOLUSD|1m|")
