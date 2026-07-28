from datetime import UTC, datetime, timedelta

from axetos_market_data.full_history import FullHistoryBackfillManager


def _finish_discovery(manager: FullHistoryBackfillManager, provider: str, now: datetime) -> None:
    first = manager.next_request(provider).split("|")
    manager.availability_result(
        provider, first[5], earliest=now - timedelta(hours=2), latest=now, count=121
    )
    for _ in ("1h", "1d"):
        request = manager.next_request(provider).split("|")
        manager.availability_result(provider, request[5], earliest=None, latest=None, count=0)


def test_planning_decisions_are_reported_before_any_download() -> None:
    now = datetime(2026, 7, 27, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *args: 0, now_factory=lambda: now)
    manager.start("P", [("SOLUSD", "SOL/USD")])
    _finish_discovery(manager, "P", now)

    decisions: list[str] = []
    for _ in range(20):
        request = manager.next_request("P")
        if request.startswith("BACKFILL|"):
            break
        parts = request.split("|")
        assert parts[:3] == ["AVAILABILITY", "SOLUSD", "1m"]
        decisions.append(manager.availability_result(
            "P", parts[5], earliest=datetime.fromisoformat(parts[3]),
            latest=datetime.fromisoformat(parts[4]), count=1,
        ))
    else:
        raise AssertionError("planner did not reach the download phase")

    assert any(value.startswith("PLAN_SPLIT|") for value in decisions)
    assert any(value.startswith("PLAN_MISSING|") for value in decisions)
    assert request.startswith("BACKFILL|SOLUSD|1m|")
    assert manager.next_request("P") == ""


def test_complete_discovery_skips_all_range_checks() -> None:
    now = datetime(2026, 7, 27, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *args: 1000, now_factory=lambda: now)
    manager.start("P", [("SOLUSD", "SOL/USD")])
    for timeframe, days in (("1m", 30), ("1h", 700), ("1d", 2500)):
        request = manager.next_request("P").split("|")
        assert request[:3] == ["DISCOVER", "SOLUSD", timeframe]
        decision = manager.availability_result(
            "P", request[5], earliest=now - timedelta(days=days), latest=now, count=1000
        )
        assert decision.startswith("DISCOVERED_COMPLETE|")
    assert manager.next_request("P") == ""
