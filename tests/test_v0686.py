from datetime import UTC, datetime

from axetos_market_data.full_history import FullHistoryBackfillManager


def test_full_history_builds_explicit_ten_year_source_plan_and_logs_it() -> None:
    events: list[tuple[str, str, str, dict[str, object]]] = []
    manager = FullHistoryBackfillManager(
        lambda *_: 0,
        on_event=lambda category, message, provider, details: events.append(
            (category, message, provider, details)
        ),
        now_factory=lambda: datetime(2026, 7, 30, 9, 0, tzinfo=UTC),
    )

    manager.start("ICMarkets.MT5", [("GBPUSD", "GBP/USD")])

    plan = next(event for event in events if event[0] == "backfill.instrument_plan_created")
    assert plan[2] == "ICMarkets.MT5"
    assert plan[3]["provider_symbol"] == "GBPUSD"
    planned = plan[3]["planned_ranges"]
    assert planned["1m"] >= 120
    assert planned["1h"] >= 10
    assert planned["1d"] == 1


def test_empty_all_timeframes_is_failure_and_skips_repair() -> None:
    events: list[tuple[str, str, str, dict[str, object]]] = []
    repaired: list[str] = []
    manager = FullHistoryBackfillManager(
        lambda *_: 0,
        on_instrument_completed=lambda provider, instrument, details: repaired.append(instrument),
        on_event=lambda category, message, provider, details: events.append(
            (category, message, provider, details)
        ),
        now_factory=lambda: datetime(2026, 7, 30, 9, 0, tzinfo=UTC),
    )
    manager.start("ICMarkets.MT5", [("GBPUSD", "GBP/USD")])

    while True:
        command = manager.next_request("ICMarkets.MT5")
        if not command:
            break
        action, _symbol, _timeframe, _start, _end, request_id = command.split("|")
        assert action == "BACKFILL"
        manager.batch_result(
            "ICMarkets.MT5", request_id, 0, 0, True, unavailable=True
        )

    assert repaired == []
    failed = [event for event in events if event[0] == "backfill.instrument_failed"]
    assert len(failed) == 1
    summary = failed[0][3]["timeframes"]
    assert summary["1m"]["attempted"] == summary["1m"]["planned"]
    assert summary["1h"]["attempted"] == summary["1h"]["planned"]
    assert summary["1d"]["attempted"] == 1
    assert summary["1m"]["unavailable"] == summary["1m"]["attempted"]
