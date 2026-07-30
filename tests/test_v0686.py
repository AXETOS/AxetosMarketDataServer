from datetime import UTC, datetime

from axetos_market_data.full_history import FullHistoryBackfillManager


def _answer_availability(manager: FullHistoryBackfillManager, provider: str, values: dict[str, tuple[datetime | None, datetime | None, int]]) -> None:
    for expected_timeframe in ("1m", "1h", "1d"):
        command = manager.next_request(provider)
        action, _symbol, timeframe, _start, _end, request_id = command.split("|")
        assert action == "DISCOVER"
        assert timeframe == expected_timeframe
        earliest, latest, count = values[timeframe]
        manager.availability_result(provider, request_id, earliest=earliest, latest=latest, count=count)


def test_full_history_checks_availability_before_building_source_plan() -> None:
    events: list[tuple[str, str, str, dict[str, object]]] = []
    manager = FullHistoryBackfillManager(
        lambda *_: 0,
        on_event=lambda category, message, provider, details: events.append(
            (category, message, provider, details)
        ),
        now_factory=lambda: datetime(2026, 7, 30, 9, 0, tzinfo=UTC),
    )

    manager.start("ICMarkets.MT5", [("SOLUSD", "SOL/USD")])
    assert not any(event[0] == "backfill.instrument_plan_created" for event in events)

    _answer_availability(manager, "ICMarkets.MT5", {
        "1m": (datetime(2024, 3, 18, tzinfo=UTC), datetime(2026, 7, 30, 8, 59, tzinfo=UTC), 1_000_000),
        "1h": (datetime(2021, 11, 2, tzinfo=UTC), datetime(2026, 7, 30, 8, 0, tzinfo=UTC), 40_000),
        "1d": (datetime(2020, 9, 15, tzinfo=UTC), datetime(2026, 7, 29, tzinfo=UTC), 2_000),
    })

    availability = next(event for event in events if event[0] == "backfill.instrument_availability_checked")
    assert availability[3]["earliest_by_timeframe"]["1m"].startswith("2024-03-18")
    plan = next(event for event in events if event[0] == "backfill.instrument_plan_created")
    planned = plan[3]["planned_ranges"]
    assert 28 <= planned["1m"] <= 30
    assert planned["1h"] == 6
    assert planned["1d"] == 1

    first_download = manager.next_request("ICMarkets.MT5")
    action, symbol, timeframe, start, _end, _request_id = first_download.split("|")
    assert action == "BACKFILL"
    assert symbol == "SOLUSD"
    assert timeframe == "1m"
    assert start.startswith("2024-03-18")


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

    _answer_availability(manager, "ICMarkets.MT5", {
        "1m": (None, None, 0),
        "1h": (None, None, 0),
        "1d": (None, None, 0),
    })
    assert manager.next_request("ICMarkets.MT5") == ""
    assert repaired == []
    assert any(event[0] == "backfill.instrument_failed" for event in events)
