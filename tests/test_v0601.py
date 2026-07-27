from datetime import UTC, datetime

from axetos_market_data.full_history import FullHistoryBackfillManager


def test_availability_probe_is_not_blocked_by_live_queue_pressure() -> None:
    manager = FullHistoryBackfillManager(
        lambda *_args: 0,
        pressure_probe=lambda: False,
        now_factory=lambda: datetime(2026, 7, 27, tzinfo=UTC),
    )
    manager.start("ICMarkets.MT5", [("EURUSD", "EUR/USD")])

    request = manager.next_request("ICMarkets.MT5")

    assert request.startswith("AVAILABILITY|EURUSD|1m|")


def test_backfill_command_dispatch_is_not_stranded_by_live_queue_pressure() -> None:
    pressure = {"allow": True}
    manager = FullHistoryBackfillManager(
        lambda *_args: 0,
        pressure_probe=lambda: pressure["allow"],
        now_factory=lambda: datetime(2026, 7, 27, tzinfo=UTC),
    )
    manager.start("ICMarkets.MT5", [("EURUSD", "EUR/USD")])
    availability = manager.next_request("ICMarkets.MT5")
    request_id = availability.rsplit("|", 1)[1]
    decision = manager.availability_result(
        "ICMarkets.MT5",
        request_id,
        earliest=datetime(2026, 6, 27, tzinfo=UTC),
        latest=datetime(2026, 7, 27, tzinfo=UTC),
        count=43200,
    )
    assert decision.startswith("DRILLDOWN|")
    fine = manager.next_request("ICMarkets.MT5").split("|")
    manager.availability_result(
        "ICMarkets.MT5",
        fine[5],
        earliest=datetime.fromisoformat(fine[3]),
        latest=datetime.fromisoformat(fine[4]),
        count=1440,
    )

    pressure["allow"] = False
    assert manager.next_request("ICMarkets.MT5").startswith("BACKFILL|EURUSD|1m|")
    assert manager.next_request("ICMarkets.MT5") == ""
