from __future__ import annotations

from datetime import UTC, datetime, timedelta

from axetos_market_data.full_history import FullHistoryBackfillManager, PlannedRange


def test_recent_refresh_waits_for_instrument_verification_before_next_symbol() -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
    callbacks: list[tuple[str, str, dict[str, object]]] = []
    manager = FullHistoryBackfillManager(
        lambda *_: 0,
        on_instrument_completed=lambda provider, instrument, details: callbacks.append(
            (provider, instrument, details)
        ),
        now_factory=lambda: now,
    )
    start = now - timedelta(hours=24)
    result = manager.start_targeted(
        "ICMarkets.MT5",
        [
            ("EURUSD", "EUR/USD", [PlannedRange("1m", start, now, 4)]),
            ("BTCUSD", "Bitcoin", [PlannedRange("1m", start, now, 4)]),
        ],
        workflow="recent_m1",
    )

    request = manager.next_request("ICMarkets.MT5")
    request_id = request.split("|")[-1]
    manager.batch_result("ICMarkets.MT5", request_id, 1440, 1440, True)

    # Polling cannot dispatch BTCUSD until EUR/USD verification explicitly completes.
    assert manager.next_request("ICMarkets.MT5") == ""
    assert callbacks and callbacks[0][1] == "EUR/USD"
    assert manager.instrument_verified(
        "ICMarkets.MT5", result["job_id"], "EUR/USD",
        {"database_rows_verified": 1440, "chart_rows_verified": 1440},
    )
    assert "BTCUSD" in manager.next_request("ICMarkets.MT5")


def test_failed_instrument_verification_does_not_freeze_next_symbol() -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
    manager = FullHistoryBackfillManager(
        lambda *_: 0,
        on_instrument_completed=lambda *_: None,
        now_factory=lambda: now,
    )
    start = now - timedelta(hours=24)
    result = manager.start_targeted(
        "ICMarkets.MT5",
        [
            ("SOLUSD", "Solana", [PlannedRange("1m", start, now, 4)]),
            ("ETHUSD", "Ethereum", [PlannedRange("1m", start, now, 4)]),
        ],
        workflow="recent_m1",
    )
    request_id = manager.next_request("ICMarkets.MT5").split("|")[-1]
    manager.batch_result("ICMarkets.MT5", request_id, 100, 100, True)
    assert manager.next_request("ICMarkets.MT5") == ""
    assert manager.instrument_verified(
        "ICMarkets.MT5", result["job_id"], "Solana", {"status": "BAD"},
        error="verification mismatch",
    )
    assert "ETHUSD" in manager.next_request("ICMarkets.MT5")
