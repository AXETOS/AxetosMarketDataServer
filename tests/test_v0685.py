from datetime import UTC, datetime

from axetos_market_data.full_history import FullHistoryBackfillManager


def test_full_history_logs_per_timeframe_summary() -> None:
    events: list[tuple[str, str, dict[str, object]]] = []
    manager = FullHistoryBackfillManager(
        lambda *_: 0,
        on_event=lambda category, message, _provider, details: events.append((category, message, details)),
        now_factory=lambda: datetime(2026, 7, 30, 12, tzinfo=UTC),
    )
    manager.start("P", [("AUS200", "AUS200")])

    # Finish the fixed source plan while reporting deterministic counts per timeframe.
    while True:
        command = manager.next_request("P")
        if not command:
            break
        parts = command.split("|")
        timeframe, request_id = parts[2], parts[-1]
        count = {"1m": 10, "1h": 20, "1d": 30}[timeframe]
        manager.batch_result("P", request_id, count, count, True)

    summaries = [entry for entry in events if entry[0] == "backfill.instrument_download_summary"]
    assert len(summaries) == 1
    _, message, details = summaries[0]
    assert "MT5 history received for AUS200" in message
    assert "M1 received=" in message
    assert "H1 received=" in message
    assert "D1 received=" in message
    assert details["timeframes"]["1d"]["received"] == 30
