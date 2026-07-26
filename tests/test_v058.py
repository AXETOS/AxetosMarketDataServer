from datetime import UTC, datetime
from pathlib import Path

from axetos_market_data.full_history import FullHistoryBackfillManager


def _manager():
    return FullHistoryBackfillManager(lambda _p, _i: None, batch_days=3)


def test_unavailable_history_range_advances_instead_of_retrying_forever():
    manager = _manager()
    manager.start("ICMarkets.MT5", [("SOLUSD", "SOL/USD")])
    availability = manager.next_request("ICMarkets.MT5")
    manager.availability_result(
        "ICMarkets.MT5",
        availability.split("|")[-1],
        datetime(2026, 5, 11, tzinfo=UTC),
    )
    first = manager.next_request("ICMarkets.MT5")
    first_id = first.split("|")[-1]
    manager.batch_result(
        "ICMarkets.MT5", first_id, 0, 0, False,
        unavailable=True, error_code=4401,
    )
    second = manager.next_request("ICMarkets.MT5")
    assert second
    assert second != first
    status = manager.status("ICMarkets.MT5")["jobs"][0]["instruments"][0]
    assert status["unavailable_ranges"] == 1
    assert status["last_error_code"] == 4401
    assert status["retry_count"] == 0


def test_transient_history_failure_is_bounded_to_three_attempts():
    manager = _manager()
    manager.start("ICMarkets.MT5", [("EURUSD", "EUR/USD")])
    availability = manager.next_request("ICMarkets.MT5")
    manager.availability_result(
        "ICMarkets.MT5",
        availability.split("|")[-1],
        datetime(2026, 1, 1, tzinfo=UTC),
    )
    requests = []
    for _ in range(3):
        request = manager.next_request("ICMarkets.MT5")
        requests.append(request)
        manager.batch_result(
            "ICMarkets.MT5", request.split("|")[-1], 0, 0, False,
            error_code=4066,
        )
    next_request = manager.next_request("ICMarkets.MT5")
    assert next_request not in requests
    status = manager.status("ICMarkets.MT5")["jobs"][0]["instruments"][0]
    assert status["unavailable_ranges"] == 1
    assert status["retry_count"] == 0


def test_bridge_confirms_availability_and_reports_4401():
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "1.17"' in source
    assert "FindEarliestRetrievableM1" in source
    assert "CopyRates(symbol, PERIOD_M1, probe_start, probe_end" in source
    assert "copy_error == 4401" in source
    assert '"&unavailable="' in source
    assert '"&errorCode="' in source
