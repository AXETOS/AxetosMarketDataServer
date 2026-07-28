from datetime import UTC, datetime, timedelta
from decimal import Decimal

from axetos_market_data.domain import Candle
from axetos_market_data.full_history import FullHistoryBackfillManager, PlannedRange
from axetos_market_data.hierarchical_repair import HierarchicalCandleRepair
from axetos_market_data.storage import MarketDataStore


def test_hierarchical_planner_reaches_hour_leaves_before_m1_download() -> None:
    now = datetime(2026, 7, 28, 12, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_: 0, now_factory=lambda: now)
    manager.start("P", [("EURUSD", "EUR/USD")])

    request = manager.next_request("P").split("|")
    manager.availability_result(
        "P", request[5], earliest=now - timedelta(hours=2), latest=now, count=121
    )
    for _ in ("1h", "1d"):
        request = manager.next_request("P").split("|")
        manager.availability_result("P", request[5], earliest=None, latest=None, count=0)

    availability_count = 0
    while True:
        command = manager.next_request("P")
        if command.startswith("BACKFILL|"):
            break
        parts = command.split("|")
        availability_count += 1
        manager.availability_result(
            "P", parts[5], earliest=datetime.fromisoformat(parts[3]),
            latest=datetime.fromisoformat(parts[4]), count=1,
        )
        assert availability_count < 30

    # root -> year -> month -> day -> hour leaves; adjacent missing hours merge.
    assert availability_count >= 5
    parts = command.split("|")
    assert parts[:3] == ["BACKFILL", "EURUSD", "1m"]
    assert datetime.fromisoformat(parts[3]) == now - timedelta(hours=2)
    assert datetime.fromisoformat(parts[4]) == now


def test_unavailable_leaf_is_marked_bad_and_does_not_freeze() -> None:
    now = datetime(2026, 7, 28, 12, tzinfo=UTC)
    bad: list[tuple[str, datetime, datetime, str]] = []
    manager = FullHistoryBackfillManager(
        lambda *_: 0,
        bad_range_recorder=lambda _p, _i, tf, start, end, reason, _code: bad.append((tf, start, end, reason)),
        now_factory=lambda: now,
    )
    manager.start("P", [("EURUSD", "EUR/USD")])
    first = manager.next_request("P").split("|")
    manager.availability_result(
        "P", first[5], earliest=now - timedelta(hours=1), latest=now, count=61
    )
    for _ in ("1h", "1d"):
        request = manager.next_request("P").split("|")
        manager.availability_result("P", request[5], earliest=None, latest=None, count=0)

    for _ in range(20):
        command = manager.next_request("P")
        if not command:
            break
        parts = command.split("|")
        if parts[0] == "BACKFILL":
            manager.batch_result("P", parts[5], 0, 0, False, unavailable=True, error_code=4401)
            continue
        depth = manager.status("P")["jobs"][0]["instruments"][0]["tier_index"]
        manager.availability_result(
            "P", parts[5], earliest=None if depth >= 4 else datetime.fromisoformat(parts[3]),
            latest=None if depth >= 4 else datetime.fromisoformat(parts[4]),
            count=0 if depth >= 4 else 1,
        )
    assert bad
    assert manager.next_request("P") == ""
    status = manager.status("P")["jobs"][0]
    assert status["status"] == "completed"


def _candle(provider: str, instrument: str, timeframe: str, at: datetime, value: str) -> Candle:
    price = Decimal(value)
    return Candle(provider, instrument, timeframe, at, price, price, price, price, 1, complete=True)


def test_quality_aware_repair_overwrites_provider_once_then_retains_same(tmp_path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    provider, instrument = "P", "EUR/USD"
    start = datetime(2026, 7, 28, 10, tzinfo=UTC)
    store.upsert_candle(_candle(provider, instrument, "1h", start, "10"))
    for minute in range(60):
        store.upsert_candle(_candle(provider, instrument, "1m", start + timedelta(minutes=minute), "11"))

    repair = HierarchicalCandleRepair(store)
    first = repair._repair_candles(provider, instrument, "1m", "1h", "run-1", 500)
    assert first.overwritten == 1
    assert store.read_candles(instrument, "1h", provider=provider)[0].close == Decimal("11")
    provenance = store.get_candle_provenance(provider, instrument, "1h", start)
    assert provenance is not None and provenance["source_timeframe"] == "1m"

    second = repair._repair_candles(provider, instrument, "1m", "1h", "run-2", 500)
    assert second.retained_same == 1
    assert second.overwritten == 0


def test_coarser_candidate_cannot_overwrite_finer_provenance(tmp_path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    provider, instrument = "P", "EUR/USD"
    start = datetime(2026, 7, 28, 0, tzinfo=UTC)
    existing = _candle(provider, instrument, "1d", start, "12")
    store.upsert_candle(existing)
    store.set_candle_provenance(
        provider, instrument, "1d", start,
        source_kind="derived", source_timeframe="ticks", quality_rank=600,
        coverage_complete=True, repair_run_id="fine",
    )
    for hour in range(24):
        store.upsert_candle(_candle(provider, instrument, "1h", start + timedelta(hours=hour), "11"))

    result = HierarchicalCandleRepair(store)._repair_candles(
        provider, instrument, "1h", "1d", "coarse", 450
    )
    assert result.overwritten == 0
    assert result.retained_better_or_equal == 1
    assert store.read_candles(instrument, "1d", provider=provider)[0].close == Decimal("12")


def test_targeted_recent_m1_job_downloads_only_supplied_ranges() -> None:
    now = datetime(2026, 7, 28, 12, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_: 0, now_factory=lambda: now)
    ranges = [
        PlannedRange("1m", now - timedelta(minutes=20), now - timedelta(minutes=11), 4),
        PlannedRange("1m", now - timedelta(minutes=10), now - timedelta(minutes=1), 4),
    ]
    result = manager.start_targeted("P", [("EURUSD", "EUR/USD", ranges)])
    assert result["workflow"] == "recent_m1"
    command = manager.next_request("P").split("|")
    assert command[:3] == ["BACKFILL", "EURUSD", "1m"]
    # Adjacent minute ranges are merged into one exact repair request.
    assert datetime.fromisoformat(command[3]) == now - timedelta(minutes=20)
    assert datetime.fromisoformat(command[4]) == now - timedelta(minutes=1)
    manager.batch_result("P", command[5], 20, 20, True)
    assert manager.next_request("P") == ""
    status = manager.status("P")["jobs"][0]
    assert status["status"] == "completed"


def test_full_job_can_resume_once_with_recent_m1_ranges() -> None:
    now = datetime(2026, 7, 28, 12, tzinfo=UTC)
    manager = FullHistoryBackfillManager(lambda *_: 0, now_factory=lambda: now)
    started = manager.start("P", [("EURUSD", "EUR/USD")])
    job_id = started["job_id"]
    # Place the job in the state used by the post-download repair callback.
    job = manager._jobs[job_id]
    job.status = "repairing"
    job.phase = "repair"
    gap = PlannedRange("1m", now - timedelta(minutes=5), now - timedelta(minutes=1), 4)
    assert manager.resume_targeted_after_repair("P", job_id, {"EUR/USD": [gap]})
    context = manager.job_context("P", job_id)
    assert context is not None and context["repair_pass"] == 1
    command = manager.next_request("P").split("|")
    assert command[:3] == ["BACKFILL", "EURUSD", "1m"]
