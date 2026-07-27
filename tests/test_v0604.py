from datetime import datetime, timedelta, timezone
from axetos_market_data.full_history import FullHistoryBackfillManager

UTC=timezone.utc

def test_availability_decision_is_reported_before_next_range():
    now=datetime(2026,7,27,tzinfo=UTC)
    manager=FullHistoryBackfillManager(lambda *args: 0, now_factory=lambda: now)
    manager.start("P", [("SOLUSD","SOL/USD")])
    req=manager.next_request("P")
    parts=req.split("|")
    assert parts[0]=="AVAILABILITY"
    decision=manager.availability_result("P", parts[5], earliest=now-timedelta(days=1), latest=now, count=1436)
    assert decision=="BACKFILL|1436|0"
    next_req=manager.next_request("P")
    assert next_req.startswith("BACKFILL|SOLUSD|1m|")
    assert manager.next_request("P")==""

def test_availability_skip_is_explicit():
    now=datetime(2026,7,27,tzinfo=UTC)
    manager=FullHistoryBackfillManager(lambda *args: 1436, now_factory=lambda: now)
    manager.start("P", [("SOLUSD","SOL/USD")])
    req=manager.next_request("P")
    parts=req.split("|")
    decision=manager.availability_result("P", parts[5], earliest=now-timedelta(days=1), latest=now, count=1436)
    assert decision=="SKIP|1436|1436"
