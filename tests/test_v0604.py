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
    decision=manager.availability_result("P", parts[5], earliest=now-timedelta(days=30), latest=now, count=43000)
    assert decision=="DRILLDOWN|43000|0"
    fine=manager.next_request("P").split("|")
    fine_decision=manager.availability_result("P", fine[5], earliest=datetime.fromisoformat(fine[3]), latest=datetime.fromisoformat(fine[4]), count=1436)
    assert fine_decision=="BACKFILL|1436|0"
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
