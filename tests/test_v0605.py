from datetime import UTC, datetime, timedelta
from pathlib import Path

from axetos_market_data.full_history import FullHistoryBackfillManager


def test_one_missing_candle_is_planned_then_downloaded_and_acknowledged() -> None:
    now = datetime(2026, 7, 27, tzinfo=UTC)
    leaf_start = now - timedelta(minutes=59)

    def local_count(_provider, _instrument, timeframe, start, end):
        if timeframe == "1m" and start >= leaf_start:
            return 1434
        return 0

    manager = FullHistoryBackfillManager(local_count, now_factory=lambda: now)
    manager.start("ICMarkets.MT5", [("SOLUSD", "SOL/USD")])

    discovery = manager.next_request("ICMarkets.MT5").split("|")
    manager.availability_result(
        "ICMarkets.MT5", discovery[5], earliest=leaf_start, latest=now, count=1435
    )
    for _ in ("1h", "1d"):
        request = manager.next_request("ICMarkets.MT5").split("|")
        manager.availability_result("ICMarkets.MT5", request[5], earliest=None, latest=None, count=0)

    final_decision = ""
    for _ in range(20):
        request = manager.next_request("ICMarkets.MT5")
        if request.startswith("BACKFILL|"):
            download = request.split("|")
            break
        parts = request.split("|")
        final_decision = manager.availability_result(
            "ICMarkets.MT5", parts[5], earliest=datetime.fromisoformat(parts[3]),
            latest=datetime.fromisoformat(parts[4]), count=1435,
        )
    else:
        raise AssertionError("download was not dispatched")

    assert final_decision == "PLAN_MISSING|1435|1434"
    assert download[:3] == ["BACKFILL", "SOLUSD", "1m"]
    assert manager.next_request("ICMarkets.MT5") == ""

    acknowledgement = manager.batch_result(
        "ICMarkets.MT5", download[5], bars_received=1435, bars_inserted=1, completed=True
    )
    assert acknowledgement == "STORED|1435|1|1434"


def test_bridge_and_server_expose_explicit_storage_handshake() -> None:
    bridge = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    web = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    manager = Path("src/axetos_market_data/full_history.py").read_text(encoding="utf-8")

    assert '#property version   "1.29"' in bridge
    assert "download started" in bridge
    assert "CopyRates returned" in bridge
    assert "server stored %d and skipped %d" in bridge
    assert "stored/skipped result" in bridge
    assert "PostJsonText(result_path" in bridge
    assert '"acknowledgement": "stored"' in web
    assert '"stored": stored' in web
    assert '"skipped": skipped' in web
    assert 'return f"STORED|{received}|{inserted}|{skipped}"' in manager
