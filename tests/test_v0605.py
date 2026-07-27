from datetime import UTC, datetime, timedelta
from pathlib import Path

from axetos_market_data.full_history import FullHistoryBackfillManager


def test_one_missing_candle_dispatches_backfill_and_waits_for_storage_ack() -> None:
    now = datetime(2026, 7, 27, tzinfo=UTC)
    manager = FullHistoryBackfillManager(
        lambda *_args: 1434,
        pressure_probe=lambda: False,
        now_factory=lambda: now,
    )
    manager.start("ICMarkets.MT5", [("SOLUSD", "SOL/USD")])

    availability = manager.next_request("ICMarkets.MT5")
    availability_parts = availability.split("|")
    decision = manager.availability_result(
        "ICMarkets.MT5",
        availability_parts[5],
        earliest=now - timedelta(days=1),
        latest=now,
        count=1435,
    )

    assert decision == "BACKFILL|1435|1434"
    download = manager.next_request("ICMarkets.MT5")
    download_parts = download.split("|")
    assert download_parts[:5] == [
        "BACKFILL",
        "SOLUSD",
        "1m",
        availability_parts[3],
        availability_parts[4],
    ]
    assert manager.next_request("ICMarkets.MT5") == ""

    acknowledgement = manager.batch_result(
        "ICMarkets.MT5",
        download_parts[5],
        bars_received=1435,
        bars_inserted=1,
        completed=True,
    )
    assert acknowledgement == "STORED|1435|1|1434"

    next_range = manager.next_request("ICMarkets.MT5")
    assert next_range.startswith("AVAILABILITY|SOLUSD|1m|")
    assert next_range.split("|")[3] != availability_parts[3]


def test_bridge_and_server_expose_explicit_storage_handshake() -> None:
    bridge = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    web = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    manager = Path("src/axetos_market_data/full_history.py").read_text(encoding="utf-8")

    assert '#property version   "1.23"' in bridge
    assert "download started" in bridge
    assert "CopyRates returned" in bridge
    assert "server stored %d and skipped %d" in bridge
    assert "stored/skipped result" in bridge
    assert "PostJsonText(result_path" in bridge
    assert '"acknowledgement": "stored"' in web
    assert '"stored": stored' in web
    assert '"skipped": skipped' in web
    assert 'return f"STORED|{received}|{inserted}|{skipped}"' in manager
