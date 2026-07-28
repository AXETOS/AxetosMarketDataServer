from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from axetos_market_data.bridge import BridgeCandle, BridgeCandlesRequest, Mt5BridgeService
from axetos_market_data.domain import Candle
from axetos_market_data.storage import MarketDataStore


def test_authoritative_completed_m1_force_replaces_exact_timestamp(tmp_path: Path) -> None:
    store = MarketDataStore(str(tmp_path / "market.sqlite")); store.initialize()
    store.upsert_symbol_policy("ICMarkets.MT5", "EURUSD", "EUR/USD")
    stamp = datetime(2026, 7, 28, 15, 40, tzinfo=UTC)
    store.upsert_candles([Candle(
        "ICMarkets.MT5", "EUR/USD", "1m", stamp,
        Decimal("1.1000"), Decimal("1.1000"), Decimal("1.1000"), Decimal("1.1000"), 1,
    )])
    bridge = Mt5BridgeService(store)
    try:
        written = bridge.candles(BridgeCandlesRequest(
            provider_key="ICMarkets.MT5", terminal_instance_id="ic",
            provider_symbol="EURUSD", canonical_instrument="EUR/USD", interval="1m",
            authoritative=True, candles=[BridgeCandle(
                time_utc=stamp, open=Decimal("1.1010"), high=Decimal("1.1020"),
                low=Decimal("1.1005"), close=Decimal("1.1015"), tick_volume=42,
            )],
        ))
        assert written == 1
        candles = store.read_candles("EUR/USD", "1m", provider="ICMarkets.MT5")
        assert len(candles) == 1
        assert candles[0].open == Decimal("1.1010")
        assert candles[0].close == Decimal("1.1015")
    finally:
        bridge.shutdown()


def test_bridge_source_polls_previous_completed_m1_and_marks_authoritative() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert "SendPreviousCompletedM1();" in source
    assert "authoritative" in source and "true" in source
    assert "CopyRates(symbol, PERIOD_M1, completed_open, completed_end" in source
    assert "Live observations are feed-health/current-price evidence only" in Path(
        "src/axetos_market_data/bridge.py"
    ).read_text(encoding="utf-8")


def test_recent_repair_source_schedules_run_window_catchup() -> None:
    source = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert "repair.recent_m1_catchup_started" in source
    assert "repair.recent_m1_catchup_completed" in source
    assert "recent_refresh_started_at" in source
    assert "catchup_from_utc" in source
