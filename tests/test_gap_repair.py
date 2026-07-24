from datetime import UTC, datetime, timedelta
from decimal import Decimal

from axetos_market_data.domain import Candle
from axetos_market_data.history import HistoricalBackfillService
from axetos_market_data.storage import MarketDataStore


class FakeHistoryProvider:
    def fetch_candles(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> list[Candle]:
        candles: list[Candle] = []
        cursor = start
        step = timedelta(minutes=1)
        while cursor < end:
            candles.append(Candle(
                provider="fake",
                instrument="EUR/USD",
                timeframe=timeframe,
                open_time=cursor,
                open=Decimal("1.1000"),
                high=Decimal("1.1010"),
                low=Decimal("1.0990"),
                close=Decimal("1.1005"),
                tick_count=12,
                complete=True,
            ))
            cursor += step
        return candles


def test_gap_repair_groups_windows_and_resolves_rows(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    start = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
    store.record_gap("ICMarkets.MT5", "EUR/USD", "1m", start, start + timedelta(minutes=1))
    store.record_gap("ICMarkets.MT5", "EUR/USD", "1m", start + timedelta(minutes=1), start + timedelta(minutes=2))

    result = HistoricalBackfillService(store).repair_gaps(
        FakeHistoryProvider(),
        "ICMarkets.MT5",
        {"EUR/USD": "EURUSD.raw"},
    )

    assert result.gaps_selected == 2
    assert result.windows_requested == 1
    assert result.candles_received == 2
    assert result.gaps_resolved == 2
    assert result.gaps_remaining == 0
    assert len(store.list_repair_runs()) == 1
