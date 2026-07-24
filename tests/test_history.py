from datetime import datetime, timezone
from decimal import Decimal
from axetos_market_data.domain import Candle
from axetos_market_data.history import HistoricalBackfillService
from axetos_market_data.storage import MarketDataStore

class FakeHistory:
    def fetch_candles(self, symbol, timeframe, start, end):
        return [Candle("source","EUR/USD","1m",datetime(2026,1,1,12,0,tzinfo=timezone.utc),Decimal("1"),Decimal("2"),Decimal("1"),Decimal("1.5"),10)]

def test_backfill_writes_candles_and_state(tmp_path):
    store=MarketDataStore(tmp_path/'market.sqlite'); store.initialize()
    start=datetime(2026,1,1,12,0,tzinfo=timezone.utc); end=datetime(2026,1,1,12,2,tzinfo=timezone.utc)
    result=HistoricalBackfillService(store).run(FakeHistory(),"test","EURUSD","EUR/USD","1m",start,end)
    assert result.written == 1
    assert len(store.read_candles("EUR/USD","1m",provider="test")) == 1
    assert store.list_ingestion_state()[0]["status"] == "Completed"
    assert result.gaps == 1
