from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from axetos_market_data.domain import Candle
from axetos_market_data.full_history import FullHistoryBackfillManager
from history_test_helpers import complete_history_discovery
from axetos_market_data.storage import MarketDataStore

def test_full_history_directly_downloads_source_ranges(tmp_path):
    now=datetime(2026,7,27,tzinfo=UTC); m=FullHistoryBackfillManager(lambda *_:0,now_factory=lambda:now)
    m.start('P',[('EURUSD','EUR/USD')]); assert complete_history_discovery(m, 'P').startswith('BACKFILL|EURUSD|1m|')

def test_insert_candles_missing_preserves_existing(tmp_path):
    s=MarketDataStore(tmp_path/'x.sqlite'); s.initialize(); t=datetime(2026,7,1,tzinfo=UTC)
    a=Candle('P','EUR/USD','1m',t,Decimal('1'),Decimal('2'),Decimal('1'),Decimal('2'),1,None,True)
    b=Candle('P','EUR/USD','1m',t,Decimal('9'),Decimal('9'),Decimal('9'),Decimal('9'),1,None,True)
    s.upsert_candle(a); assert s.insert_candles_missing([b])==0

def test_management_ui_exposes_full_history():
    source=Path('src/axetos_market_data/web.py').read_text(); assert '/api/full-history/' in source
