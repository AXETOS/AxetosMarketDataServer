from datetime import UTC, datetime
from decimal import Decimal

from axetos_market_data.domain import Candle, Tick
from axetos_market_data.quality import CandleQualityService
from axetos_market_data.storage import MarketDataStore
from axetos_market_data import __version__


def test_quality_scan_quarantine_and_rebuild(tmp_path):
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    start = datetime(2026, 1, 2, 10, 0, tzinfo=UTC)
    store.insert_ticks([
        Tick("mt5", "EUR/USD", start, Decimal("1.1000"), Decimal("1.1002")),
        Tick("mt5", "EUR/USD", start.replace(second=30), Decimal("1.1010"), Decimal("1.1012")),
    ])
    with store.connect() as connection:
        connection.execute("""INSERT INTO candles VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                           ("mt5","EUR/USD","1m",start.isoformat(timespec="microseconds"),
                            "1.1","0.9","1.2","1.1",2,None,1))
    quality = CandleQualityService(store)
    result = quality.scan()
    assert result.severe >= 1
    issue = quality.list_issues()[0]
    quality.quarantine(int(issue["id"]))
    rebuilt = quality.rebuild_one_minute(int(issue["id"]))
    assert rebuilt["tick_count"] == 2
    candles = store.read_candles("EUR/USD", "1m", provider="mt5")
    assert candles[0].high == Decimal("1.1011")
    assert candles[0].low == Decimal("1.1001")


def test_version_is_v011():
    assert __version__ == "0.47.0"
