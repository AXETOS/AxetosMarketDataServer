from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.candle_builder import CandleBuilder
from axetos_market_data.domain import Candle, Tick
from axetos_market_data.storage import MarketDataStore


def tick(at: datetime, price: str) -> Tick:
    value = Decimal(price)
    return Tick("ICMarkets.MT5", "EUR/USD", at, value, value)


def test_version_and_release_notes() -> None:
    root = Path(__file__).resolve().parents[1]
    assert __version__ == "0.61.1"
    assert 'version = "0.61.1"' in (root / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.61.1" in (root / "README.md").read_text(encoding="utf-8")


def test_identical_complete_ohlc_is_not_persisted(tmp_path: Path) -> None:
    store = MarketDataStore(f"sqlite:///{tmp_path / 'market.sqlite'}")
    store.initialize()
    start = datetime.now().astimezone().replace(second=0, microsecond=0)
    previous = Candle("ICMarkets.MT5", "EUR/USD", "1m", start, Decimal("1.1"), Decimal("1.1"), Decimal("1.1"), Decimal("1.1"), 1, complete=True)
    store.upsert_candle(previous)
    builder = CandleBuilder(store)

    builder.ingest(tick(start + timedelta(minutes=1, seconds=1), "1.1"))
    builder.ingest(tick(start + timedelta(minutes=2, seconds=1), "1.1"))

    candles = store.read_candles("EUR/USD", "1m", provider="ICMarkets.MT5")
    assert [item.open_time for item in candles] == [start]


def test_short_gap_verifies_history_then_fills_only_missing_minutes(tmp_path: Path) -> None:
    store = MarketDataStore(f"sqlite:///{tmp_path / 'market.sqlite'}")
    store.initialize()
    start = datetime.now().astimezone().replace(second=0, microsecond=0)
    store.upsert_candle(Candle("ICMarkets.MT5", "EUR/USD", "1m", start, Decimal("1.1"), Decimal("1.1"), Decimal("1.1"), Decimal("1.1"), 1, complete=True))
    calls = []

    def verify(provider, instrument, gap_start, gap_end):
        calls.append((gap_start, gap_end))
        return [Candle(provider, instrument, "1m", start + timedelta(minutes=2), Decimal("1.1"), Decimal("1.1005"), Decimal("1.1"), Decimal("1.1005"), 2, complete=True)]

    builder = CandleBuilder(store, gap_verifier=verify, short_gap_minutes=60)
    builder.ingest(tick(start + timedelta(minutes=4, seconds=1), "1.1010"))

    candles = store.read_candles("EUR/USD", "1m", provider="ICMarkets.MT5")
    assert calls == [(start + timedelta(minutes=1), start + timedelta(minutes=4))]
    assert [item.open_time for item in candles] == [start, start + timedelta(minutes=1), start + timedelta(minutes=2), start + timedelta(minutes=3), start + timedelta(minutes=4)]
    assert candles[2].high == Decimal("1.1005")


def test_long_gap_is_verified_but_not_flat_filled_when_history_empty(tmp_path: Path) -> None:
    store = MarketDataStore(f"sqlite:///{tmp_path / 'market.sqlite'}")
    store.initialize()
    start = datetime.now().astimezone().replace(second=0, microsecond=0)
    store.upsert_candle(Candle("ICMarkets.MT5", "EUR/USD", "1m", start, Decimal("1.1"), Decimal("1.1"), Decimal("1.1"), Decimal("1.1"), 1, complete=True))
    calls = []

    def verify(provider, instrument, gap_start, gap_end):
        calls.append((gap_start, gap_end))
        return []

    builder = CandleBuilder(store, gap_verifier=verify, short_gap_minutes=60)
    resumed = start + timedelta(hours=3)
    builder.ingest(tick(resumed + timedelta(seconds=1), "1.2"))

    candles = store.read_candles("EUR/USD", "1m", provider="ICMarkets.MT5")
    assert calls == [(start + timedelta(minutes=1), resumed)]
    assert [item.open_time for item in candles] == [start, resumed]
    assert candles[-1].open == Decimal("1.2")
