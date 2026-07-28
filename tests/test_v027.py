from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.candle_builder import CandleBuilder
from axetos_market_data.domain import Tick
from axetos_market_data.feed import FeedStateEngine, FeedThresholds
from axetos_market_data.storage import MarketDataStore

ROOT = Path(__file__).resolve().parents[1]


def fx_tick(at: datetime, bid: str, ask: str) -> Tick:
    return Tick("Oanda.MT5", "EUR/USD", at, Decimal(bid), Decimal(ask))


def test_release_and_readme_reference_price_documentation() -> None:
    assert __version__ == "0.61.1"
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "## Version 0.61.1" in readme
    assert "normalized midpoint" in readme
    assert "spread-only flicker" in readme


def test_spread_only_change_does_not_keep_feed_live() -> None:
    engine = FeedStateEngine(FeedThresholds(60, 180, 600))
    start = datetime(2026, 7, 25, 0, 0, tzinfo=UTC)

    first = engine.observe(fx_tick(start, "1.13697", "1.13709"))
    # Bid falls one point while ask rises one point: same midpoint/reference price.
    spread_only = engine.observe(fx_tick(start + timedelta(seconds=61), "1.13696", "1.13710"))

    assert first.accept_tick
    assert not spread_only.accept_tick
    assert spread_only.state == "QUIET"
    report = engine.reports(now=start + timedelta(seconds=61))[0]
    assert report["market_price"] == "1.13703"
    assert report["last_bid"] == "1.13697"
    assert report["last_ask"] == "1.13709"


def test_candle_builder_uses_same_normalized_reference_price(tmp_path: Path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    builder = CandleBuilder(store)
    start = datetime(2026, 7, 25, 0, 0, tzinfo=UTC)

    first = fx_tick(start, "1.1000", "1.1001")
    assert first.market_price == Decimal("1.1001")
    builder.ingest(first, continuity="DETACHED")
    builder.finalize("Oanda.MT5", "EUR/USD")

    candle = store.read_candles("EUR/USD", "1m", provider="Oanda.MT5")[0]
    assert candle.open == first.market_price
    assert candle.close == first.market_price
