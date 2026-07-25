from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.candle_builder import CandleBuilder
from axetos_market_data.domain import Tick
from axetos_market_data.feed import FeedStateEngine, FeedThresholds
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.web import create_app

ROOT = Path(__file__).resolve().parents[1]


def tick(at: datetime, bid: str, ask: str, provider: str = "Oanda.MT5") -> Tick:
    return Tick(provider, "EUR/USD", at, Decimal(bid), Decimal(ask))


def test_release_readme_and_runtime_data_policy() -> None:
    assert __version__ == "0.34.2"
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "## Version 0.34.2" in readme
    assert "GET /api/feed-status" in readme
    assert "flat candles" in readme
    assert "data/" in (ROOT / ".gitignore").read_text(encoding="utf-8")


def test_feed_state_ignores_unchanged_quotes_and_recovers() -> None:
    engine = FeedStateEngine(FeedThresholds(60, 180, 600))
    start = datetime(2026, 7, 24, 20, 0, tzinfo=UTC)
    first = engine.observe(tick(start, "1.1000", "1.1002"))
    quiet = engine.observe(tick(start + timedelta(seconds=61), "1.1000", "1.1002"))
    stalled = engine.observe(tick(start + timedelta(seconds=181), "1.1000", "1.1002"))
    inactive = engine.observe(tick(start + timedelta(seconds=601), "1.1000", "1.1002"))
    recovered = engine.observe(tick(start + timedelta(seconds=602), "1.1010", "1.1012"))

    assert first.accept_tick and first.continuity == "DETACHED"
    assert not quiet.accept_tick and quiet.state == "QUIET"
    assert not stalled.accept_tick and stalled.state == "STALLED"
    assert not inactive.accept_tick and inactive.state == "INACTIVE"
    assert recovered.accept_tick and recovered.recovery_required
    report = engine.reports(now=start + timedelta(seconds=602))[0]
    assert report["accepted_ticks"] == 2
    assert report["ignored_unchanged_updates"] == 3
    assert report["monitoring"] is True


def test_candle_builder_connects_or_detaches_from_feed_decision(tmp_path: Path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite")
    store.initialize()
    builder = CandleBuilder(store)
    start = datetime(2026, 7, 24, 20, 0, 10, tzinfo=UTC)

    builder.ingest(tick(start, "1.1000", "1.1002"), continuity="DETACHED")
    builder.ingest(tick(start + timedelta(minutes=1), "1.1010", "1.1012"), continuity="CONNECTED")
    builder.finalize("Oanda.MT5", "EUR/USD")
    candles = store.read_candles("EUR/USD", "1m", provider="Oanda.MT5")
    assert candles[1].open == candles[0].close
    assert candles[1].low == candles[0].close

    builder.ingest(tick(start + timedelta(minutes=20), "1.1200", "1.1202"), continuity="DETACHED")
    builder.finalize("Oanda.MT5", "EUR/USD")
    candles = store.read_candles("EUR/USD", "1m", provider="Oanda.MT5")
    assert candles[-1].open == Decimal("1.1201")
    assert candles[-1].open != candles[-2].close


def test_feed_status_endpoint(tmp_path: Path) -> None:
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        response = client.get("/api/feed-status")
        assert response.status_code == 200
        assert response.json() == {"overall_state": "INITIALIZING", "count": 0, "items": []}
