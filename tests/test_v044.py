from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.domain import Tick
from axetos_market_data.service import MarketDataService
from axetos_market_data.storage import MarketDataStore

ROOT = Path(__file__).resolve().parents[1]


def test_release_metadata() -> None:
    assert __version__ == "0.67.9"
    assert 'version = "0.67.9"' in (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.67.9" in (ROOT / "README.md").read_text(encoding="utf-8")


def test_service_persists_server_owned_partial_candles_for_all_timeframes(tmp_path) -> None:
    store = MarketDataStore(f"sqlite:///{tmp_path / 'market.sqlite'}")
    store.initialize()
    service = MarketDataService(store)
    at = datetime(2026, 7, 26, 10, 7, 12, tzinfo=UTC)

    service.run([Tick("ICMarkets.MT5", "BTC/USD", at, Decimal("64478"), Decimal("64493"))])

    one_minute = store.read_candles("BTC/USD", "1m", provider="ICMarkets.MT5")
    assert len(one_minute) == 1
    assert one_minute[0].complete is False
    assert one_minute[0].open_time == datetime(2026, 7, 26, 10, 7, tzinfo=UTC)

    for timeframe in ("5m", "15m", "30m", "1h", "4h", "1d"):
        values = store.read_candles("BTC/USD", timeframe, provider="ICMarkets.MT5")
        assert len(values) == 1, timeframe
        assert values[0].complete is False
        assert values[0].open == one_minute[0].open
        assert values[0].close == one_minute[0].close


def test_server_updates_partial_ohlc_and_completes_previous_minute(tmp_path) -> None:
    store = MarketDataStore(f"sqlite:///{tmp_path / 'market.sqlite'}")
    store.initialize()
    service = MarketDataService(store)
    start = datetime(2026, 7, 26, 10, 7, 12, tzinfo=UTC)

    service.run([
        Tick("ICMarkets.MT5", "BTC/USD", start, Decimal("100"), Decimal("102")),
        Tick("ICMarkets.MT5", "BTC/USD", start + timedelta(seconds=20), Decimal("104"), Decimal("106")),
        Tick("ICMarkets.MT5", "BTC/USD", start + timedelta(minutes=1), Decimal("103"), Decimal("105")),
    ])

    values = store.read_candles("BTC/USD", "1m", provider="ICMarkets.MT5")
    assert len(values) == 2
    assert values[0].complete is True
    assert values[0].open == Decimal("101")
    assert values[0].high == Decimal("105")
    assert values[0].low == Decimal("101")
    assert values[0].close == Decimal("105")
    assert values[1].complete is False
    assert values[1].open == Decimal("105")
    assert values[1].close == Decimal("104")


def test_candle_api_returns_only_server_selected_time_window(tmp_path) -> None:
    from fastapi.testclient import TestClient
    from axetos_market_data.web import create_app

    database = tmp_path / "api.sqlite"
    store = MarketDataStore(f"sqlite:///{database}")
    store.initialize()
    service = MarketDataService(store)
    start = datetime(2026, 7, 26, 10, 0, tzinfo=UTC)
    service.run([
        Tick("ICMarkets.MT5", "BTC/USD", start, Decimal("100"), Decimal("102")),
        Tick("ICMarkets.MT5", "BTC/USD", start + timedelta(minutes=1), Decimal("101"), Decimal("103")),
        Tick("ICMarkets.MT5", "BTC/USD", start + timedelta(minutes=2), Decimal("102"), Decimal("104")),
    ])

    app = create_app(f"sqlite:///{database}")
    with TestClient(app) as client:
        response = client.get("/api/candles", params={
            "instrument": "BTC/USD",
            "timeframe": "1m",
            "provider": "ICMarkets.MT5",
            "from_utc": (start + timedelta(minutes=1)).isoformat(),
            "to_utc": (start + timedelta(minutes=2)).isoformat(),
            "limit": 100,
        })
    assert response.status_code == 200
    payload = response.json()
    assert payload["authority"] == "Axetos Market Data Server"
    assert payload["candle_generation"] == "server"
    assert [item["open_time_utc"] for item in payload["candles"]] == [
        (start + timedelta(minutes=1)).isoformat(),
        (start + timedelta(minutes=2)).isoformat(),
    ]
