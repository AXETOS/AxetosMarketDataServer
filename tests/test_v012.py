from datetime import UTC, datetime
from decimal import Decimal

from axetos_market_data import __version__
from axetos_market_data.calendar import MarketCalendar
from axetos_market_data.providers.yahoo import YahooHistoricalProvider


def test_version_012():
    assert __version__ == "0.62.0"


def test_fx_market_week_boundaries():
    calendar = MarketCalendar()
    assert not calendar.is_expected_open("EUR/USD", datetime(2026, 7, 24, 22, 1, tzinfo=UTC))  # Friday
    assert not calendar.is_expected_open("EUR/USD", datetime(2026, 7, 26, 21, 59, tzinfo=UTC)) # Sunday
    assert calendar.is_expected_open("EUR/USD", datetime(2026, 7, 26, 22, 0, tzinfo=UTC))


def test_crypto_is_247():
    assert MarketCalendar().is_expected_open("BTC/USD", datetime(2026, 7, 25, 12, tzinfo=UTC))


def test_yahoo_parser_deduplicates_and_validates():
    provider = YahooHistoricalProvider("Yahoo.Test")
    payload = {"chart": {"result": [{"timestamp": [60, 60, 120], "indicators": {"quote": [{
        "open": [1.0, 1.1, None], "high": [1.2, 1.3, 2.0], "low": [0.9, 1.0, 1.0], "close": [1.1, 1.2, 1.5]
    }]}}], "error": None}}
    candles = provider._parse(payload, "1m")
    assert len(candles) == 1
    assert candles[0].provider == "Yahoo.Test"
    assert candles[0].close == Decimal("1.2")
