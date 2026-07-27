from datetime import UTC, datetime, timedelta
from decimal import Decimal

from axetos_market_data import __version__
from axetos_market_data.feed import FeedStateEngine, FeedThresholds


def test_seeded_feed_restores_inactive_state():
    engine = FeedStateEngine(FeedThresholds(1, 2, 3))
    engine.seed_inactive(
        "Oanda.MT5", "EUR/USD", datetime.now(UTC) - timedelta(hours=1),
        Decimal("1.13703"), Decimal("1.13697"), Decimal("1.13709"),
    )
    report = engine.reports()[0]
    assert report["feed_state"] == "INACTIVE"
    assert report["monitoring"] is True


def test_release_028_metadata_and_readme():
    assert __version__ == "0.60.10"
    readme = open("README.md", encoding="utf-8").read()
    assert "## Version 0.60.10" in readme
    assert "system health healthy" in readme.lower()
