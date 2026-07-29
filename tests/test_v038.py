from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.web import aggregate_feed_state


def reports(*states: str) -> list[dict[str, object]]:
    return [{"feed_state": state} for state in states]


def test_live_and_inactive_feeds_are_partial() -> None:
    state, counts = aggregate_feed_state(reports("LIVE", "LIVE", "INACTIVE"))
    assert state == "PARTIAL"
    assert counts == {"LIVE": 2, "INACTIVE": 1}


def test_live_and_quiet_feeds_remain_live() -> None:
    state, _ = aggregate_feed_state(reports("LIVE", "QUIET"))
    assert state == "LIVE"


def test_all_inactive_feeds_are_inactive() -> None:
    state, _ = aggregate_feed_state(reports("INACTIVE", "INACTIVE"))
    assert state == "INACTIVE"


def test_no_feed_reports_are_initializing() -> None:
    state, counts = aggregate_feed_state([])
    assert state == "INITIALIZING"
    assert counts == {}


def test_v038_release_metadata() -> None:
    root = Path(__file__).resolve().parents[1]
    assert __version__ == "0.67.8"
    assert 'version = "0.67.8"' in (root / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.67.8" in (root / "README.md").read_text(encoding="utf-8")
