from pathlib import Path

from axetos_market_data import __version__


def test_release_metadata_and_bridge_display_version() -> None:
    assert __version__ == "0.68.12"
    bridge = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "3.06"' in bridge
    assert "Axetos MT5 Bridge v3.06 started" in bridge
    assert "Axetos MT5 Bridge v3.05 started" not in bridge


def test_startup_refresh_is_m1_only_and_compact() -> None:
    web = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    history = Path("src/axetos_market_data/full_history.py").read_text(encoding="utf-8")
    assert 'if workflow == "startup_m1"' in web
    assert '"higher_timeframes_rebuilt": False' in web
    assert 'if job.workflow == "startup_m1"' in history
    assert 'startup.m1_refresh_completed' in history
    assert 'startup M1 refresh\\n' in history
    assert 'requested={requested}, received={received}, stored={stored}' in history
