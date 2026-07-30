from pathlib import Path

from axetos_market_data import __version__


def test_release_metadata() -> None:
    assert __version__ == "0.68.5"
    assert 'version = "0.68.5"' in Path("pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.68.5" in Path("README.md").read_text(encoding="utf-8")


def test_passive_bridge_has_concise_command_logging() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "3.04"' in source
    assert "command received action=" in source
    assert "CopyRates started" in source
    assert "CopyRates returned bars=" in source
    assert "upload chunk %d/%d" in source
    assert "result %s received=" in source
    assert "InpLogCommands" in source


def test_recent_refresh_has_two_catchups_and_final_safety() -> None:
    source = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert "first_catchup" in source
    assert "second_catchup" in source
    assert "final_safety" in source
    assert "timedelta(minutes=2)" in source
    assert "timedelta(minutes=9)" in source
    manager = Path("src/axetos_market_data/full_history.py").read_text(encoding="utf-8")
    assert "recent_pass_started_at" in manager
