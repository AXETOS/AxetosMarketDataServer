from pathlib import Path

from axetos_market_data import __version__


def test_release_metadata() -> None:
    assert __version__ == "0.67.7"
    assert 'version = "0.67.7"' in Path("pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.67.7" in Path("README.md").read_text(encoding="utf-8")


def test_bridge_is_passive_question_answer_adapter() -> None:
    source = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")
    assert '#property version   "3.02"' in source
    assert len(source.splitlines()) <= 450
    assert "PollCommand" in source
    assert "CopyRates" in source
    assert "UploadCandles" in source
    assert "SendTicks" in source
    for forbidden in (
        "LiveM1", "Backfill", "RepairRecent", "FindEarliestRetrievableM1",
        "SendPreviousCompletedM1", "InpCompletedM1DelaySeconds",
        "g_last_m1_bar", "retry_after", "RECOVERING",
    ):
        assert forbidden not in source
