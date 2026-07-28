from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.endurance import EnduranceRunner


def test_v031_version_readme_and_two_row_toolbar():
    assert __version__ == "0.62.1"
    readme = Path("README.md").read_text(encoding="utf-8")
    web = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert "## Version 0.62.1" in readme
    assert "axetos-market-data endurance" in readme
    assert 'class="toolbar-actions"' in web
    assert web.count('class="toolbar-row"') >= 2


def test_endurance_runner_reports_cycles_and_drift():
    result = EnduranceRunner(
        duration_seconds=0.001,
        ticks_per_cycle=100,
        instruments=2,
        batch_size=50,
        regression_threshold_percent=100,
    ).run()
    assert result.cycles_completed >= 1
    assert result.total_ticks_written == result.cycles_completed * 100
    assert result.minimum_ticks_per_second > 0
    assert result.maximum_peak_memory_bytes > 0
    assert result.regression_detected is False
