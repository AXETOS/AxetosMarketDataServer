from pathlib import Path

from axetos_market_data import __version__


def test_v0621_metadata_and_readme_order() -> None:
    root = Path(__file__).resolve().parents[1]
    readme = (root / "README.md").read_text(encoding="utf-8")
    assert __version__ == "0.68.10"
    assert readme.startswith("# Axetos Market Data Server\n")
    assert readme.index("## Version 0.68.10") < readme.index("## Version 0.62.0")
    assert "Dedicated history and candle-repair processes" in readme


def test_workers_ignore_console_sigint() -> None:
    root = Path(__file__).resolve().parents[1]
    for relative in (
        "src/axetos_market_data/history_worker.py",
        "src/axetos_market_data/repair_worker.py",
    ):
        source = (root / relative).read_text(encoding="utf-8")
        assert "signal.signal(signal.SIGINT, signal.SIG_IGN)" in source
