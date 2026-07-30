from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.web import create_app


def test_release_metadata() -> None:
    assert __version__ == "1.0.0"
    assert 'version = "1.0.0"' in Path("pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 1.0.0" in Path("README.md").read_text(encoding="utf-8")


def test_manual_full_history_button_is_restored_without_other_removed_controls(tmp_path: Path) -> None:
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        html = client.get("/").text
    assert "Download full MT5 history" in html
    assert "fullHistory(" in html
    assert "Rebuild clean 7d" not in html
    assert "Repair gaps" not in html
    assert "Repair last 24h (M1)" not in html
    assert ">Restart</button>" not in html
