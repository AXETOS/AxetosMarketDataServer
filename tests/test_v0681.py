from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.web import create_app


def test_release_metadata() -> None:
    assert __version__ == "0.68.12"


def test_provider_card_keeps_only_manual_full_history_button(tmp_path: Path) -> None:
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        html = client.get("/").text
    assert "Download full MT5 history" in html
    assert "Rebuild clean 7d" not in html
    assert "Repair gaps" not in html
    assert "Repair last 24h (M1)" not in html
    assert ">Restart</button>" not in html
    assert "Test connection" in html


def test_startup_refresh_is_authoritative_and_ten_minutes() -> None:
    source = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert 'minutes: int = 10' in source
    assert 'workflow="startup_m1"' in source
    assert 'in {"recent_m1", "startup_m1"}' in source
    assert 'replace_window=(context["from_utc"], context["to_utc"])' in source
