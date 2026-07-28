from pathlib import Path

from axetos_market_data import __version__


def test_provider_form_formats_structured_api_errors():
    source = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert __version__ == "0.62.2"
    assert "const apiError=" in source
    assert "Array.isArray(detail)" in source
    assert "error.textContent=apiError(payload" in source
    assert "[object Object]" not in source
