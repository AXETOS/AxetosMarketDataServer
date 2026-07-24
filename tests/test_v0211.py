from axetos_market_data import __version__
from axetos_market_data.web import CONTROL_CENTER_HTML


def test_v0211_symbol_dialog_layout():
    assert __version__ == "0.27.0"
    assert "grid-template-columns:minmax(190px,1.25fr)" in CONTROL_CENTER_HTML
    assert "Canonical instrument" in CONTROL_CENTER_HTML
    assert "Mapping state" in CONTROL_CENTER_HTML
    assert "class=\"symbol-info\"" in CONTROL_CENTER_HTML
    assert "class=\"symbol-save\"" in CONTROL_CENTER_HTML
