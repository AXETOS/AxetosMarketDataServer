from pathlib import Path

from axetos_market_data import __version__


def test_manage_symbols_keeps_discovery_snapshot_while_dialog_is_open() -> None:
    source = Path("src/axetos_market_data/web.py").read_text(encoding="utf-8")
    assert __version__ == "0.68.5"
    assert "symbolDiscoveryActive" in source
    assert "async function discoverSymbols()" in source
    assert "if(symbolDiscoveryActive&&target)" in source
    assert "target.canonical_instrument=canonical" in source
    assert "await loadSymbols(false)" in source
    assert "symbolItems=[];symbolDuplicateCount=0;symbolDiscoveryActive=false" in source
