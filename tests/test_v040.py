from pathlib import Path

from axetos_market_data import __version__


ROOT = Path(__file__).resolve().parents[1]
BRIDGE = ROOT / "bridges" / "mt5" / "Experts" / "AxetosMarketDataBridge.mq5"


def test_release_metadata_is_v040() -> None:
    assert __version__ == "0.61.5"
    assert 'version = "0.61.5"' in (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.61.5" in (ROOT / "README.md").read_text(encoding="utf-8")


def test_mt5_bridge_source_is_distributed() -> None:
    assert BRIDGE.is_file()
    source = BRIDGE.read_text(encoding="utf-8")
    assert '#property version   "1.27"' in source
    assert 'InpServerUrl         = "http://127.0.0.1:8000"' in source


def test_mt5_get_uses_explicit_empty_dynamic_request_buffer() -> None:
    source = BRIDGE.read_text(encoding="utf-8")
    get_start = source.index("bool GetText(")
    post_start = source.index("bool PostJson(")
    get_source = source[get_start:post_start]
    assert "char data[];" in get_source
    assert "char result[];" in get_source
    assert "ArrayResize(data, 0);" in get_source
    assert 'WebRequest("GET"' in get_source


def test_mt5_post_removes_string_terminator_before_webrequest() -> None:
    source = BRIDGE.read_text(encoding="utf-8")
    post_start = source.index("bool PostJson(")
    post_end = source.index("string CanonicalSymbol(", post_start)
    post_source = source[post_start:post_end]
    assert "char data[];" in post_source
    assert "char result[];" in post_source
    assert "StringToCharArray(payload, data, 0, WHOLE_ARRAY, CP_UTF8)" in post_source
    assert "ArrayResize(data, length - 1);" in post_source
    assert 'WebRequest("POST"' in post_source
