from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.server import build_parser

ROOT = Path(__file__).resolve().parents[1]
BRIDGE = ROOT / "bridges" / "mt5" / "Experts" / "AxetosMarketDataBridge.mq5"


def test_release_metadata_is_v041() -> None:
    assert __version__ == "0.68.5"
    assert 'version = "0.68.5"' in (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.68.5" in (ROOT / "README.md").read_text(encoding="utf-8")


def test_access_logging_is_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("AXETOS_ACCESS_LOG", raising=False)
    args = build_parser().parse_args([])
    assert args.access_log is False


def test_access_logging_can_be_enabled_by_cli() -> None:
    args = build_parser().parse_args(["--access-log"])
    assert args.access_log is True


def test_bridge_captures_webrequest_error_before_response_decode() -> None:
    source = BRIDGE.read_text(encoding="utf-8")
    assert '#property version   "3.04"' in source
    assert source.index("int error_code = GetLastError();") < source.index("CharArrayToString(result")
    assert 'WebRequest("GET"' in source

def test_bridge_has_independent_channels_and_transport_self_test() -> None:
    source = BRIDGE.read_text(encoding="utf-8")
    assert "HttpGet" in source
    assert "HttpPost" in source
    assert "g_http_retry_after" not in source
    assert "PollCommand" in source
    assert "SendTicks" in source
