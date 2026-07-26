from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.web import create_app

ROOT = Path(__file__).resolve().parents[1]


def test_release_metadata() -> None:
    assert __version__ == "0.54.0"
    assert 'version = "0.54.0"' in (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.54.0" in (ROOT / "README.md").read_text(encoding="utf-8")


def test_mt5_heartbeat_accepts_bridge_camel_case(tmp_path) -> None:
    app = create_app(f"sqlite:///{tmp_path / 'camel.sqlite'}")
    with TestClient(app) as client:
        response = client.post(
            "/api/market-data/ingest/mt5/heartbeat",
            json={
                "providerKey": "ICMarkets.MT5",
                "terminalInstanceId": "terminal-1",
                "brokerName": "IC Markets",
                "serverName": "ICMarketsEU-Demo",
                "accountLogin": 52963557,
                "timeUtc": datetime.now(UTC).isoformat(),
            },
        )
        assert response.status_code == 200, response.text


def test_all_mt5_payload_models_accept_camel_case(tmp_path) -> None:
    app = create_app(f"sqlite:///{tmp_path / 'payloads.sqlite'}")
    now = datetime.now(UTC).isoformat()
    with TestClient(app) as client:
        instruments = client.post(
            "/api/market-data/ingest/mt5/instruments",
            json={
                "providerKey": "ICMarkets.MT5",
                "terminalInstanceId": "terminal-1",
                "timeUtc": now,
                "instruments": [{
                    "providerSymbol": "EURUSD",
                    "canonicalInstrument": "EUR/USD",
                    "digits": 5,
                    "point": 0.00001,
                    "isVisible": True,
                    "displayName": "EURUSD",
                    "description": "Euro vs US Dollar",
                    "path": "Forex\\Majors",
                    "assetClass": "Forex",
                    "isSelected": True,
                }],
            },
        )
        assert instruments.status_code == 200, instruments.text

        ticks = client.post(
            "/api/market-data/ingest/mt5/ticks",
            json={
                "providerKey": "ICMarkets.MT5",
                "terminalInstanceId": "terminal-1",
                "ticks": [{
                    "providerSymbol": "EURUSD",
                    "canonicalInstrument": "EUR/USD",
                    "timeUtc": now,
                    "bid": 1.1,
                    "ask": 1.10002,
                    "last": 1.10001,
                    "volume": 1,
                }],
            },
        )
        assert ticks.status_code == 202, ticks.text

        candles = client.post(
            "/api/market-data/ingest/mt5/candles",
            json={
                "providerKey": "ICMarkets.MT5",
                "terminalInstanceId": "terminal-1",
                "providerSymbol": "EURUSD",
                "canonicalInstrument": "EUR/USD",
                "interval": "1m",
                "candles": [{
                    "timeUtc": now,
                    "open": 1.1,
                    "high": 1.1001,
                    "low": 1.0999,
                    "close": 1.10002,
                    "tickVolume": 5,
                }],
            },
        )
        assert candles.status_code == 200, candles.text
