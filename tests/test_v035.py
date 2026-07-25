from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from axetos_market_data import __version__
from axetos_market_data.atomic_files import atomic_write_text
from axetos_market_data.config import ConfigurationStore, ProviderConfig
from axetos_market_data.secrets import SecretStore


def test_atomic_write_preserves_target_when_replace_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "providers.json"
    target.write_text("original\n", encoding="utf-8")

    def fail_replace(source: str | Path, destination: str | Path) -> None:
        raise OSError("simulated replace failure")

    monkeypatch.setattr(os, "replace", fail_replace)

    with pytest.raises(OSError, match="simulated replace failure"):
        atomic_write_text(target, "replacement\n")

    assert target.read_text(encoding="utf-8") == "original\n"
    assert list(tmp_path.glob(".providers.json.*.tmp")) == []


def test_configuration_store_uses_complete_atomic_json_document(tmp_path: Path) -> None:
    path = tmp_path / "providers.json"
    store = ConfigurationStore(path)
    store.write_all([
        ProviderConfig("ICMarkets.MT5", "IC Markets", kind="mt5"),
        ProviderConfig("Oanda.MT5", "OANDA", kind="mt5"),
    ])

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert [item["provider_key"] for item in payload["providers"]] == [
        "ICMarkets.MT5",
        "Oanda.MT5",
    ]
    assert list(tmp_path.glob(".providers.json.*.tmp")) == []


def test_secret_store_atomic_write_keeps_private_permissions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AXETOS_SECRET_KEY", "test-only-machine-local-key")
    path = tmp_path / "mt5_secrets.json"
    store = SecretStore(path)

    store.set("Oanda.MT5", "secret-password")

    assert store.get("Oanda.MT5") == "secret-password"
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600
    assert list(tmp_path.glob(".mt5_secrets.json.*.tmp")) == []


def test_v035_release_metadata() -> None:
    root = Path(__file__).resolve().parents[1]
    assert __version__ == "0.39.0"
    assert 'version = "0.39.0"' in (root / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.39.0" in (root / "README.md").read_text(encoding="utf-8")
