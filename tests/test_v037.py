from __future__ import annotations

from pathlib import Path

import pytest

from axetos_market_data import __version__
from axetos_market_data.security import SecuritySettings


TOKEN_NAMES = (
    "AXETOS_VIEWER_TOKEN",
    "AXETOS_OPERATOR_TOKEN",
    "AXETOS_ADMIN_TOKEN",
    "AXETOS_BRIDGE_TOKEN",
)


def clear_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in TOKEN_NAMES:
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(f"{name}_FILE", raising=False)


def test_tokens_can_be_loaded_from_secret_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    clear_tokens(monkeypatch)
    admin = tmp_path / "admin-token"
    bridge = tmp_path / "bridge-token"
    admin.write_text("admin-from-file\n", encoding="utf-8")
    bridge.write_text("bridge-from-file\n", encoding="utf-8")
    monkeypatch.setenv("AXETOS_AUTH_ENABLED", "true")
    monkeypatch.setenv("AXETOS_ADMIN_TOKEN_FILE", str(admin))
    monkeypatch.setenv("AXETOS_BRIDGE_TOKEN_FILE", str(bridge))

    settings = SecuritySettings.from_environment()

    assert settings.administrator_token == "admin-from-file"
    assert settings.bridge_token == "bridge-from-file"
    settings.validate()


def test_direct_and_file_secret_are_mutually_exclusive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clear_tokens(monkeypatch)
    token_file = tmp_path / "admin-token"
    token_file.write_text("file-value", encoding="utf-8")
    monkeypatch.setenv("AXETOS_ADMIN_TOKEN", "direct-value")
    monkeypatch.setenv("AXETOS_ADMIN_TOKEN_FILE", str(token_file))

    with pytest.raises(RuntimeError, match="Configure only one"):
        SecuritySettings.from_environment()


def test_missing_secret_file_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    clear_tokens(monkeypatch)
    monkeypatch.setenv("AXETOS_ADMIN_TOKEN_FILE", str(tmp_path / "missing"))

    with pytest.raises(RuntimeError, match="Unable to read AXETOS_ADMIN_TOKEN_FILE"):
        SecuritySettings.from_environment()


def test_empty_secret_file_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    clear_tokens(monkeypatch)
    token_file = tmp_path / "empty"
    token_file.write_text("\n", encoding="utf-8")
    monkeypatch.setenv("AXETOS_ADMIN_TOKEN_FILE", str(token_file))

    with pytest.raises(RuntimeError, match="is empty"):
        SecuritySettings.from_environment()


def test_authentication_tokens_must_be_distinct() -> None:
    settings = SecuritySettings(
        enabled=True,
        viewer_token=None,
        operator_token="shared-token",
        administrator_token="admin-token",
        bridge_token="shared-token",
    )

    with pytest.raises(RuntimeError, match="must be distinct"):
        settings.validate()


def test_v037_release_metadata() -> None:
    root = Path(__file__).resolve().parents[1]
    assert __version__ == "0.64.1"
    assert 'version = "0.64.1"' in (root / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.64.1" in (root / "README.md").read_text(encoding="utf-8")
