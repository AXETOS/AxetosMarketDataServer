from __future__ import annotations

from pathlib import Path

import pytest

from axetos_market_data import __version__
from axetos_market_data.security import SecuritySettings
from axetos_market_data.server import is_loopback_host, validate_network_startup


def settings(*, enabled: bool, admin: str | None = None, bridge: str | None = None) -> SecuritySettings:
    return SecuritySettings(
        enabled=enabled,
        viewer_token=None,
        operator_token=None,
        administrator_token=admin,
        bridge_token=bridge,
    )


@pytest.mark.parametrize("host", ["127.0.0.1", "127.12.34.56", "::1", "localhost", "LOCALHOST"])
def test_loopback_hosts_are_recognized(host: str) -> None:
    assert is_loopback_host(host)


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "192.168.1.20", "market-data.internal"])
def test_public_hosts_are_not_treated_as_loopback(host: str) -> None:
    assert not is_loopback_host(host)


def test_unauthenticated_public_bind_is_rejected() -> None:
    with pytest.raises(RuntimeError, match="Refusing to bind an unauthenticated server"):
        validate_network_startup("0.0.0.0", settings(enabled=False))


def test_unauthenticated_loopback_bind_is_allowed() -> None:
    validate_network_startup("127.0.0.1", settings(enabled=False))


def test_authenticated_public_bind_is_allowed() -> None:
    validate_network_startup(
        "0.0.0.0",
        settings(enabled=True, admin="admin-token", bridge="bridge-token"),
    )


def test_explicit_insecure_public_override_is_allowed() -> None:
    validate_network_startup(
        "0.0.0.0",
        settings(enabled=False),
        allow_insecure_public_bind=True,
    )


def test_authentication_configuration_is_validated_before_binding() -> None:
    with pytest.raises(RuntimeError, match="AXETOS_ADMIN_TOKEN"):
        validate_network_startup("127.0.0.1", settings(enabled=True))


def test_v036_release_metadata() -> None:
    root = Path(__file__).resolve().parents[1]
    assert __version__ == "0.67.0"
    assert 'version = "0.67.0"' in (root / "pyproject.toml").read_text(encoding="utf-8")
    assert "## Version 0.67.0" in (root / "README.md").read_text(encoding="utf-8")
