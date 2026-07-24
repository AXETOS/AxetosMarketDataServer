from datetime import UTC, datetime, timedelta

from axetos_market_data.config import ProviderConfig
from axetos_market_data.routing import ProviderAuthorityRegistry


def provider(key: str, priority: int, fallback: float = 10.0) -> ProviderConfig:
    return ProviderConfig(
        provider_key=key,
        display_name=key,
        symbols=["EUR/USD"],
        priority=priority,
        fallback_after_seconds=fallback,
    )


def test_highest_priority_fresh_provider_is_authoritative() -> None:
    registry = ProviderAuthorityRegistry()
    registry.replace_configs([provider("ICMarkets.MT5", 10), provider("Oanda.MT5", 20)])
    now = datetime.now(UTC)
    registry.record_tick("Oanda.MT5", "EUR/USD", now)
    registry.record_tick("ICMarkets.MT5", "EUR/USD", now)

    decision = registry.decision("EUR/USD", now)

    assert decision.provider_key == "ICMarkets.MT5"


def test_fallback_activates_when_primary_is_stale() -> None:
    registry = ProviderAuthorityRegistry()
    registry.replace_configs([provider("ICMarkets.MT5", 10, 5), provider("Oanda.MT5", 20, 5)])
    now = datetime.now(UTC)
    registry.record_tick("ICMarkets.MT5", "EUR/USD", now - timedelta(seconds=30))
    registry.record_tick("Oanda.MT5", "EUR/USD", now)

    decision = registry.decision("EUR/USD", now)

    assert decision.provider_key == "Oanda.MT5"


def test_disabled_provider_is_not_selected() -> None:
    primary = provider("ICMarkets.MT5", 10)
    primary.enabled = False
    registry = ProviderAuthorityRegistry()
    registry.replace_configs([primary, provider("Oanda.MT5", 20)])
    now = datetime.now(UTC)
    registry.record_tick("ICMarkets.MT5", "EUR/USD", now)
    registry.record_tick("Oanda.MT5", "EUR/USD", now)

    assert registry.decision("EUR/USD", now).provider_key == "Oanda.MT5"
