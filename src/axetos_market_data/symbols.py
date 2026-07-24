from __future__ import annotations

import re
from dataclasses import dataclass

_KNOWN_QUOTES = (
    "USDT", "USDC", "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD",
    "CNH", "HKD", "SGD", "SEK", "NOK", "DKK", "PLN", "CZK", "HUF", "TRY",
    "ZAR", "MXN", "BRL",
)
_SUFFIXES = (
    ".PRO", ".RAW", ".ECN", ".A", ".B", ".C", ".M", ".R", ".I", ".S",
    "_PRO", "_RAW", "_ECN", "-PRO", "-RAW", "-ECN",
)
_ALIASES = {
    "XAUUSD": "XAU/USD",
    "GOLD": "XAU/USD",
    "XAGUSD": "XAG/USD",
    "SILVER": "XAG/USD",
    "BTCUSD": "BTC/USD",
    "BTCUSDT": "BTC/USDT",
    "ETHUSD": "ETH/USD",
    "ETHUSDT": "ETH/USDT",
    "USOIL": "WTI/USD",
    "WTI": "WTI/USD",
    "WTICOUSD": "WTI/USD",
    "UKOIL": "BRENT/USD",
    "BRENT": "BRENT/USD",
    "BRN": "BRENT/USD",
}


def _strip_broker_decoration(value: str) -> str:
    upper = value.strip().upper().replace(" ", "")
    for suffix in _SUFFIXES:
        if upper.endswith(suffix):
            return upper[: -len(suffix)]
    # Common broker suffixes are often a separator followed by 1-5 alphanumerics.
    match = re.fullmatch(r"(.+?)[._-]([A-Z0-9]{1,5})", upper)
    if match and len(match.group(1)) >= 6:
        return match.group(1)
    return upper


def normalize_instrument(symbol: str) -> str:
    """Return a deterministic canonical instrument identity.

    The function is deliberately conservative: explicit separators are retained,
    known broker decorations are removed, known aliases are mapped, and compact
    base/quote symbols are split only when the quote currency is recognizable.
    """
    if not symbol or not symbol.strip():
        raise ValueError("symbol is required")
    raw = _strip_broker_decoration(symbol)
    compact = re.sub(r"[/_:-]", "", raw)
    if compact in _ALIASES:
        return _ALIASES[compact]
    if "/" in raw:
        left, right = raw.split("/", 1)
        if left and right:
            return f"{left}/{right}"
    for separator in ("_", ":"):
        if separator in raw:
            left, right = raw.split(separator, 1)
            if left and right:
                return f"{left}/{right}"
    for quote in _KNOWN_QUOTES:
        if compact.endswith(quote) and len(compact) > len(quote):
            base = compact[: -len(quote)]
            if 2 <= len(base) <= 8 and base.isalnum():
                return f"{base}/{quote}"
    return raw


@dataclass(frozen=True, slots=True)
class SymbolResolution:
    provider_key: str
    provider_symbol: str
    canonical_instrument: str
    source: str


class SymbolResolver:
    def __init__(self, store=None, aliases: dict[str, str] | None = None) -> None:
        self.store = store
        self.aliases = {key.upper(): normalize_instrument(value) for key, value in (aliases or {}).items()}

    def resolve(self, provider_key: str, provider_symbol: str, reported: str | None = None) -> SymbolResolution:
        if self.store is not None:
            policy = self.store.get_symbol_policy(provider_key, provider_symbol)
            if policy and policy.get("enabled", True):
                return SymbolResolution(provider_key, provider_symbol, normalize_instrument(str(policy["canonical_instrument"])), "policy")
        explicit = self.aliases.get(provider_symbol.upper())
        if explicit:
            return SymbolResolution(provider_key, provider_symbol, explicit, "configuration")
        candidate = reported.strip() if reported and reported.strip() else provider_symbol
        return SymbolResolution(provider_key, provider_symbol, normalize_instrument(candidate), "automatic")
