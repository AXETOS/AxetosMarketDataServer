from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SymbolPolicy:
    provider_key: str
    provider_symbol: str
    canonical_instrument: str
    enabled: bool = True
    allow_live: bool = True
    allow_history: bool = True
    priority_override: int | None = None


def choose_canonical_source(
    instrument: str,
    providers: list[dict[str, object]],
    policies: list[dict[str, object]],
) -> dict[str, object]:
    """Return a deterministic preferred source without blending provider feeds."""
    by_key = {str(p["provider_key"]): p for p in providers if bool(p.get("enabled", True))}
    candidates: list[dict[str, object]] = []
    for policy in policies:
        if str(policy["canonical_instrument"]) != instrument or not bool(policy["enabled"]):
            continue
        provider_key = str(policy["provider_key"])
        provider = by_key.get(provider_key)
        if provider is None:
            continue
        priority = policy.get("priority_override")
        if priority is None:
            priority = int(provider.get("priority", 100))
        candidates.append({
            "provider_key": provider_key,
            "provider_symbol": str(policy["provider_symbol"]),
            "canonical_instrument": instrument,
            "priority": int(priority),
            "allow_live": bool(policy["allow_live"]),
            "allow_history": bool(policy["allow_history"]),
        })
    candidates.sort(key=lambda x: (int(x["priority"]), str(x["provider_key"]).lower(), str(x["provider_symbol"])))
    return {
        "instrument": instrument,
        "preferred": candidates[0] if candidates else None,
        "candidates": candidates,
        "policy": "lowest effective priority wins; feeds are never blended",
    }
