from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import UTC, datetime

from .config import ProviderConfig


@dataclass(frozen=True, slots=True)
class SourceDecision:
    instrument: str
    provider_key: str | None
    reason: str
    candidates: tuple[str, ...]


class ProviderAuthorityRegistry:
    """Selects one authoritative provider per instrument without blending feeds."""

    def __init__(self) -> None:
        self._configs: dict[str, ProviderConfig] = {}
        self._last_ticks: dict[tuple[str, str], datetime] = {}
        self._lock = threading.RLock()

    def replace_configs(self, configs: list[ProviderConfig]) -> None:
        with self._lock:
            self._configs = {item.provider_key: item for item in configs}

    def record_tick(self, provider_key: str, instrument: str, timestamp: datetime) -> None:
        with self._lock:
            self._last_ticks[(provider_key, instrument)] = timestamp.astimezone(UTC)

    def decision(self, instrument: str, now: datetime | None = None) -> SourceDecision:
        now = (now or datetime.now(UTC)).astimezone(UTC)
        with self._lock:
            candidates = [
                config for config in self._configs.values()
                if config.enabled and instrument in config.normalized_symbols()
            ]
            candidates.sort(key=lambda item: (item.priority, item.provider_key.lower()))
            candidate_keys = tuple(item.provider_key for item in candidates)
            for config in candidates:
                last = self._last_ticks.get((config.provider_key, instrument))
                if last is None:
                    continue
                age = max(0.0, (now - last).total_seconds())
                if age <= config.fallback_after_seconds:
                    return SourceDecision(instrument, config.provider_key, "fresh highest-priority source", candidate_keys)
            if candidates:
                return SourceDecision(instrument, candidates[0].provider_key, "no fresh source; preferred provider retained", candidate_keys)
            return SourceDecision(instrument, None, "no enabled provider configured", candidate_keys)

    def is_authoritative(self, provider_key: str, instrument: str, now: datetime | None = None) -> bool:
        return self.decision(instrument, now).provider_key == provider_key

    def snapshot(self) -> list[dict[str, object]]:
        instruments = sorted({symbol for config in self._configs.values() for symbol in config.normalized_symbols()})
        return [
            {
                "instrument": decision.instrument,
                "provider_key": decision.provider_key,
                "reason": decision.reason,
                "candidates": list(decision.candidates),
            }
            for decision in (self.decision(instrument) for instrument in instruments)
        ]
