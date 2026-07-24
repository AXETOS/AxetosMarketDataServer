from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(slots=True)
class ProviderConfig:
    provider_key: str
    display_name: str
    kind: str = "mock"
    enabled: bool = True
    auto_start: bool = True
    poll_interval_seconds: float = 1.0
    symbols: list[str] | None = None
    terminal_path: str | None = None
    priority: int = 100
    fallback_after_seconds: float = 10.0

    def normalized_symbols(self) -> list[str]:
        return self.symbols or ["EUR/USD"]


class ConfigurationStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def read_all(self) -> list[ProviderConfig]:
        if not self.path.exists():
            return []
        data = json.loads(self.path.read_text(encoding="utf-8"))
        return [ProviderConfig(**item) for item in data.get("providers", [])]

    def write_all(self, providers: list[ProviderConfig]) -> None:
        payload = {"providers": [asdict(provider) for provider in providers]}
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def upsert(self, config: ProviderConfig) -> ProviderConfig:
        providers = self.read_all()
        providers = [p for p in providers if p.provider_key.lower() != config.provider_key.lower()]
        providers.append(config)
        providers.sort(key=lambda x: x.provider_key.lower())
        self.write_all(providers)
        return config

    def delete(self, provider_key: str) -> bool:
        providers = self.read_all()
        remaining = [p for p in providers if p.provider_key.lower() != provider_key.lower()]
        if len(remaining) == len(providers):
            return False
        self.write_all(remaining)
        return True
