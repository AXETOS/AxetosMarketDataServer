from __future__ import annotations

import json
import re
import threading
from dataclasses import asdict, dataclass
from pathlib import Path

from .atomic_files import atomic_write_text


_ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def valid_password_env_name(value: str | None) -> bool:
    return bool(value and _ENV_NAME_RE.fullmatch(value))


@dataclass(slots=True)
class ProviderConfig:
    provider_key: str
    display_name: str
    kind: str = "mock"
    enabled: bool = True
    auto_start: bool = True
    poll_interval_seconds: float = 1.0
    symbols: list[str] | None = None
    symbol_aliases: dict[str, str] | None = None
    terminal_path: str | None = None
    account_login: int | None = None
    account_server: str | None = None
    password_env: str | None = None
    priority: int = 100
    fallback_after_seconds: float = 10.0
    batch_window_seconds: int = 5
    batch_limit: int = 50000
    maintenance_enabled: bool = False
    maintenance_interval_minutes: int = 60
    maintenance_backfill_days: int = 2
    feed_quiet_seconds: float = 60.0
    feed_stalled_seconds: float = 180.0
    feed_inactive_seconds: float = 600.0

    def normalized_symbols(self) -> list[str]:
        # MT5 symbols must be selected through the managed symbol workflow.
        # Other provider types retain the original default for compatibility.
        if self.symbols:
            return self.symbols
        return [] if self.kind.lower() == "mt5" else ["EUR/USD"]


class ConfigurationStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def read_all(self) -> list[ProviderConfig]:
        with self._lock:
            return self._read_all_unlocked()

    def _read_all_unlocked(self) -> list[ProviderConfig]:
        if not self.path.exists():
            return []
        data = json.loads(self.path.read_text(encoding="utf-8"))
        changed = False
        providers: list[ProviderConfig] = []
        for item in data.get("providers", []):
            item = dict(item)
            password_env = item.get("password_env")
            if password_env and not valid_password_env_name(str(password_env)):
                # Never retain a value that looks like an actual password in providers.json.
                item["password_env"] = None
                changed = True
            providers.append(ProviderConfig(**item))
        if changed:
            self._write_all_unlocked(providers)
        return providers

    def write_all(self, providers: list[ProviderConfig]) -> None:
        with self._lock:
            self._write_all_unlocked(providers)

    def _write_all_unlocked(self, providers: list[ProviderConfig]) -> None:
        payload = {"providers": [asdict(provider) for provider in providers]}
        atomic_write_text(self.path, json.dumps(payload, indent=2) + "\n")

    def upsert(self, config: ProviderConfig) -> ProviderConfig:
        with self._lock:
            providers = self._read_all_unlocked()
            providers = [
                provider
                for provider in providers
                if provider.provider_key.lower() != config.provider_key.lower()
            ]
            providers.append(config)
            providers.sort(key=lambda item: item.provider_key.lower())
            self._write_all_unlocked(providers)
            return config

    def delete(self, provider_key: str) -> bool:
        with self._lock:
            providers = self._read_all_unlocked()
            remaining = [
                provider
                for provider in providers
                if provider.provider_key.lower() != provider_key.lower()
            ]
            if len(remaining) == len(providers):
                return False
            self._write_all_unlocked(remaining)
            return True
