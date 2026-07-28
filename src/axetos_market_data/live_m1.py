from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import RLock
from uuid import uuid4


@dataclass(slots=True)
class LiveM1Request:
    request_id: str
    provider_key: str
    provider_symbol: str
    start: datetime
    end: datetime
    issued_at: datetime


class LiveM1CommandScheduler:
    """Server-side scheduler for the bridge's routine completed-M1 requests.

    The MT5 bridge has no minute scheduler. It only executes commands returned by
    the server. The server requests the previous two completed M1 candles once per
    configured symbol and minute, one command at a time per provider.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._active: dict[str, LiveM1Request] = {}
        self._last_completed_minute: dict[tuple[str, str], datetime] = {}
        self._cursor: dict[str, int] = {}

    def next_command(self, provider_key: str, symbols: list[str], now: datetime | None = None) -> str:
        now = (now or datetime.now(UTC)).astimezone(UTC)
        with self._lock:
            active = self._active.get(provider_key)
            if active is not None:
                # Re-issue the same command until the result acknowledgement arrives.
                return self._format(active)
            if not symbols:
                return ""
            completed = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
            start_index = self._cursor.get(provider_key, 0) % len(symbols)
            for offset in range(len(symbols)):
                index = (start_index + offset) % len(symbols)
                symbol = symbols[index]
                if self._last_completed_minute.get((provider_key, symbol)) == completed:
                    continue
                request = LiveM1Request(
                    request_id=f"live-{uuid4().hex}", provider_key=provider_key,
                    provider_symbol=symbol, start=completed - timedelta(minutes=1),
                    end=completed, issued_at=now,
                )
                self._active[provider_key] = request
                self._cursor[provider_key] = index + 1
                return self._format(request)
            return ""

    def accepts(self, provider_key: str, request_id: str) -> bool:
        with self._lock:
            active = self._active.get(provider_key)
            return active is not None and active.request_id == request_id

    def context(self, provider_key: str, request_id: str) -> dict[str, object] | None:
        with self._lock:
            active = self._active.get(provider_key)
            if active is None or active.request_id != request_id:
                return None
            return {
                "workflow": "live_m1",
                "phase": "live_m1",
                "instrument": active.provider_symbol,
                "timeframe": "1m",
                "from_utc": active.start,
                "to_utc": active.end,
            }

    def complete(self, provider_key: str, request_id: str, completed: bool) -> str:
        with self._lock:
            active = self._active.get(provider_key)
            if active is None or active.request_id != request_id:
                return "IGNORED"
            if completed:
                self._last_completed_minute[(provider_key, active.provider_symbol)] = active.end
                self._active.pop(provider_key, None)
                return "STORED"
            return "RETRY"

    @staticmethod
    def _format(request: LiveM1Request) -> str:
        return "|".join((
            "FETCH", request.provider_symbol, "1m",
            request.start.isoformat(), request.end.isoformat(), request.request_id,
        ))
