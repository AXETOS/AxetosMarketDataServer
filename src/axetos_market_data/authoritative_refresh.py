from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from threading import RLock

from .bridge import BridgeCandle, BridgeCandlesRequest


@dataclass(slots=True)
class _PendingRefresh:
    created_at: datetime
    provider_key: str
    terminal_instance_id: str
    provider_symbol: str
    canonical_instrument: str
    interval: str
    request_id: str
    chunk_count: int
    chunks: dict[int, list[BridgeCandle]] = field(default_factory=dict)


class AuthoritativeRefreshBuffer:
    """Collect every chunk before an authoritative window replacement.

    The existing database window remains untouched until all MT5 chunks have arrived.
    Once complete, the caller receives one assembled request and can replace the window
    atomically. Duplicate chunk deliveries replace the buffered copy and remain safe.
    """

    def __init__(self, *, ttl: timedelta = timedelta(minutes=15)) -> None:
        self._ttl = ttl
        self._lock = RLock()
        self._pending: dict[tuple[str, str], _PendingRefresh] = {}

    def add(self, request: BridgeCandlesRequest) -> BridgeCandlesRequest | None:
        if not request.request_id:
            raise ValueError("Authoritative refresh requires a request ID")
        if request.chunk_count < 1:
            raise ValueError("Authoritative refresh chunk count must be positive")
        if request.chunk_index < 1 or request.chunk_index > request.chunk_count:
            raise ValueError("Authoritative refresh chunk index is outside the declared range")

        now = datetime.now(UTC)
        key = (request.provider_key, request.request_id)
        with self._lock:
            self._expire(now)
            state = self._pending.get(key)
            if state is None:
                state = _PendingRefresh(
                    created_at=now,
                    provider_key=request.provider_key,
                    terminal_instance_id=request.terminal_instance_id,
                    provider_symbol=request.provider_symbol,
                    canonical_instrument=request.canonical_instrument,
                    interval=request.interval,
                    request_id=request.request_id,
                    chunk_count=request.chunk_count,
                )
                self._pending[key] = state
            else:
                identity = (
                    state.terminal_instance_id,
                    state.provider_symbol,
                    state.canonical_instrument,
                    state.interval,
                    state.chunk_count,
                )
                incoming_identity = (
                    request.terminal_instance_id,
                    request.provider_symbol,
                    request.canonical_instrument,
                    request.interval,
                    request.chunk_count,
                )
                if identity != incoming_identity:
                    raise ValueError("Authoritative refresh chunk metadata changed during upload")

            state.chunks[request.chunk_index] = list(request.candles)
            if len(state.chunks) != state.chunk_count:
                return None

            candles: list[BridgeCandle] = []
            for index in range(1, state.chunk_count + 1):
                chunk = state.chunks.get(index)
                if chunk is None:
                    return None
                candles.extend(chunk)
            del self._pending[key]

        return request.model_copy(update={
            "candles": candles,
            "chunk_index": 1,
            "chunk_count": 1,
            "authoritative": True,
        })

    def discard(self, provider_key: str, request_id: str) -> None:
        with self._lock:
            self._pending.pop((provider_key, request_id), None)

    def _expire(self, now: datetime) -> None:
        expired = [
            key for key, value in self._pending.items()
            if now - value.created_at >= self._ttl
        ]
        for key in expired:
            del self._pending[key]
