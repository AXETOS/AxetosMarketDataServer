from __future__ import annotations

import asyncio
import json
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import AsyncIterator

from .domain import Candle, Tick


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


@dataclass(frozen=True, slots=True)
class StreamFilter:
    instruments: frozenset[str] = frozenset()
    providers: frozenset[str] = frozenset()
    event_types: frozenset[str] = frozenset({"tick", "candle"})

    def accepts(self, event: dict[str, object]) -> bool:
        event_type = str(event.get("type", ""))
        if event_type not in self.event_types:
            return False
        provider = str(event.get("provider", ""))
        instrument = str(event.get("instrument", ""))
        if self.providers and provider not in self.providers:
            return False
        if self.instruments and instrument not in self.instruments:
            return False
        return True


@dataclass(slots=True)
class _Subscriber:
    loop: asyncio.AbstractEventLoop
    queue: asyncio.Queue[dict[str, object]]
    filter: StreamFilter


class LiveStreamHub:
    """Thread-safe in-process fan-out for live tick and candle consumers."""

    def __init__(self, queue_size: int = 1000) -> None:
        self.queue_size = max(10, int(queue_size))
        self._lock = threading.RLock()
        self._subscribers: dict[int, _Subscriber] = {}
        self._next_subscriber_id = 1
        self._sequence = 0
        self._published = 0
        self._dropped = 0

    def publish_tick(self, tick: Tick) -> None:
        self.publish({
            "type": "tick",
            "provider": tick.provider,
            "instrument": tick.instrument,
            "timestamp_utc": tick.timestamp,
            "bid": tick.bid,
            "ask": tick.ask,
            "mid": tick.mid,
            "volume": tick.volume,
        })

    def publish_candle(self, candle: Candle) -> None:
        payload = asdict(candle)
        payload["type"] = "candle"
        payload["open_time_utc"] = payload.pop("open_time")
        self.publish(payload)

    def publish(self, payload: dict[str, object]) -> None:
        with self._lock:
            self._sequence += 1
            event = {
                **payload,
                "sequence": self._sequence,
                "published_utc": datetime.now(UTC),
            }
            subscribers = list(self._subscribers.values())
            self._published += 1

        for subscriber in subscribers:
            if not subscriber.filter.accepts(event):
                continue
            subscriber.loop.call_soon_threadsafe(self._enqueue, subscriber.queue, event)

    def _enqueue(self, queue: asyncio.Queue[dict[str, object]], event: dict[str, object]) -> None:
        if queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            with self._lock:
                self._dropped += 1
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            with self._lock:
                self._dropped += 1

    async def subscribe(self, stream_filter: StreamFilter) -> AsyncIterator[dict[str, object]]:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[dict[str, object]] = asyncio.Queue(maxsize=self.queue_size)
        with self._lock:
            subscriber_id = self._next_subscriber_id
            self._next_subscriber_id += 1
            self._subscribers[subscriber_id] = _Subscriber(loop, queue, stream_filter)
        try:
            while True:
                yield await queue.get()
        finally:
            with self._lock:
                self._subscribers.pop(subscriber_id, None)

    def status(self) -> dict[str, int]:
        with self._lock:
            return {
                "subscribers": len(self._subscribers),
                "published_events": self._published,
                "dropped_events": self._dropped,
                "last_sequence": self._sequence,
                "queue_size": self.queue_size,
            }

    @staticmethod
    def sse(event: dict[str, object]) -> str:
        event_type = str(event.get("type", "message"))
        event_id = str(event.get("sequence", ""))
        data = json.dumps(event, default=_json_default, separators=(",", ":"))
        return f"id: {event_id}\nevent: {event_type}\ndata: {data}\n\n"
