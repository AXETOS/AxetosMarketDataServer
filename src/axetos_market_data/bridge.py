from __future__ import annotations

import queue
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Iterable

from pydantic import BaseModel, Field

from .domain import Candle, Tick
from .service import MarketDataService
from .storage import MarketDataStore


class BridgeHeartbeatRequest(BaseModel):
    provider_key: str = Field(alias="ProviderKey")
    terminal_instance_id: str = Field(alias="TerminalInstanceId")
    broker_name: str | None = Field(default=None, alias="BrokerName")
    server_name: str | None = Field(default=None, alias="ServerName")
    account_login: int | None = Field(default=None, alias="AccountLogin")
    time_utc: datetime = Field(alias="TimeUtc")
    model_config = {"populate_by_name": True}


class BridgeInstrument(BaseModel):
    provider_symbol: str = Field(alias="ProviderSymbol")
    canonical_instrument: str = Field(alias="CanonicalInstrument")
    digits: int = Field(default=0, alias="Digits")
    point: Decimal = Field(default=Decimal("0"), alias="Point")
    is_visible: bool = Field(default=True, alias="IsVisible")
    display_name: str | None = Field(default=None, alias="DisplayName")
    description: str | None = Field(default=None, alias="Description")
    path: str | None = Field(default=None, alias="Path")
    asset_class: str | None = Field(default=None, alias="AssetClass")
    is_selected: bool = Field(default=False, alias="IsSelected")
    model_config = {"populate_by_name": True}


class BridgeInstrumentsRequest(BaseModel):
    provider_key: str = Field(alias="ProviderKey")
    terminal_instance_id: str = Field(alias="TerminalInstanceId")
    time_utc: datetime = Field(alias="TimeUtc")
    instruments: list[BridgeInstrument] = Field(default_factory=list, alias="Instruments")
    model_config = {"populate_by_name": True}


class BridgeTick(BaseModel):
    provider_symbol: str = Field(alias="ProviderSymbol")
    canonical_instrument: str = Field(alias="CanonicalInstrument")
    time_utc: datetime = Field(alias="TimeUtc")
    bid: Decimal = Field(alias="Bid")
    ask: Decimal = Field(alias="Ask")
    last: Decimal | None = Field(default=None, alias="Last")
    volume: Decimal | None = Field(default=None, alias="Volume")
    received_utc: datetime | None = Field(default=None, alias="ReceivedUtc")
    model_config = {"populate_by_name": True}


class BridgeTicksRequest(BaseModel):
    provider_key: str = Field(alias="ProviderKey")
    terminal_instance_id: str = Field(alias="TerminalInstanceId")
    ticks: list[BridgeTick] = Field(default_factory=list, alias="Ticks")
    model_config = {"populate_by_name": True}


class BridgeQuotesRequest(BaseModel):
    provider_key: str = Field(alias="ProviderKey")
    terminal_instance_id: str = Field(alias="TerminalInstanceId")
    quotes: list[BridgeTick] = Field(default_factory=list, alias="Quotes")
    model_config = {"populate_by_name": True}


class BridgeCandle(BaseModel):
    time_utc: datetime = Field(alias="TimeUtc")
    open: Decimal = Field(alias="Open")
    high: Decimal = Field(alias="High")
    low: Decimal = Field(alias="Low")
    close: Decimal = Field(alias="Close")
    tick_volume: int | None = Field(default=None, alias="TickVolume")
    model_config = {"populate_by_name": True}


class BridgeCandlesRequest(BaseModel):
    provider_key: str = Field(alias="ProviderKey")
    terminal_instance_id: str = Field(alias="TerminalInstanceId")
    provider_symbol: str = Field(alias="ProviderSymbol")
    canonical_instrument: str = Field(alias="CanonicalInstrument")
    interval: str = Field(alias="Interval")
    candles: list[BridgeCandle] = Field(default_factory=list, alias="Candles")
    model_config = {"populate_by_name": True}


class InstrumentSelectionRequest(BaseModel):
    provider_key: str = Field(alias="ProviderKey")
    terminal_instance_id: str = Field(alias="TerminalInstanceId")
    provider_symbol: str = Field(alias="ProviderSymbol")
    enabled: bool = Field(alias="Enabled")
    model_config = {"populate_by_name": True}


@dataclass(slots=True)
class QueueStats:
    queued_batches: int = 0
    queued_ticks: int = 0
    processed_ticks: int = 0
    rejected_ticks: int = 0
    queue_depth: int = 0
    last_batch_utc: str | None = None
    last_error: str | None = None


class Mt5BridgeService:
    ALLOWED_INTERVALS = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}

    def __init__(self, store: MarketDataStore, max_queue_batches: int = 1000) -> None:
        self.store = store
        self._queue: queue.Queue[BridgeTicksRequest | None] = queue.Queue(maxsize=max_queue_batches)
        self.stats = QueueStats()
        self._thread = threading.Thread(target=self._consume, daemon=True, name="mt5-bridge-ingestion")
        self._thread.start()

    @staticmethod
    def validate_identity(provider_key: str, terminal_instance_id: str) -> None:
        if not provider_key.strip() or not terminal_instance_id.strip():
            raise ValueError("ProviderKey and TerminalInstanceId are required")

    def shutdown(self) -> None:
        try: self._queue.put_nowait(None)
        except queue.Full: return
        self._thread.join(timeout=5)

    def heartbeat(self, request: BridgeHeartbeatRequest) -> None:
        self.validate_identity(request.provider_key, request.terminal_instance_id)
        self.store.upsert_bridge_heartbeat(request.model_dump(by_alias=False))

    def instruments(self, request: BridgeInstrumentsRequest) -> int:
        self.validate_identity(request.provider_key, request.terminal_instance_id)
        rows = [x.model_dump(by_alias=False) for x in request.instruments if x.provider_symbol.strip() and x.canonical_instrument.strip()]
        self.store.upsert_bridge_instruments(request.provider_key, request.terminal_instance_id, request.time_utc, rows)
        return len(rows)

    def enqueue_ticks(self, request: BridgeTicksRequest) -> int:
        self.validate_identity(request.provider_key, request.terminal_instance_id)
        if not request.ticks: return 0
        try: self._queue.put_nowait(request)
        except queue.Full as exc: raise RuntimeError("MT5 ingestion queue is full") from exc
        self.stats.queued_batches += 1; self.stats.queued_ticks += len(request.ticks); self.stats.queue_depth = self._queue.qsize()
        return len(request.ticks)

    def quotes(self, request: BridgeQuotesRequest) -> int:
        self.validate_identity(request.provider_key, request.terminal_instance_id)
        accepted = 0
        now = datetime.now(UTC)
        for item in request.quotes:
            if item.bid <= 0 or item.ask < item.bid: continue
            self.store.upsert_bridge_quote(request.provider_key, request.terminal_instance_id, item.model_dump(by_alias=False), now)
            accepted += 1
        return accepted

    def candles(self, request: BridgeCandlesRequest) -> int:
        self.validate_identity(request.provider_key, request.terminal_instance_id)
        interval = request.interval.lower()
        if interval not in self.ALLOWED_INTERVALS: raise ValueError(f"Unsupported MT5 interval '{request.interval}'")
        values=[]
        for item in request.candles:
            try:
                values.append(Candle(request.provider_key, request.canonical_instrument, interval, item.time_utc, item.open, item.high, item.low, item.close, item.tick_volume or 0, Decimal(item.tick_volume or 0), True))
            except ValueError: continue
        return self.store.upsert_candles(values)

    def _consume(self) -> None:
        services: dict[tuple[str,str], MarketDataService] = {}
        while True:
            request=self._queue.get()
            if request is None: break
            try:
                key=(request.provider_key, request.terminal_instance_id)
                service=services.setdefault(key, MarketDataService(self.store))
                ticks=[]
                for item in request.ticks:
                    try:
                        ticks.append(Tick(request.provider_key, item.canonical_instrument, item.received_utc or datetime.now(UTC), item.bid, item.ask, item.volume))
                    except ValueError: self.stats.rejected_ticks += 1
                ticks.sort(key=lambda x:x.timestamp)
                if ticks: service.run(ticks)
                self.stats.processed_ticks += len(ticks); self.stats.last_batch_utc=datetime.now(UTC).isoformat(); self.stats.last_error=None
            except Exception as exc:
                self.stats.last_error=str(exc)
            finally:
                self._queue.task_done(); self.stats.queue_depth=self._queue.qsize()

    def view(self) -> dict[str, object]:
        self.stats.queue_depth=self._queue.qsize()
        return {k:getattr(self.stats,k) for k in self.stats.__slots__}
