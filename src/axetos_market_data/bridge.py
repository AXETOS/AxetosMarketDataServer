from __future__ import annotations

import queue
import threading
from threading import RLock
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Iterable

from pydantic import AliasChoices, BaseModel, Field

from .domain import Candle, Tick
from .service import MarketDataService
from .storage import MarketDataStore
from .symbols import SymbolResolver


class BridgeHeartbeatRequest(BaseModel):
    provider_key: str = Field(validation_alias=AliasChoices("ProviderKey", "providerKey"), serialization_alias="ProviderKey")
    terminal_instance_id: str = Field(validation_alias=AliasChoices("TerminalInstanceId", "terminalInstanceId"), serialization_alias="TerminalInstanceId")
    broker_name: str | None = Field(default=None, validation_alias=AliasChoices("BrokerName", "brokerName"), serialization_alias="BrokerName")
    server_name: str | None = Field(default=None, validation_alias=AliasChoices("ServerName", "serverName"), serialization_alias="ServerName")
    account_login: int | None = Field(default=None, validation_alias=AliasChoices("AccountLogin", "accountLogin"), serialization_alias="AccountLogin")
    time_utc: datetime = Field(validation_alias=AliasChoices("TimeUtc", "timeUtc"), serialization_alias="TimeUtc")
    model_config = {"populate_by_name": True}


class BridgeInstrument(BaseModel):
    provider_symbol: str = Field(validation_alias=AliasChoices("ProviderSymbol", "providerSymbol"), serialization_alias="ProviderSymbol")
    canonical_instrument: str = Field(validation_alias=AliasChoices("CanonicalInstrument", "canonicalInstrument"), serialization_alias="CanonicalInstrument")
    digits: int = Field(default=0, validation_alias=AliasChoices("Digits", "digits"), serialization_alias="Digits")
    point: Decimal = Field(default=Decimal("0"), validation_alias=AliasChoices("Point", "point"), serialization_alias="Point")
    is_visible: bool = Field(default=True, validation_alias=AliasChoices("IsVisible", "isVisible"), serialization_alias="IsVisible")
    display_name: str | None = Field(default=None, validation_alias=AliasChoices("DisplayName", "displayName"), serialization_alias="DisplayName")
    description: str | None = Field(default=None, validation_alias=AliasChoices("Description", "description"), serialization_alias="Description")
    path: str | None = Field(default=None, validation_alias=AliasChoices("Path", "path"), serialization_alias="Path")
    asset_class: str | None = Field(default=None, validation_alias=AliasChoices("AssetClass", "assetClass"), serialization_alias="AssetClass")
    is_selected: bool = Field(default=False, validation_alias=AliasChoices("IsSelected", "isSelected"), serialization_alias="IsSelected")
    model_config = {"populate_by_name": True}


class BridgeInstrumentsRequest(BaseModel):
    provider_key: str = Field(validation_alias=AliasChoices("ProviderKey", "providerKey"), serialization_alias="ProviderKey")
    terminal_instance_id: str = Field(validation_alias=AliasChoices("TerminalInstanceId", "terminalInstanceId"), serialization_alias="TerminalInstanceId")
    time_utc: datetime = Field(validation_alias=AliasChoices("TimeUtc", "timeUtc"), serialization_alias="TimeUtc")
    instruments: list[BridgeInstrument] = Field(default_factory=list, validation_alias=AliasChoices("Instruments", "instruments"), serialization_alias="Instruments")
    model_config = {"populate_by_name": True}


class BridgeTick(BaseModel):
    provider_symbol: str = Field(validation_alias=AliasChoices("ProviderSymbol", "providerSymbol"), serialization_alias="ProviderSymbol")
    canonical_instrument: str = Field(validation_alias=AliasChoices("CanonicalInstrument", "canonicalInstrument"), serialization_alias="CanonicalInstrument")
    time_utc: datetime = Field(validation_alias=AliasChoices("TimeUtc", "timeUtc"), serialization_alias="TimeUtc")
    bid: Decimal = Field(validation_alias=AliasChoices("Bid", "bid"), serialization_alias="Bid")
    ask: Decimal = Field(validation_alias=AliasChoices("Ask", "ask"), serialization_alias="Ask")
    last: Decimal | None = Field(default=None, validation_alias=AliasChoices("Last", "last"), serialization_alias="Last")
    volume: Decimal | None = Field(default=None, validation_alias=AliasChoices("Volume", "volume"), serialization_alias="Volume")
    received_utc: datetime | None = Field(default=None, validation_alias=AliasChoices("ReceivedUtc", "receivedUtc"), serialization_alias="ReceivedUtc")
    model_config = {"populate_by_name": True}


class BridgeTicksRequest(BaseModel):
    provider_key: str = Field(validation_alias=AliasChoices("ProviderKey", "providerKey"), serialization_alias="ProviderKey")
    terminal_instance_id: str = Field(validation_alias=AliasChoices("TerminalInstanceId", "terminalInstanceId"), serialization_alias="TerminalInstanceId")
    ticks: list[BridgeTick] = Field(default_factory=list, validation_alias=AliasChoices("Ticks", "ticks"), serialization_alias="Ticks")
    model_config = {"populate_by_name": True}


class BridgeQuotesRequest(BaseModel):
    provider_key: str = Field(validation_alias=AliasChoices("ProviderKey", "providerKey"), serialization_alias="ProviderKey")
    terminal_instance_id: str = Field(validation_alias=AliasChoices("TerminalInstanceId", "terminalInstanceId"), serialization_alias="TerminalInstanceId")
    quotes: list[BridgeTick] = Field(default_factory=list, validation_alias=AliasChoices("Quotes", "quotes"), serialization_alias="Quotes")
    model_config = {"populate_by_name": True}


class BridgeCandle(BaseModel):
    time_utc: datetime = Field(validation_alias=AliasChoices("TimeUtc", "timeUtc"), serialization_alias="TimeUtc")
    open: Decimal = Field(validation_alias=AliasChoices("Open", "open"), serialization_alias="Open")
    high: Decimal = Field(validation_alias=AliasChoices("High", "high"), serialization_alias="High")
    low: Decimal = Field(validation_alias=AliasChoices("Low", "low"), serialization_alias="Low")
    close: Decimal = Field(validation_alias=AliasChoices("Close", "close"), serialization_alias="Close")
    tick_volume: int | None = Field(default=None, validation_alias=AliasChoices("TickVolume", "tickVolume"), serialization_alias="TickVolume")
    model_config = {"populate_by_name": True}


class BridgeCandlesRequest(BaseModel):
    provider_key: str = Field(validation_alias=AliasChoices("ProviderKey", "providerKey"), serialization_alias="ProviderKey")
    terminal_instance_id: str = Field(validation_alias=AliasChoices("TerminalInstanceId", "terminalInstanceId"), serialization_alias="TerminalInstanceId")
    provider_symbol: str = Field(validation_alias=AliasChoices("ProviderSymbol", "providerSymbol"), serialization_alias="ProviderSymbol")
    canonical_instrument: str = Field(validation_alias=AliasChoices("CanonicalInstrument", "canonicalInstrument"), serialization_alias="CanonicalInstrument")
    interval: str = Field(validation_alias=AliasChoices("Interval", "interval"), serialization_alias="Interval")
    candles: list[BridgeCandle] = Field(default_factory=list, validation_alias=AliasChoices("Candles", "candles"), serialization_alias="Candles")
    model_config = {"populate_by_name": True}


class InstrumentSelectionRequest(BaseModel):
    provider_key: str = Field(validation_alias=AliasChoices("ProviderKey", "providerKey"), serialization_alias="ProviderKey")
    terminal_instance_id: str = Field(validation_alias=AliasChoices("TerminalInstanceId", "terminalInstanceId"), serialization_alias="TerminalInstanceId")
    provider_symbol: str = Field(validation_alias=AliasChoices("ProviderSymbol", "providerSymbol"), serialization_alias="ProviderSymbol")
    enabled: bool = Field(validation_alias=AliasChoices("Enabled", "enabled"), serialization_alias="Enabled")
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
        self.symbols = SymbolResolver(store)
        self._queue: queue.Queue[BridgeTicksRequest | None] = queue.Queue(maxsize=max_queue_batches)
        self.stats = QueueStats()
        self._services: dict[tuple[str, str], MarketDataService] = {}
        self._last_ingested: dict[tuple[str, str], datetime] = {}
        self._ingest_lock = RLock()
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
        rows = []
        for item in request.instruments:
            if not item.provider_symbol.strip():
                continue
            row = item.model_dump(by_alias=False)
            row["canonical_instrument"] = self.symbols.resolve(
                request.provider_key, item.provider_symbol, item.canonical_instrument
            ).canonical_instrument
            rows.append(row)
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
        """Persist provider-scoped quotes and use them as the live candle source.

        MT5 bridge installations may submit quote snapshots without a separate tick batch.
        In that mode the quote is still an authoritative market observation and must reach
        the server candle builder. Duplicate or older observations are ignored per
        provider/instrument so quote and tick endpoints can safely coexist.
        """
        self.validate_identity(request.provider_key, request.terminal_instance_id)
        accepted = 0
        now = datetime.now(UTC)
        for item in request.quotes:
            if item.bid <= 0 or item.ask < item.bid:
                continue
            instrument = self.symbols.resolve(
                request.provider_key, item.provider_symbol, item.canonical_instrument
            ).canonical_instrument
            value = item.model_dump(by_alias=False)
            value["canonical_instrument"] = instrument
            self.store.upsert_bridge_quote(request.provider_key, request.terminal_instance_id, value, now)
            tick = Tick(
                request.provider_key,
                instrument,
                item.time_utc,
                item.bid,
                item.ask,
                item.volume,
            )
            if self._ingest_observation(request.terminal_instance_id, tick):
                accepted += 1
        return accepted

    def _ingest_observation(self, terminal_instance_id: str, tick: Tick) -> bool:
        key = (tick.provider, tick.instrument)
        service_key = (tick.provider, terminal_instance_id)
        with self._ingest_lock:
            previous = self._last_ingested.get(key)
            if previous is not None and tick.timestamp <= previous:
                return False
            service = self._services.setdefault(service_key, MarketDataService(self.store))
            service.run([tick])
            self._last_ingested[key] = tick.timestamp
            return True

    def candles(self, request: BridgeCandlesRequest) -> int:
        self.validate_identity(request.provider_key, request.terminal_instance_id)
        interval = request.interval.lower()
        if interval not in self.ALLOWED_INTERVALS: raise ValueError(f"Unsupported MT5 interval '{request.interval}'")
        values=[]
        for item in request.candles:
            try:
                instrument = self.symbols.resolve(
                    request.provider_key, request.provider_symbol, request.canonical_instrument
                ).canonical_instrument
                values.append(Candle(request.provider_key, instrument, interval, item.time_utc, item.open, item.high, item.low, item.close, item.tick_volume or 0, Decimal(item.tick_volume or 0), True))
            except ValueError: continue
        return self.store.upsert_candles(values)

    def _consume(self) -> None:
        while True:
            request = self._queue.get()
            if request is None:
                break
            try:
                ticks = []
                for item in request.ticks:
                    try:
                        instrument = self.symbols.resolve(
                            request.provider_key, item.provider_symbol, item.canonical_instrument
                        ).canonical_instrument
                        # Candle boundaries must use the MT5 source timestamp, never HTTP
                        # receipt time or local server processing time.
                        ticks.append(Tick(
                            request.provider_key, instrument, item.time_utc,
                            item.bid, item.ask, item.volume,
                        ))
                    except ValueError:
                        self.stats.rejected_ticks += 1
                ticks.sort(key=lambda x: x.timestamp)
                processed = 0
                for tick in ticks:
                    if self._ingest_observation(request.terminal_instance_id, tick):
                        processed += 1
                self.stats.processed_ticks += processed
                self.stats.last_batch_utc = datetime.now(UTC).isoformat()
                self.stats.last_error = None
            except Exception as exc:
                self.stats.last_error = str(exc)
            finally:
                self._queue.task_done()
                self.stats.queue_depth = self._queue.qsize()

    def view(self) -> dict[str, object]:
        self.stats.queue_depth=self._queue.qsize()
        return {k:getattr(self.stats,k) for k in self.stats.__slots__}
