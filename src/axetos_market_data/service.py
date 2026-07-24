from __future__ import annotations

import logging
from collections.abc import Iterable

from .candle_builder import CandleBuilder
from .domain import Tick
from .storage import MarketDataStore


class MarketDataService:
    def __init__(self, store: MarketDataStore, persist_ticks: bool = True) -> None:
        self.store = store
        self.persist_ticks = persist_ticks
        self.builder = CandleBuilder(store)
        self.log = logging.getLogger(__name__)

    def run(self, ticks: Iterable[Tick]) -> None:
        for tick in ticks:
            if self.persist_ticks:
                self.store.insert_ticks([tick])
            self.builder.ingest(tick)
            self.log.debug(
                "tick provider=%s instrument=%s timestamp=%s bid=%s ask=%s",
                tick.provider,
                tick.instrument,
                tick.timestamp.isoformat(),
                tick.bid,
                tick.ask,
            )
