from __future__ import annotations

import logging
from collections.abc import Iterable

from .aggregation import CANONICAL_DERIVED_TIMEFRAMES, CandleAggregator
from .candle_builder import CandleBuilder
from .domain import Tick
from .storage import MarketDataStore


class MarketDataService:
    def __init__(self, store: MarketDataStore, persist_ticks: bool = True) -> None:
        self.store = store
        self.persist_ticks = persist_ticks
        self.builder = CandleBuilder(store)
        self.aggregator = CandleAggregator(store)
        self.log = logging.getLogger(__name__)

    def run(self, ticks: Iterable[Tick], continuity: str = "CONNECTED") -> None:
        for tick in ticks:
            if self.persist_ticks:
                self.store.insert_ticks([tick])
            self.builder.ingest(tick, continuity=continuity)
            for timeframe in CANONICAL_DERIVED_TIMEFRAMES:
                self.aggregator.refresh_bucket(tick.instrument, timeframe, tick.provider, tick.timestamp)
            self.log.debug(
                "tick provider=%s instrument=%s timestamp=%s bid=%s ask=%s continuity=%s",
                tick.provider, tick.instrument, tick.timestamp.isoformat(), tick.bid, tick.ask, continuity,
            )
