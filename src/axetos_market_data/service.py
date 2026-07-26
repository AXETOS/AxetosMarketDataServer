from __future__ import annotations

import logging
from collections.abc import Iterable

from .aggregation import CANONICAL_DERIVED_TIMEFRAMES, CandleAggregator
from .candle_builder import CandleBuilder
from .domain import Tick
from .storage import MarketDataStore


class MarketDataService:
    def __init__(self, store: MarketDataStore, persist_ticks: bool = True, gap_verifier=None) -> None:
        self.store = store
        self.persist_ticks = persist_ticks
        self.builder = CandleBuilder(store, gap_verifier=gap_verifier)
        self.aggregator = CandleAggregator(store)
        self.log = logging.getLogger(__name__)

    def run(self, ticks: Iterable[Tick], continuity: str = "CONNECTED") -> None:
        for tick in ticks:
            if self.persist_ticks:
                self.store.insert_ticks([tick])
            changed = self.builder.ingest(tick, continuity=continuity)
            refresh_times = {item.open_time for item in changed}
            refresh_times.add(tick.timestamp)
            for open_time in refresh_times:
                for timeframe in CANONICAL_DERIVED_TIMEFRAMES:
                    self.aggregator.refresh_bucket(tick.instrument, timeframe, tick.provider, open_time)
            self.log.debug(
                "tick provider=%s instrument=%s timestamp=%s bid=%s ask=%s continuity=%s",
                tick.provider, tick.instrument, tick.timestamp.isoformat(), tick.bid, tick.ask, continuity,
            )
