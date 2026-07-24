from __future__ import annotations

import random
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal

from ..domain import Tick


class MockTickProvider:
    name = "mock"

    def __init__(self, instrument: str = "EUR/USD", interval_seconds: float = 1.0) -> None:
        self.instrument = instrument
        self.interval_seconds = interval_seconds
        self._mid = Decimal("1.10000")

    def stream(self) -> Iterator[Tick]:
        while True:
            movement = Decimal(str(random.choice([-2, -1, 0, 1, 2]))) / Decimal("100000")
            self._mid += movement
            spread = Decimal("0.00010")
            yield Tick(
                provider=self.name,
                instrument=self.instrument,
                timestamp=datetime.now(timezone.utc),
                bid=self._mid - spread / 2,
                ask=self._mid + spread / 2,
            )
            time.sleep(self.interval_seconds)
