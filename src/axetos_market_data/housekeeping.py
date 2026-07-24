from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .storage import MarketDataStore
from .operational import OperationalEventService


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    tick_days: int = 30
    operational_days: int = 90

    def cutoffs(self, now: datetime | None = None) -> tuple[datetime, datetime]:
        current = now or datetime.now(UTC)
        return (
            current - timedelta(days=max(1, self.tick_days)),
            current - timedelta(days=max(1, self.operational_days)),
        )


class HousekeepingService:
    def __init__(self, store: MarketDataStore) -> None:
        self.store = store
        self.events = OperationalEventService(store)

    def preview(self, policy: RetentionPolicy, now: datetime | None = None) -> dict[str, object]:
        ticks_before, operational_before = policy.cutoffs(now)
        counts = self.store.retention_preview(ticks_before, operational_before)
        return {
            "ticks_before_utc": ticks_before.isoformat(),
            "operational_before_utc": operational_before.isoformat(),
            "would_delete": counts,
            "candles_deleted": 0,
        }

    def run(
        self,
        policy: RetentionPolicy,
        vacuum: bool = False,
        now: datetime | None = None,
    ) -> dict[str, object]:
        ticks_before, operational_before = policy.cutoffs(now)
        result = self.store.run_retention(ticks_before, operational_before, vacuum)
        events_deleted = self.store.delete_operational_events_before(operational_before)
        result["operational_events_deleted"] = events_deleted
        self.events.record("info", "retention.completed", "Retention cleanup completed", details={**result, "vacuum": vacuum})
        return result
