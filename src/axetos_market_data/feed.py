from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from threading import RLock

from .domain import Tick


@dataclass(frozen=True, slots=True)
class FeedThresholds:
    quiet_seconds: float = 60.0
    stalled_seconds: float = 180.0
    inactive_seconds: float = 600.0

    def __post_init__(self) -> None:
        if not (0 < self.quiet_seconds < self.stalled_seconds < self.inactive_seconds):
            raise ValueError("feed thresholds must satisfy quiet < stalled < inactive")


@dataclass(slots=True)
class FeedObservation:
    provider: str
    instrument: str
    state: str = "INITIALIZING"
    last_observation_utc: datetime | None = None
    last_price_change_utc: datetime | None = None
    last_bid: Decimal | None = None
    last_ask: Decimal | None = None
    last_market_price: Decimal | None = None
    observations: int = 0
    accepted_ticks: int = 0
    ignored_unchanged_updates: int = 0
    recoveries: int = 0
    recovery_from_utc: datetime | None = None
    recovery_to_utc: datetime | None = None
    recovery_result: str | None = None


@dataclass(frozen=True, slots=True)
class FeedDecision:
    accept_tick: bool
    state: str
    previous_state: str
    continuity: str
    recovery_required: bool = False
    recovery_from_utc: datetime | None = None
    recovery_to_utc: datetime | None = None


class FeedStateEngine:
    """Infers feed activity from observed quotes without assuming exchange hours."""

    def __init__(self, thresholds: FeedThresholds | None = None) -> None:
        self.thresholds = thresholds or FeedThresholds()
        self._items: dict[tuple[str, str], FeedObservation] = {}
        self._lock = RLock()

    def seed_inactive(
        self,
        provider: str,
        instrument: str,
        last_price_change_utc: datetime | None = None,
        market_price: Decimal | None = None,
        bid: Decimal | None = None,
        ask: Decimal | None = None,
    ) -> None:
        """Restore a known configured feed as inactive while monitoring continues."""
        with self._lock:
            key = (provider, instrument)
            if key in self._items:
                return
            reference_time = last_price_change_utc or (datetime.now(UTC) - timedelta(seconds=self.thresholds.inactive_seconds))
            self._items[key] = FeedObservation(
                provider=provider,
                instrument=instrument,
                state="INACTIVE",
                last_price_change_utc=reference_time,
                last_bid=bid,
                last_ask=ask,
                last_market_price=market_price,
            )

    def observe(self, tick: Tick) -> FeedDecision:
        key = (tick.provider, tick.instrument)
        with self._lock:
            item = self._items.get(key)
            if item is None:
                item = FeedObservation(provider=tick.provider, instrument=tick.instrument)
                self._items[key] = item

            previous_state = self._state_at(item, tick.timestamp)
            market_price = tick.market_price
            changed = item.last_market_price is None or market_price != item.last_market_price
            item.observations += 1
            item.last_observation_utc = tick.timestamp

            if not changed:
                item.ignored_unchanged_updates += 1
                item.state = self._state_at(item, tick.timestamp)
                return FeedDecision(False, item.state, previous_state, "HOLD")

            recovery_required = previous_state in {"STALLED", "INACTIVE"}
            recovery_from = item.last_price_change_utc if recovery_required else None
            item.last_bid = tick.bid
            item.last_ask = tick.ask
            item.last_market_price = market_price
            item.last_price_change_utc = tick.timestamp
            item.accepted_ticks += 1
            if recovery_required:
                item.state = "RECOVERING"
                item.recoveries += 1
                item.recovery_from_utc = recovery_from
                item.recovery_to_utc = tick.timestamp
                item.recovery_result = "investigating"
                return FeedDecision(True, "RECOVERING", previous_state, "PENDING", True, recovery_from, tick.timestamp)

            item.state = "LIVE"
            return FeedDecision(True, "LIVE", previous_state, "CONNECTED" if previous_state != "INITIALIZING" else "DETACHED")

    def complete_recovery(self, provider: str, instrument: str, result: str, connected: bool) -> None:
        with self._lock:
            item = self._items.get((provider, instrument))
            if item is None:
                return
            item.state = "LIVE"
            item.recovery_result = result

    def reports(self, now: datetime | None = None) -> list[dict[str, object]]:
        now = (now or datetime.now(UTC)).astimezone(UTC)
        with self._lock:
            reports: list[dict[str, object]] = []
            for item in self._items.values():
                state = item.state if item.state == "RECOVERING" else self._state_at(item, now)
                unchanged = 0.0
                if item.last_price_change_utc is not None:
                    unchanged = max(0.0, (now - item.last_price_change_utc).total_seconds())
                reports.append({
                    "provider": item.provider,
                    "instrument": item.instrument,
                    "feed_state": state,
                    "monitoring": True,
                    "last_observation_utc": self._iso(item.last_observation_utc),
                    "last_price_change_utc": self._iso(item.last_price_change_utc),
                    "market_price": None if item.last_market_price is None else str(item.last_market_price),
                    "last_bid": None if item.last_bid is None else str(item.last_bid),
                    "last_ask": None if item.last_ask is None else str(item.last_ask),
                    "unchanged_seconds": round(unchanged, 3),
                    "observations": item.observations,
                    "accepted_ticks": item.accepted_ticks,
                    "ignored_unchanged_updates": item.ignored_unchanged_updates,
                    "recoveries": item.recoveries,
                    "recovery_from_utc": self._iso(item.recovery_from_utc),
                    "recovery_to_utc": self._iso(item.recovery_to_utc),
                    "recovery_result": item.recovery_result,
                    "thresholds": asdict(self.thresholds),
                })
            return sorted(reports, key=lambda row: (str(row["provider"]), str(row["instrument"])))

    def _state_at(self, item: FeedObservation, timestamp: datetime) -> str:
        if item.last_price_change_utc is None:
            return "INITIALIZING"
        seconds = max(0.0, (timestamp - item.last_price_change_utc).total_seconds())
        if seconds >= self.thresholds.inactive_seconds:
            return "INACTIVE"
        if seconds >= self.thresholds.stalled_seconds:
            return "STALLED"
        if seconds >= self.thresholds.quiet_seconds:
            return "QUIET"
        return "LIVE"

    @staticmethod
    def _iso(value: datetime | None) -> str | None:
        return None if value is None else value.astimezone(UTC).isoformat()
