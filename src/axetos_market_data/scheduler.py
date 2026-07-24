from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta

from .housekeeping import HousekeepingService, RetentionPolicy
from .operational import OperationalEventService
from .storage import MarketDataStore


class MaintenanceScheduler:
    def __init__(self, store: MarketDataStore, poll_seconds: float = 30.0) -> None:
        self.store = store
        self.poll_seconds = max(1.0, poll_seconds)
        self.housekeeping = HousekeepingService(store)
        self.events = OperationalEventService(store)
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="maintenance-scheduler")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def wake(self) -> None:
        self._wake.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            self.run_due()
            self._wake.wait(self.poll_seconds)
            self._wake.clear()

    def run_due(self, now: datetime | None = None) -> list[dict[str, object]]:
        current = now or datetime.now(UTC)
        return [self.execute(item, current) for item in self.store.due_maintenance_schedules(current)]

    def execute_by_name(self, name: str, now: datetime | None = None) -> dict[str, object]:
        schedule = self.store.get_maintenance_schedule(name)
        if not schedule:
            raise KeyError(name)
        return self.execute(schedule, now or datetime.now(UTC))

    def execute(self, schedule: dict[str, object], started: datetime) -> dict[str, object]:
        run_id = self.store.begin_maintenance_schedule_run(schedule, started)
        interval = max(1, int(schedule["interval_minutes"]))
        next_run = started + timedelta(minutes=interval)
        try:
            if schedule["task_type"] != "retention":
                raise ValueError(f"Unsupported scheduled task: {schedule['task_type']}")
            result = self.housekeeping.run(
                RetentionPolicy(int(schedule["tick_days"]), int(schedule["operational_days"])),
                bool(schedule["vacuum"]),
                now=started,
            )
            self.store.complete_maintenance_schedule_run(
                run_id, int(schedule["id"]), "completed", datetime.now(UTC), next_run,
                json.dumps(result, sort_keys=True, default=str),
            )
            self.events.record("info", "maintenance.schedule.completed", "Scheduled maintenance completed",
                               details={"schedule": schedule["name"], "task_type": schedule["task_type"], **result})
            return {"schedule": schedule["name"], "status": "completed", "result": result, "next_run_utc": next_run.isoformat()}
        except Exception as exc:
            self.store.complete_maintenance_schedule_run(
                run_id, int(schedule["id"]), "failed", datetime.now(UTC), next_run, error=str(exc)
            )
            self.events.record("error", "maintenance.schedule.failed", "Scheduled maintenance failed",
                               details={"schedule": schedule["name"], "task_type": schedule["task_type"], "error": str(exc)})
            return {"schedule": schedule["name"], "status": "failed", "error": str(exc), "next_run_utc": next_run.isoformat()}
