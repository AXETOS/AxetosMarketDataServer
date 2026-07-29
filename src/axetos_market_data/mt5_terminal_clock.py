from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import RLock
from uuid import uuid4


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


@dataclass(slots=True)
class _PendingClockRequest:
    command: str
    clock_request_id: str
    created_utc: datetime
    terminal_offset: timedelta | None = None


class Mt5TerminalClockCoordinator:
    """Per-command MT5 clock handshake and timestamp correlation.

    Every CopyRates command is preceded by an explicit TIME command.  The MT5
    bridge reports TimeTradeServer(), the server calculates the current terminal
    clock offset, translates the requested window, and records the same offset
    against the CopyRates request id so returned bar timestamps can be translated
    back to the server clock before persistence.
    """

    def __init__(self, *, timeout_seconds: float = 10.0) -> None:
        self._timeout = timedelta(seconds=timeout_seconds)
        self._pending: dict[tuple[str, str], _PendingClockRequest] = {}
        self._request_offsets: dict[str, timedelta] = {}
        self._lock = RLock()

    @staticmethod
    def _command_request_id(command: str) -> str | None:
        fields = command.split("|")
        return fields[5] if len(fields) == 6 else None

    def prepare(self, provider: str, terminal: str, command: str) -> str:
        key = (provider, terminal)
        now = datetime.now(UTC)
        with self._lock:
            pending = self._pending.get(key)
            if pending is not None and now - pending.created_utc > self._timeout:
                # Retain the CopyRates command already reserved by the scheduler,
                # but issue a fresh TIME request id. Dropping the command here would
                # strand the scheduler's in-flight request permanently.
                pending.clock_request_id = f"clock-{uuid4().hex}"
                pending.created_utc = now
                pending.terminal_offset = None

            if pending is None:
                if not command:
                    return ""
                pending = _PendingClockRequest(
                    command=command,
                    clock_request_id=f"clock-{uuid4().hex}",
                    created_utc=now,
                )
                self._pending[key] = pending

            if pending.terminal_offset is None:
                return f"TIME|-|-|-|-|{pending.clock_request_id}"

            fields = pending.command.split("|")
            if len(fields) != 6:
                del self._pending[key]
                return ""
            fields[3] = (_parse(fields[3]) + pending.terminal_offset).isoformat()
            fields[4] = (_parse(fields[4]) + pending.terminal_offset).isoformat()
            request_id = fields[5]
            self._request_offsets[request_id] = pending.terminal_offset
            del self._pending[key]
            return "|".join(fields)

    def record_terminal_time(
        self,
        provider: str,
        terminal: str,
        clock_request_id: str,
        terminal_time: datetime,
        *,
        received_utc: datetime | None = None,
    ) -> timedelta:
        key = (provider, terminal)
        received = (received_utc or datetime.now(UTC)).astimezone(UTC)
        terminal_now = terminal_time.astimezone(UTC)
        with self._lock:
            pending = self._pending.get(key)
            if pending is None or pending.clock_request_id != clock_request_id:
                raise ValueError("Unknown or expired MT5 terminal-time request")
            seconds = (terminal_now - received).total_seconds()
            if abs(seconds) > 14 * 3600:
                raise ValueError("Implausible MT5 terminal/server clock difference")
            # Broker clocks are minute-aligned. Rounding removes HTTP latency.
            pending.terminal_offset = timedelta(minutes=round(seconds / 60.0))
            return pending.terminal_offset

    def normalize_returned_timestamp(self, request_id: str | None, value: datetime) -> datetime:
        timestamp = value.astimezone(UTC)
        if not request_id:
            return timestamp
        with self._lock:
            offset = self._request_offsets.get(request_id, timedelta(0))
        return timestamp - offset

    def complete(self, request_id: str) -> None:
        with self._lock:
            self._request_offsets.pop(request_id, None)
