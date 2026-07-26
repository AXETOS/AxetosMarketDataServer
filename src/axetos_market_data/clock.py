from __future__ import annotations

from datetime import datetime, tzinfo


def server_timezone() -> tzinfo:
    """Return the operating system's configured local timezone."""
    return datetime.now().astimezone().tzinfo


def server_now() -> datetime:
    """Current server-local, timezone-aware wall-clock time."""
    return datetime.now().astimezone()


def ensure_server_local(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(server_timezone())
