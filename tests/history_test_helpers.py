from datetime import datetime


def complete_history_discovery(manager, provider: str) -> str:
    """Answer the manager's M1/H1/D1 availability requests and return first BACKFILL."""
    for expected in ("1m", "1h", "1d"):
        request = manager.next_request(provider)
        parts = request.split("|")
        assert parts[:3] == ["DISCOVER", parts[1], expected]
        manager.availability_result(
            provider,
            parts[5],
            earliest=datetime.fromisoformat(parts[3]),
            latest=datetime.fromisoformat(parts[4]),
            count=1,
        )
    return manager.next_request(provider)
