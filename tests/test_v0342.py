from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.database import PostgresConnectionAdapter


class RecordingCursor:
    def __init__(self) -> None:
        self.rowcount = 0
        self.calls = []

    def executemany(self, query, params) -> None:
        self.calls.append((query, list(params)))
        self.rowcount = len(params)

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class CursorOnlyConnection:
    def __init__(self) -> None:
        self.native_cursor = RecordingCursor()

    def cursor(self):
        return self.native_cursor

    def execute(self, query, params=()):
        raise AssertionError("executemany must use a cursor, not connection.executemany")


def test_postgres_executemany_uses_cursor_and_tracks_changes() -> None:
    connection = CursorOnlyConnection()
    adapter = PostgresConnectionAdapter(connection)
    cursor = adapter.executemany(
        "INSERT OR IGNORE INTO sample(id) VALUES (?)",
        [(1,), (2,), (3,)],
    )
    assert cursor.rowcount == 3
    assert adapter.total_changes == 3
    translated, rows = connection.native_cursor.calls[0]
    assert "%s" in translated
    assert "ON CONFLICT DO NOTHING" in translated
    assert rows == [(1,), (2,), (3,)]


def test_v0342_release_metadata() -> None:
    root = Path(__file__).resolve().parents[1]
    assert __version__ == "0.46.0"
    assert 'version = "0.46.0"' in (root / "pyproject.toml").read_text()
    assert "## Version 0.46.0" in (root / "README.md").read_text()
