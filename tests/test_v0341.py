from pathlib import Path

from axetos_market_data import __version__
from axetos_market_data.database import PostgresConnectionAdapter


class FakeCursor:
    def __init__(self, rowcount: int):
        self.rowcount = rowcount

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class FakeNativeCursor(FakeCursor):
    def __init__(self):
        super().__init__(0)

    def executemany(self, query, params):
        self.rowcount = len(params)


class FakeConnection:
    def execute(self, query, params=()):
        return FakeCursor(1)

    def cursor(self):
        return FakeNativeCursor()


def test_postgres_adapter_tracks_total_changes():
    adapter = PostgresConnectionAdapter(FakeConnection())
    assert adapter.total_changes == 0
    adapter.execute("DELETE FROM sample WHERE id=?", (1,))
    assert adapter.total_changes == 1
    adapter.executemany("INSERT INTO sample(id) VALUES (?)", [(1,), (2,)])
    assert adapter.total_changes == 3


def test_v0341_release_metadata_and_numpy_dev_dependency():
    root = Path(__file__).resolve().parents[1]
    pyproject = (root / "pyproject.toml").read_text()
    readme = (root / "README.md").read_text()
    assert __version__ == "0.60.11"
    assert 'version = "0.60.11"' in pyproject
    assert '"numpy>=1.26"' in pyproject
    assert "## Version 0.60.11" in readme
