from __future__ import annotations

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.database import DatabaseBackend, HybridRow, PostgresConnectionAdapter
from axetos_market_data.storage import _postgres_schema
from axetos_market_data.web import create_app


class _FakeCursor:
    rowcount = 1
    def fetchone(self):
        return {"id": 7, "name": "EUR/USD"}
    def fetchall(self):
        return [{"id": 7, "name": "EUR/USD"}]


class _FakeConnection:
    def __init__(self):
        self.query = None
        self.params = None
    def execute(self, query, params=()):
        self.query = query
        self.params = params
        return _FakeCursor()
    def executemany(self, query, params):
        self.query = query
        self.params = params
        return _FakeCursor()


def test_version_and_backend_detection(tmp_path):
    assert __version__ == "0.60.2"
    sqlite = DatabaseBackend(tmp_path / "market.sqlite")
    postgres = DatabaseBackend("postgresql://user:secret@localhost/market")
    assert sqlite.kind == "sqlite"
    assert sqlite.path == tmp_path / "market.sqlite"
    assert postgres.kind == "postgresql"
    assert postgres.path is None


def test_postgres_translation_and_hybrid_rows():
    raw = _FakeConnection()
    connection = PostgresConnectionAdapter(raw)
    cursor = connection.execute(
        "INSERT OR IGNORE INTO ticks(provider,instrument) VALUES (?,?)",
        ("Mock", "EUR/USD"),
    )
    assert raw.query.endswith("ON CONFLICT DO NOTHING")
    assert "%s" in raw.query and "?" not in raw.query
    row = cursor.fetchone()
    assert isinstance(row, HybridRow)
    assert row[0] == 7
    assert row["name"] == "EUR/USD"
    assert dict(row)["id"] == 7


def test_postgres_schema_is_portable():
    schema = _postgres_schema()
    assert "PRAGMA" not in schema
    assert "AUTOINCREMENT" not in schema
    assert "BIGSERIAL PRIMARY KEY" in schema
    assert "CREATE TABLE IF NOT EXISTS candles" in schema


def test_storage_status_endpoint_defaults_to_sqlite(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        response = client.get("/api/storage")
        assert response.status_code == 200
        payload = response.json()
        assert payload["backend"] == "sqlite"
        assert payload["sqlite_wal"] is True
        assert payload["retention_vacuum_supported"] is True
