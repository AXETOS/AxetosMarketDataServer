from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data import __version__
from axetos_market_data.backups import BackupError, BackupService
from axetos_market_data.web import create_app


def test_backup_create_verify_and_restore(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE sample(value TEXT NOT NULL)")
        connection.execute("INSERT INTO sample(value) VALUES('preserved')")
    config = tmp_path / "providers.json"
    config.write_text(json.dumps({"providers": [{"provider_key": "Oanda.MT5"}]}))
    service = BackupService(database, config, tmp_path / "backups")

    created = service.create()
    archive = Path(created["path"])
    assert created["verified"] is True
    assert service.verify(archive)["valid"] is True

    restored_database = tmp_path / "restored.sqlite"
    restored_config = tmp_path / "restored-providers.json"
    result = service.restore(archive, restored_database, restored_config)
    assert result["verified"] is True
    with sqlite3.connect(restored_database) as connection:
        assert connection.execute("SELECT value FROM sample").fetchone()[0] == "preserved"
    assert "Oanda.MT5" in restored_config.read_text()


def test_backup_refuses_overwrite_and_postgresql(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE sample(value TEXT)")
    service = BackupService(database, backup_directory=tmp_path / "backups")
    archive = Path(service.create()["path"])
    try:
        service.restore(archive)
    except BackupError as exc:
        assert "use --overwrite" in str(exc)
    else:
        raise AssertionError("restore should refuse overwriting by default")
    try:
        BackupService("postgresql://example/db").create()
    except BackupError as exc:
        assert "pg_dump" in str(exc)
    else:
        raise AssertionError("PostgreSQL built-in backup should be rejected")


def test_backup_api(tmp_path: Path) -> None:
    database = tmp_path / "market.sqlite"
    app = create_app(database, tmp_path / "providers.json")
    with TestClient(app) as client:
        response = client.post("/api/backups")
        assert response.status_code == 200
        assert response.json()["verified"] is True
        listing = client.get("/api/backups").json()
        assert listing["supported"] is True
        assert listing["count"] == 1


def test_release_metadata_and_readme() -> None:
    assert __version__ == "0.26.0"
    readme = Path("README.md").read_text(encoding="utf-8")
    assert "Version 0.26.0" in readme
    assert "axetos-market-data --database data/market_data.sqlite backup" in readme
