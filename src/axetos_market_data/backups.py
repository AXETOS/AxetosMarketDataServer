from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import __version__


class BackupError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class BackupService:
    def __init__(
        self,
        database_target: str | Path,
        configuration_path: str | Path = "data/providers.json",
        backup_directory: str | Path | None = None,
    ) -> None:
        self.database_target = str(database_target)
        self.configuration_path = Path(configuration_path)
        if backup_directory is None and self.is_sqlite:
            self.backup_directory = Path(self.database_target).parent / "backups"
        else:
            self.backup_directory = Path(backup_directory or "data/backups")

    @property
    def is_sqlite(self) -> bool:
        return not self.database_target.startswith(("postgresql://", "postgres://"))

    def _require_sqlite(self) -> Path:
        if not self.is_sqlite:
            raise BackupError(
                "Built-in backups currently support SQLite only. Use pg_dump/pg_restore for PostgreSQL."
            )
        return Path(self.database_target)

    def create(self, destination: str | Path | None = None) -> dict[str, Any]:
        database_path = self._require_sqlite()
        if not database_path.exists():
            raise BackupError(f"Database does not exist: {database_path}")
        self.backup_directory.mkdir(parents=True, exist_ok=True)
        created = datetime.now(UTC)
        output = Path(destination) if destination else self.backup_directory / (
            f"axetos-market-data-{created.strftime('%Y%m%dT%H%M%SZ')}.zip"
        )
        output.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(prefix="axetos-backup-") as temp_name:
            temp = Path(temp_name)
            database_copy = temp / "market_data.sqlite"
            with sqlite3.connect(database_path, timeout=30) as source:
                with sqlite3.connect(database_copy) as target:
                    source.backup(target)
            files: list[dict[str, Any]] = [
                {
                    "archive_name": "database/market_data.sqlite",
                    "size_bytes": database_copy.stat().st_size,
                    "sha256": _sha256(database_copy),
                }
            ]
            config_copy: Path | None = None
            if self.configuration_path.exists():
                config_copy = temp / "providers.json"
                shutil.copy2(self.configuration_path, config_copy)
                files.append(
                    {
                        "archive_name": "configuration/providers.json",
                        "size_bytes": config_copy.stat().st_size,
                        "sha256": _sha256(config_copy),
                    }
                )
            manifest = {
                "format": "axetos-market-data-backup",
                "format_version": 1,
                "application_version": __version__,
                "created_utc": created.isoformat(),
                "database_backend": "sqlite",
                "files": files,
            }
            manifest_path = temp / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.write(manifest_path, "manifest.json")
                archive.write(database_copy, "database/market_data.sqlite")
                if config_copy is not None:
                    archive.write(config_copy, "configuration/providers.json")
        verification = self.verify(output)
        return {
            "path": str(output),
            "filename": output.name,
            "size_bytes": output.stat().st_size,
            "created_utc": created.isoformat(),
            "verified": verification["valid"],
            "file_count": len(files),
        }

    def verify(self, archive_path: str | Path) -> dict[str, Any]:
        archive_path = Path(archive_path)
        errors: list[str] = []
        manifest: dict[str, Any] = {}
        if not archive_path.exists():
            return {"valid": False, "errors": [f"Backup does not exist: {archive_path}"]}
        try:
            with tempfile.TemporaryDirectory(prefix="axetos-verify-") as temp_name:
                temp = Path(temp_name)
                with zipfile.ZipFile(archive_path) as archive:
                    names = set(archive.namelist())
                    if "manifest.json" not in names:
                        raise BackupError("Backup manifest is missing")
                    manifest = json.loads(archive.read("manifest.json"))
                    if manifest.get("format") != "axetos-market-data-backup":
                        errors.append("Unsupported backup format")
                    for item in manifest.get("files", []):
                        name = str(item["archive_name"])
                        if name not in names:
                            errors.append(f"Missing archived file: {name}")
                            continue
                        extracted = Path(archive.extract(name, temp))
                        if _sha256(extracted) != item.get("sha256"):
                            errors.append(f"Checksum mismatch: {name}")
                    db_name = "database/market_data.sqlite"
                    if db_name in names:
                        database = Path(archive.extract(db_name, temp))
                        with sqlite3.connect(database) as connection:
                            result = connection.execute("PRAGMA integrity_check").fetchone()
                        if not result or str(result[0]).lower() != "ok":
                            errors.append("SQLite integrity check failed")
        except (OSError, ValueError, json.JSONDecodeError, zipfile.BadZipFile, BackupError) as exc:
            errors.append(str(exc))
        return {"valid": not errors, "errors": errors, "manifest": manifest}

    def list_backups(self) -> list[dict[str, Any]]:
        if not self.backup_directory.exists():
            return []
        items = []
        for path in sorted(self.backup_directory.glob("*.zip"), reverse=True):
            verification = self.verify(path)
            items.append(
                {
                    "filename": path.name,
                    "path": str(path),
                    "size_bytes": path.stat().st_size,
                    "modified_utc": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat(),
                    "valid": verification["valid"],
                }
            )
        return items

    def restore(
        self,
        archive_path: str | Path,
        database_destination: str | Path | None = None,
        configuration_destination: str | Path | None = None,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        self._require_sqlite()
        verification = self.verify(archive_path)
        if not verification["valid"]:
            raise BackupError("Backup verification failed: " + "; ".join(verification["errors"]))
        database_destination = Path(database_destination or self.database_target)
        configuration_destination = Path(configuration_destination or self.configuration_path)
        if database_destination.exists() and not overwrite:
            raise BackupError(f"Destination already exists: {database_destination}; use --overwrite")
        with tempfile.TemporaryDirectory(prefix="axetos-restore-") as temp_name:
            temp = Path(temp_name)
            with zipfile.ZipFile(archive_path) as archive:
                database_source = Path(archive.extract("database/market_data.sqlite", temp))
                database_destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(database_source, database_destination)
                config_restored = False
                if "configuration/providers.json" in archive.namelist():
                    config_source = Path(archive.extract("configuration/providers.json", temp))
                    configuration_destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(config_source, configuration_destination)
                    config_restored = True
        return {
            "database_path": str(database_destination),
            "configuration_path": str(configuration_destination) if config_restored else None,
            "verified": True,
        }
