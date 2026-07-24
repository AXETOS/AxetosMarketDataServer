from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol


class CursorLike(Protocol):
    rowcount: int
    lastrowid: int | None
    def fetchone(self) -> Any: ...
    def fetchall(self) -> list[Any]: ...


class ConnectionLike(Protocol):
    def execute(self, query: str, params: Sequence[Any] = ()) -> CursorLike: ...
    def executemany(self, query: str, params: list[Sequence[Any]]) -> CursorLike: ...
    def executescript(self, script: str) -> None: ...


class HybridRow(Mapping[str, Any]):
    def __init__(self, values: Mapping[str, Any]) -> None:
        self._values = dict(values)
        self._ordered = list(self._values.values())

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return self._ordered[key]
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


class PostgresCursorAdapter:
    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor
        self.rowcount = cursor.rowcount
        self.lastrowid: int | None = None

    @staticmethod
    def _wrap(row: Any) -> Any:
        return HybridRow(row) if isinstance(row, Mapping) else row

    def fetchone(self) -> Any:
        row = self._cursor.fetchone()
        return None if row is None else self._wrap(row)

    def fetchall(self) -> list[Any]:
        return [self._wrap(row) for row in self._cursor.fetchall()]


class PostgresConnectionAdapter:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    @staticmethod
    def _translate(query: str) -> str:
        was_ignore = "INSERT OR IGNORE" in query.upper()
        query = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", query, flags=re.I)
        if was_ignore and "ON CONFLICT" not in query.upper():
            query = query.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
        return query.replace("?", "%s")

    def execute(self, query: str, params: Sequence[Any] = ()) -> PostgresCursorAdapter:
        return PostgresCursorAdapter(self._connection.execute(self._translate(query), params))

    def executemany(self, query: str, params: list[Sequence[Any]]) -> PostgresCursorAdapter:
        return PostgresCursorAdapter(self._connection.executemany(self._translate(query), params))

    def executescript(self, script: str) -> None:
        for statement in script.split(";"):
            statement = statement.strip()
            if statement:
                self._connection.execute(statement)


class DatabaseBackend:
    def __init__(self, target: str | Path) -> None:
        self.target = str(target)
        self.kind = "postgresql" if self.target.startswith(("postgresql://", "postgres://")) else "sqlite"
        self.path = None if self.kind == "postgresql" else Path(self.target)
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[ConnectionLike]:
        if self.kind == "sqlite":
            assert self.path is not None
            connection = sqlite3.connect(self.path, timeout=30)
            try:
                connection.row_factory = sqlite3.Row
                yield connection
                connection.commit()
            finally:
                connection.close()
            return

        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError(
                "PostgreSQL support requires the optional dependency: pip install -e '.[postgres]'"
            ) from exc
        with psycopg.connect(self.target, row_factory=dict_row) as connection:
            yield PostgresConnectionAdapter(connection)
            connection.commit()
