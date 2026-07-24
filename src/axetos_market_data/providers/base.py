from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

from ..domain import Tick


class TickProvider(Protocol):
    name: str

    def stream(self) -> Iterator[Tick]: ...
