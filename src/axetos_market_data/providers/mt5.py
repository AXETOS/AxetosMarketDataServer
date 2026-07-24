from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal

from ..domain import Tick


class MetaTrader5TickProvider:
    """Optional direct MetaTrader 5 provider.

    The MetaTrader5 package is Windows-only and must be installed with the
    project's ``mt5`` optional dependency.
    """

    name = "mt5"

    def __init__(self, symbols: list[str], terminal_path: str | None = None) -> None:
        self.symbols = symbols
        self.terminal_path = terminal_path

    @staticmethod
    def _canonical_symbol(symbol: str) -> str:
        upper = symbol.upper()
        for suffix in (".PRO", ".RAW", ".ECN", "_PRO", "-PRO"):
            if upper.endswith(suffix):
                upper = upper[: -len(suffix)]
                break
        if len(upper) == 6 and upper.isalpha():
            return f"{upper[:3]}/{upper[3:]}"
        return upper

    def stream(self) -> Iterator[Tick]:
        try:
            import MetaTrader5 as mt5
        except ImportError as exc:
            raise RuntimeError("Install with: pip install -e '.[mt5]'") from exc

        if not mt5.initialize(path=self.terminal_path) if self.terminal_path else not mt5.initialize():
            raise RuntimeError(f"MetaTrader5 initialization failed: {mt5.last_error()}")
        try:
            for symbol in self.symbols:
                if not mt5.symbol_select(symbol, True):
                    raise RuntimeError(f"Could not select MT5 symbol {symbol}: {mt5.last_error()}")

            last_msc: dict[str, int] = {}
            while True:
                emitted = False
                for symbol in self.symbols:
                    quote = mt5.symbol_info_tick(symbol)
                    if quote is None or quote.bid <= 0 or quote.ask <= 0:
                        continue
                    time_msc = int(getattr(quote, "time_msc", int(quote.time) * 1000))
                    if time_msc <= last_msc.get(symbol, 0):
                        continue
                    last_msc[symbol] = time_msc
                    emitted = True
                    yield Tick(
                        provider=self.name,
                        instrument=self._canonical_symbol(symbol),
                        timestamp=datetime.fromtimestamp(time_msc / 1000, tz=timezone.utc),
                        bid=Decimal(str(quote.bid)),
                        ask=Decimal(str(quote.ask)),
                        volume=Decimal(str(quote.volume_real)) if quote.volume_real else None,
                    )
                if not emitted:
                    time.sleep(0.05)
        finally:
            mt5.shutdown()
