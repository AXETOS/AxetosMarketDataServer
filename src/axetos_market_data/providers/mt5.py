from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from ..domain import Candle, Tick
from ..symbols import SymbolResolver, normalize_instrument


class MetaTrader5TickProvider:
    name = "mt5"

    def __init__(
        self,
        symbols: list[str],
        terminal_path: str | None = None,
        provider_name: str = "mt5",
        batch_window_seconds: int = 5,
        batch_limit: int = 50_000,
        symbol_aliases: dict[str, str] | None = None,
    ) -> None:
        self.symbols = symbols
        self.terminal_path = terminal_path
        self.name = provider_name
        self.batch_window_seconds = max(1, batch_window_seconds)
        self.batch_limit = max(1, batch_limit)
        self.last_batch_sizes: dict[str, int] = {}
        self.symbol_resolver = SymbolResolver(aliases=symbol_aliases)

    @staticmethod
    def _canonical_symbol(symbol: str) -> str:
        return normalize_instrument(symbol)

    def _module(self):
        try:
            import MetaTrader5 as mt5
        except ImportError as exc:
            raise RuntimeError("Install with: pip install -e '.[mt5]'") from exc
        return mt5

    def _initialize(self, mt5) -> None:
        ok = mt5.initialize(path=self.terminal_path) if self.terminal_path else mt5.initialize()
        if not ok:
            raise RuntimeError(f"MetaTrader5 initialization failed: {mt5.last_error()}")

    def test_connection(self) -> dict[str, object]:
        mt5 = self._module()
        self._initialize(mt5)
        try:
            terminal = mt5.terminal_info()
            account = mt5.account_info()
            symbols: list[dict[str, object]] = []
            for symbol in self.symbols:
                selected = bool(mt5.symbol_select(symbol, True))
                info = mt5.symbol_info(symbol) if selected else None
                tick = mt5.symbol_info_tick(symbol) if selected else None
                symbols.append({
                    "symbol": symbol,
                    "selected": selected,
                    "visible": bool(getattr(info, "visible", False)) if info else False,
                    "digits": getattr(info, "digits", None) if info else None,
                    "has_tick": tick is not None,
                })
            return {
                "ok": all(item["selected"] for item in symbols),
                "terminal_connected": bool(getattr(terminal, "connected", False)) if terminal else False,
                "terminal_name": getattr(terminal, "name", None) if terminal else None,
                "account_login": getattr(account, "login", None) if account else None,
                "server": getattr(account, "server", None) if account else None,
                "symbols": symbols,
            }
        finally:
            mt5.shutdown()

    def stream(self) -> Iterator[Tick]:
        """Continuously retrieve MT5 ticks in batches.

        A small overlap is intentional. Database uniqueness protects persistence,
        while the overlap prevents ticks being lost at batch boundaries.
        """
        mt5 = self._module()
        self._initialize(mt5)
        cursors: dict[str, datetime] = {}
        try:
            for symbol in self.symbols:
                if not mt5.symbol_select(symbol, True):
                    raise RuntimeError(f"Could not select MT5 symbol {symbol}: {mt5.last_error()}")
                cursors[symbol] = datetime.now(UTC) - timedelta(seconds=self.batch_window_seconds)

            while True:
                emitted = False
                now = datetime.now(UTC)
                for symbol in self.symbols:
                    start = cursors[symbol] - timedelta(milliseconds=1)
                    rows = mt5.copy_ticks_range(symbol, start, now, mt5.COPY_TICKS_ALL)
                    if rows is None:
                        raise RuntimeError(f"MT5 tick request failed for {symbol}: {mt5.last_error()}")
                    if len(rows) > self.batch_limit:
                        rows = rows[-self.batch_limit :]
                    self.last_batch_sizes[symbol] = len(rows)
                    newest = cursors[symbol]
                    for row in rows:
                        ms = int(row["time_msc"])
                        timestamp = datetime.fromtimestamp(ms / 1000, tz=UTC)
                        if timestamp <= cursors[symbol]:
                            continue
                        bid = Decimal(str(row["bid"]))
                        ask = Decimal(str(row["ask"]))
                        if bid <= 0 or ask <= 0:
                            continue
                        volume_real = row["volume_real"] if "volume_real" in row.dtype.names else 0
                        yield Tick(
                            self.name,
                            self.symbol_resolver.resolve(self.name, symbol).canonical_instrument,
                            timestamp,
                            bid,
                            ask,
                            Decimal(str(volume_real)) if volume_real else None,
                        )
                        emitted = True
                        newest = max(newest, timestamp)
                    cursors[symbol] = newest
                if not emitted:
                    time.sleep(0.05)
        finally:
            mt5.shutdown()

    def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> list[Candle]:
        mt5 = self._module()
        self._initialize(mt5)
        mapping = {
            "1m": mt5.TIMEFRAME_M1,
            "5m": mt5.TIMEFRAME_M5,
            "15m": mt5.TIMEFRAME_M15,
            "30m": mt5.TIMEFRAME_M30,
            "1h": mt5.TIMEFRAME_H1,
            "4h": mt5.TIMEFRAME_H4,
            "1d": mt5.TIMEFRAME_D1,
        }
        try:
            rates = mt5.copy_rates_range(symbol, mapping[timeframe], start, end)
            if rates is None:
                raise RuntimeError(f"MT5 history request failed for {symbol}: {mt5.last_error()}")
            return [
                Candle(
                    self.name,
                    self.symbol_resolver.resolve(self.name, symbol).canonical_instrument,
                    timeframe,
                    datetime.fromtimestamp(int(row["time"]), tz=UTC),
                    Decimal(str(row["open"])),
                    Decimal(str(row["high"])),
                    Decimal(str(row["low"])),
                    Decimal(str(row["close"])),
                    int(row["tick_volume"]),
                    Decimal(str(row["real_volume"])) if row["real_volume"] else None,
                    True,
                )
                for row in rates
            ]
        finally:
            mt5.shutdown()
