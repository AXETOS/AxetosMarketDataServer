from __future__ import annotations

import os
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
        store=None,
        account_login: int | None = None,
        account_server: str | None = None,
        password_env: str | None = None,
        password: str | None = None,
    ) -> None:
        self.symbols = symbols
        self.terminal_path = terminal_path
        self.name = provider_name
        self.batch_window_seconds = max(1, batch_window_seconds)
        self.batch_limit = max(1, batch_limit)
        self.last_batch_sizes: dict[str, int] = {}
        self.symbol_resolver = SymbolResolver(store=store, aliases=symbol_aliases)
        self._active_mt5 = None
        self.account_login = account_login
        self.account_server = account_server
        self.password_env = password_env
        self.password = password
        self.session_status: dict[str, object] = {
            "terminal_running": False, "terminal_connected": False, "broker_connected": False,
            "account_logged_in": False, "account_login": None, "account_server": None,
            "account_company": None, "account_name": None, "login_attempted": False,
            "login_required": bool(account_login), "error": None,
        }
        self.selection_status: dict[str, dict[str, object]] = {symbol: {"selected": False, "error": None} for symbol in symbols}

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
        # MetaTrader5.initialize() connects to an existing terminal or starts the configured
        # terminal executable when it is not already running.
        ok = mt5.initialize(path=self.terminal_path) if self.terminal_path else mt5.initialize()
        if not ok:
            self.session_status.update({"error": str(mt5.last_error()), "terminal_running": False})
            raise RuntimeError(f"MetaTrader5 initialization failed: {mt5.last_error()}")
        self._ensure_session(mt5)

    def _ensure_session(self, mt5) -> None:
        terminal = mt5.terminal_info()
        if terminal is None:
            self.session_status.update({"terminal_running": True, "error": str(mt5.last_error())})
            raise RuntimeError(f"MetaTrader5 terminal information unavailable: {mt5.last_error()}")

        connected = bool(getattr(terminal, "connected", False))
        account = mt5.account_info()
        current_login = int(getattr(account, "login", 0) or 0) if account else None
        current_server = str(getattr(account, "server", "") or "") if account else None
        expected_server = (self.account_server or "").strip() or None
        login_matches = self.account_login is None or current_login == int(self.account_login)
        server_matches = expected_server is None or (current_server or "").casefold() == expected_server.casefold()

        self.session_status.update({
            "terminal_running": True,
            "terminal_connected": True,
            "broker_connected": connected,
            "account_logged_in": account is not None,
            "account_login": current_login,
            "account_server": current_server,
            "account_company": getattr(account, "company", None) if account else None,
            "account_name": getattr(account, "name", None) if account else None,
            "login_attempted": False,
            "error": None,
        })

        # Do not send a login request when the desired account is already active.
        if self.account_login is not None and not (login_matches and server_matches):
            password = self.password or (os.getenv(self.password_env or "") if self.password_env else None)
            if not password:
                raise RuntimeError(
                    f"MT5 account {self.account_login} is not active and no MT5 password is available"
                )
            self.session_status["login_attempted"] = True
            kwargs = {"login": int(self.account_login), "password": password}
            if expected_server:
                kwargs["server"] = expected_server
            if not mt5.login(**kwargs):
                self.session_status["error"] = str(mt5.last_error())
                raise RuntimeError(f"MetaTrader5 login failed: {mt5.last_error()}")
            account = mt5.account_info()
            current_login = int(getattr(account, "login", 0) or 0) if account else None
            current_server = str(getattr(account, "server", "") or "") if account else None
            if account is None or current_login != int(self.account_login) or (expected_server and current_server.casefold() != expected_server.casefold()):
                raise RuntimeError("MetaTrader5 login verification failed")
            self.session_status.update({
                "broker_connected": bool(getattr(mt5.terminal_info(), "connected", False)),
                "account_logged_in": True,
                "account_login": current_login,
                "account_server": current_server,
                "account_company": getattr(account, "company", None),
                "account_name": getattr(account, "name", None),
                "error": None,
            })

        if not self.session_status["broker_connected"]:
            raise RuntimeError("MetaTrader5 terminal is running but not connected to the broker")
        if self.account_login is not None and not self.session_status["account_logged_in"]:
            raise RuntimeError("MetaTrader5 account is not logged in")

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
                "authentication": dict(self.session_status),
                "symbols": symbols,
            }
        finally:
            mt5.shutdown()


    def discover_symbols(self, search: str | None = None, limit: int = 5000) -> list[dict[str, object]]:
        """Discover broker symbols from the connected MT5 terminal."""
        mt5 = self._module()
        self._initialize(mt5)
        try:
            rows = mt5.symbols_get()
            if rows is None:
                raise RuntimeError(f"MT5 symbol discovery failed: {mt5.last_error()}")
            needle = (search or "").strip().upper()
            result: list[dict[str, object]] = []
            for row in rows:
                name = str(getattr(row, "name", "") or "")
                description = str(getattr(row, "description", "") or "")
                if not name:
                    continue
                if needle and needle not in name.upper() and needle not in description.upper():
                    continue
                resolution = self.symbol_resolver.resolve(self.name, name)
                result.append({
                    "provider_symbol": name,
                    "description": description,
                    "canonical_instrument": resolution.canonical_instrument,
                    "mapping_source": resolution.source,
                    "visible": bool(getattr(row, "visible", False)),
                    "selected": bool(getattr(row, "select", False)),
                    "digits": getattr(row, "digits", None),
                    "path": getattr(row, "path", None),
                })
                if len(result) >= max(1, limit):
                    break
            return result
        finally:
            mt5.shutdown()

    def stream(self) -> Iterator[Tick]:
        """Continuously retrieve MT5 ticks in batches.

        A small overlap is intentional. Database uniqueness protects persistence,
        while the overlap prevents ticks being lost at batch boundaries.
        """
        mt5 = self._module()
        self._initialize(mt5)
        self._active_mt5 = mt5
        cursors: dict[str, datetime] = {}
        try:
            selected_any = False
            for symbol in self.symbols:
                selected = bool(mt5.symbol_select(symbol, True))
                error = None if selected else str(mt5.last_error())
                self.selection_status[symbol] = {"selected": selected, "error": error}
                if selected:
                    selected_any = True
                    cursors[symbol] = datetime.now(UTC) - timedelta(seconds=self.batch_window_seconds)
            if self.symbols and not selected_any:
                raise RuntimeError("Could not select any configured MT5 symbols")

            while True:
                emitted = False
                now = datetime.now(UTC)
                for symbol in self.symbols:
                    if not self.selection_status.get(symbol, {}).get("selected"):
                        continue
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
            self._active_mt5 = None
            mt5.shutdown()

    def fetch_candles_live(
        self, symbol: str, timeframe: str, start: datetime, end: datetime
    ) -> list[Candle]:
        """Fetch history through the already-connected MT5 session used by stream()."""
        mt5 = self._active_mt5
        if mt5 is None:
            return self.fetch_candles(symbol, timeframe, start, end)
        mapping = {
            "1m": mt5.TIMEFRAME_M1, "5m": mt5.TIMEFRAME_M5,
            "15m": mt5.TIMEFRAME_M15, "30m": mt5.TIMEFRAME_M30,
            "1h": mt5.TIMEFRAME_H1, "4h": mt5.TIMEFRAME_H4, "1d": mt5.TIMEFRAME_D1,
        }
        rates = mt5.copy_rates_range(symbol, mapping[timeframe], start, end)
        if rates is None:
            raise RuntimeError(f"MT5 history request failed for {symbol}: {mt5.last_error()}")
        return [
            Candle(
                self.name, self.symbol_resolver.resolve(self.name, symbol).canonical_instrument, timeframe,
                datetime.fromtimestamp(int(row["time"]), tz=UTC), Decimal(str(row["open"])),
                Decimal(str(row["high"])), Decimal(str(row["low"])), Decimal(str(row["close"])),
                int(row["tick_volume"]), Decimal(str(row["real_volume"])) if row["real_volume"] else None, True,
            ) for row in rates
        ]

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
