from datetime import UTC, datetime
from types import SimpleNamespace

import numpy as np

from axetos_market_data import __version__
from axetos_market_data.providers.mt5 import MetaTrader5TickProvider


class RecoveringMT5:
    COPY_TICKS_ALL = 0

    def __init__(self, fail_first: bool = True):
        self.fail_first = fail_first
        self.initialize_calls = 0
        self.shutdown_calls = 0
        self.select_calls = []
        self.login_calls = []
        self.copy_calls = 0
        self._account = SimpleNamespace(
            login=12345, server="OANDA-Demo", company="OANDA", name="Patric"
        )

    def initialize(self, **kwargs):
        self.initialize_calls += 1
        return True

    def shutdown(self):
        self.shutdown_calls += 1
        return True

    def terminal_info(self):
        return SimpleNamespace(connected=True, name="MetaTrader 5")

    def account_info(self):
        return self._account

    def login(self, **kwargs):
        self.login_calls.append(kwargs)
        return True

    def symbol_select(self, symbol, selected):
        self.select_calls.append((symbol, selected))
        return True

    def copy_ticks_range(self, symbol, start, end, flags):
        self.copy_calls += 1
        if self.fail_first and self.copy_calls == 1:
            return None
        dtype = np.dtype([
            ("time_msc", "i8"), ("bid", "f8"), ("ask", "f8"), ("volume_real", "f8")
        ])
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        return np.array([(now_ms, 1.1000, 1.1002, 0.0)], dtype=dtype)

    def last_error(self):
        return (-10001, "IPC send failed") if self.copy_calls == 1 else (0, "ok")


def _provider(mt5):
    provider = MetaTrader5TickProvider(
        ["EURUSD.pro"],
        terminal_path=r"C:\\OANDA\\terminal64.exe",
        provider_name="Oanda.MT5",
        account_login=12345,
        account_server="OANDA-Demo",
        password="secret",
    )
    provider._module = lambda: mt5
    return provider


def test_ipc_failure_reconnects_and_resumes_without_relogging():
    mt5 = RecoveringMT5(fail_first=True)
    provider = _provider(mt5)
    stream = provider.stream()
    tick = next(stream)

    assert tick.instrument == "EUR/USD"
    assert mt5.initialize_calls == 2  # initial connection plus IPC recovery
    assert mt5.shutdown_calls == 1  # release broken Python IPC only
    assert mt5.login_calls == []  # correct account was already active
    assert mt5.select_calls.count(("EURUSD.pro", True)) == 2
    assert provider.session_status["reconnecting"] is False
    assert provider.session_status["reconnect_attempts"] == 1
    assert provider.session_status["last_reconnect_utc"] is not None
    stream.close()


def test_healthy_existing_terminal_is_reused_without_restart():
    mt5 = RecoveringMT5(fail_first=False)
    provider = _provider(mt5)
    stream = provider.stream()
    next(stream)

    assert mt5.initialize_calls == 1
    assert mt5.shutdown_calls == 0
    assert mt5.login_calls == []
    stream.close()


def test_v0334_release_version():
    assert __version__ == "0.43.0"
