from types import SimpleNamespace

from axetos_market_data import __version__
from axetos_market_data.providers.mt5 import MetaTrader5TickProvider


class FakeMT5:
    def __init__(self, account=None, connected=True):
        self._account = account
        self.connected = connected
        self.login_calls = []

    def initialize(self, **kwargs):
        return True

    def terminal_info(self):
        return SimpleNamespace(connected=self.connected, name="MetaTrader 5")

    def account_info(self):
        return self._account

    def login(self, **kwargs):
        self.login_calls.append(kwargs)
        self._account = SimpleNamespace(
            login=kwargs["login"], server=kwargs.get("server", ""), company="Broker", name="Demo"
        )
        return True

    def last_error(self):
        return (0, "ok")


def test_version_and_readme():
    assert __version__ == "0.68.2"
    readme = open("README.md", encoding="utf-8").read()
    assert "## Version 0.68.2" in readme
    assert "conditional account authentication" in readme


def test_existing_matching_account_does_not_login(monkeypatch):
    account = SimpleNamespace(login=12345, server="OANDA-Demo", company="OANDA", name="Patric")
    mt5 = FakeMT5(account)
    provider = MetaTrader5TickProvider([], account_login=12345, account_server="OANDA-Demo", password_env="MT5_PASSWORD")
    provider._initialize(mt5)
    assert mt5.login_calls == []
    assert provider.session_status["account_logged_in"] is True
    assert provider.session_status["login_attempted"] is False


def test_missing_or_wrong_account_logs_in_once(monkeypatch):
    monkeypatch.setenv("MT5_PASSWORD", "secret")
    mt5 = FakeMT5(None)
    provider = MetaTrader5TickProvider([], account_login=98765, account_server="Broker-Demo", password_env="MT5_PASSWORD")
    provider._initialize(mt5)
    assert mt5.login_calls == [{"login": 98765, "password": "secret", "server": "Broker-Demo"}]
    assert provider.session_status["account_login"] == 98765
    assert provider.session_status["login_attempted"] is True


def test_login_is_not_attempted_without_configured_account():
    account = SimpleNamespace(login=111, server="Existing", company="Broker", name="User")
    mt5 = FakeMT5(account)
    provider = MetaTrader5TickProvider([])
    provider._initialize(mt5)
    assert mt5.login_calls == []
    assert provider.session_status["account_login"] == 111
