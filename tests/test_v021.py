from axetos_market_data.config import ProviderConfig
from axetos_market_data.web import create_app


def test_mt5_provider_has_no_implicit_symbol():
    config = ProviderConfig('Oanda.MT5', 'Oanda', kind='mt5', symbols=[])
    assert config.normalized_symbols() == []


def test_non_mt5_provider_keeps_default_symbol():
    config = ProviderConfig('Mock', 'Mock', kind='mock', symbols=[])
    assert config.normalized_symbols() == ['EUR/USD']


def test_mt5_form_uses_managed_symbols(tmp_path):
    app = create_app(tmp_path / 'data.sqlite', tmp_path / 'providers.json')
    response = next(route.endpoint() for route in app.routes if getattr(route, 'path', None) == '/')
    body = response if isinstance(response, str) else response.body.decode()
    assert "symbolsField.style.display=isMt5?'none':''" in body
    assert "symbols:kind==='mt5'?editingSymbols" in body
    assert 'MT5 symbols are selected through Manage symbols' in body


def test_release_version():
    from axetos_market_data import __version__
    assert __version__ == '0.60.6'
