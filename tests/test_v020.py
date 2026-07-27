from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from axetos_market_data.config import ConfigurationStore, ProviderConfig
from axetos_market_data.runtime import ProviderSupervisor
from axetos_market_data.storage import MarketDataStore
from axetos_market_data.symbols import SymbolResolver


def test_policy_keeps_provider_symbol_and_canonical_identity_separate(tmp_path: Path):
    store=MarketDataStore(tmp_path/'data.sqlite'); store.initialize()
    store.upsert_symbol_policy('Oanda.MT5','EURUSD.pro','EUR/USD',True,True,True,None)
    resolution=SymbolResolver(store).resolve('Oanda.MT5','EURUSD.pro')
    assert resolution.provider_symbol == 'EURUSD.pro'
    assert resolution.canonical_instrument == 'EUR/USD'


def test_authority_routes_using_canonical_policy(tmp_path: Path):
    store=MarketDataStore(tmp_path/'data.sqlite'); store.initialize()
    store.upsert_symbol_policy('Oanda.MT5','EURUSD.pro','EUR/USD',True,True,True,None)
    configs=ConfigurationStore(tmp_path/'providers.json')
    configs.upsert(ProviderConfig('Oanda.MT5','Oanda',kind='mt5',symbols=['EURUSD.pro']))
    supervisor=ProviderSupervisor(configs,store)
    supervisor.authority.replace_configs(supervisor._routing_configs(configs.read_all()))
    now=datetime.now(UTC)
    supervisor.authority.record_tick('Oanda.MT5','EUR/USD',now)
    assert supervisor.authority.is_authoritative('Oanda.MT5','EUR/USD',now)

def test_release_version():
    from axetos_market_data import __version__
    assert __version__ == '0.60.8'
