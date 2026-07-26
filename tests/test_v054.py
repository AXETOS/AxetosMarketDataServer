from datetime import datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from axetos_market_data.web import create_app


def _post_quote(client, provider: str, terminal: str, bid: str, ask: str, observed: datetime):
    return client.post('/api/market-data/ingest/mt5/quotes', json={
        'providerKey': provider, 'terminalInstanceId': terminal, 'quotes': [{
            'providerSymbol':'BTCUSD', 'canonicalInstrument':'BTC/USD',
            'timeUtc':observed.isoformat(), 'receivedUtc':observed.isoformat(),
            'bid':bid, 'ask':ask
        }]
    })


def test_quote_does_not_hop_between_fresh_providers(tmp_path: Path) -> None:
    app = create_app(tmp_path / 'market.sqlite', tmp_path / 'providers.json')
    now = datetime.now().astimezone()
    with TestClient(app) as client:
        # Both providers are fresh. ICMarkets is configured first and remains canonical
        # even when Oanda posts a fraction later with a very different spread.
        assert _post_quote(client, 'ICMarkets.MT5', 'ic', '64631.44', '64643.44', now).status_code == 200
        assert _post_quote(client, 'Oanda.MT5', 'oa', '64605.00', '64695.00', now + timedelta(milliseconds=100)).status_code == 200
        response = client.get('/api/quote', params={'instrument':'BTC/USD', 'provider':'ICMarkets.MT5'})
        assert response.status_code == 200
        assert response.json()['provider'] == 'ICMarkets.MT5'
        assert response.json()['ask'] == '64643.44'


def test_explicit_quote_provider_is_strict(tmp_path: Path) -> None:
    app = create_app(tmp_path / 'market.sqlite', tmp_path / 'providers.json')
    now = datetime.now().astimezone()
    with TestClient(app) as client:
        assert _post_quote(client, 'Oanda.MT5', 'oa', '64605', '64695', now).status_code == 200
        response = client.get('/api/quote', params={'instrument':'BTC/USD', 'provider':'ICMarkets.MT5'})
        assert response.status_code == 404
