from datetime import UTC, datetime
from fastapi.testclient import TestClient
from axetos_market_data.web import create_app

def test_bridge_contract(tmp_path):
    app=create_app(tmp_path/'m.sqlite',tmp_path/'p.json')
    with TestClient(app) as client:
        base={"ProviderKey":"ICMarkets.MT5","TerminalInstanceId":"terminal-1"}
        assert client.post('/api/market-data/ingest/mt5/heartbeat',json={**base,"BrokerName":"IC","ServerName":"Demo","AccountLogin":1,"TimeUtc":datetime.now(UTC).isoformat()}).status_code==200
        payload={**base,"TimeUtc":datetime.now(UTC).isoformat(),"Instruments":[{"ProviderSymbol":"EURUSD.raw","CanonicalInstrument":"EUR/USD","Digits":5,"Point":"0.00001","IsVisible":True,"IsSelected":True}]}
        assert client.post('/api/market-data/ingest/mt5/instruments',json=payload).json()['accepted']==1
        found=client.get('/api/market-data/mt5/discovered-instruments').json()
        assert found['count']==1
        ticks={**base,"Ticks":[{"ProviderSymbol":"EURUSD.raw","CanonicalInstrument":"EUR/USD","TimeUtc":datetime.now(UTC).isoformat(),"Bid":"1.1","Ask":"1.1002","Volume":1}]}
        response=client.post('/api/market-data/ingest/mt5/ticks',json=ticks)
        assert response.status_code==202 and response.json()['accepted']==1
        status=client.get('/api/market-data/mt5/bridge/status').json()
        assert status['discovered_instruments']==1
