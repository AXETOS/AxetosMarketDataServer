from __future__ import annotations

import base64

from fastapi.testclient import TestClient

from axetos_market_data.security import Role, SecuritySettings, required_role
from axetos_market_data.web import create_app


def basic(token: str) -> dict[str, str]:
    value = base64.b64encode(f"user:{token}".encode()).decode()
    return {"Authorization": f"Basic {value}"}


def test_role_policy():
    assert required_role('/api/providers', 'GET') == Role.VIEWER
    assert required_role('/api/providers/x/start', 'POST') == Role.OPERATOR
    assert required_role('/api/providers/x', 'PUT') == Role.ADMINISTRATOR
    assert required_role('/api/providers/x', 'DELETE') == Role.ADMINISTRATOR


def test_authentication_and_bridge_tokens(tmp_path, monkeypatch):
    monkeypatch.setenv('AXETOS_AUTH_ENABLED', 'true')
    monkeypatch.setenv('AXETOS_VIEWER_TOKEN', 'view-secret')
    monkeypatch.setenv('AXETOS_OPERATOR_TOKEN', 'operate-secret')
    monkeypatch.setenv('AXETOS_ADMIN_TOKEN', 'admin-secret')
    monkeypatch.setenv('AXETOS_BRIDGE_TOKEN', 'bridge-secret')
    app = create_app(tmp_path/'data.sqlite', tmp_path/'providers.json')
    with TestClient(app) as client:
        assert client.get('/api/health').status_code == 200
        assert client.get('/api/providers').status_code == 401
        assert client.get('/api/providers', headers=basic('view-secret')).status_code == 200
        assert client.post('/api/database/retention/preview', json={}).status_code == 401
        assert client.post('/api/database/retention/preview', json={}, headers=basic('view-secret')).status_code == 403
        assert client.post('/api/database/retention/preview', json={}, headers=basic('operate-secret')).status_code == 200
        heartbeat = {'provider_key':'ICMarkets.MT5','terminal_instance_id':'terminal-1','time_utc':'2026-07-24T19:30:00Z'}
        assert client.post('/api/market-data/ingest/mt5/heartbeat', json=heartbeat).status_code == 401
        assert client.post('/api/market-data/ingest/mt5/heartbeat', json=heartbeat, headers={'X-API-Key':'bridge-secret'}).status_code == 200


def test_security_requires_admin_and_bridge_tokens(monkeypatch):
    monkeypatch.setenv('AXETOS_AUTH_ENABLED', 'true')
    monkeypatch.delenv('AXETOS_ADMIN_TOKEN', raising=False)
    monkeypatch.delenv('AXETOS_BRIDGE_TOKEN', raising=False)
    settings = SecuritySettings.from_environment()
    try:
        settings.validate()
    except RuntimeError as exc:
        assert 'AXETOS_ADMIN_TOKEN' in str(exc)
    else:
        raise AssertionError('Expected validation failure')
