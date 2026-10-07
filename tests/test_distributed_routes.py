import asyncio
import struct
import time
from contextlib import ExitStack

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from duplex_voice.main import create_app
from duplex_voice.config import Settings
from duplex_voice.control.identity import issue_identity
from duplex_voice.playback import PlaybackLedger

ORIGIN = 'http://localhost:8000'


def settings(tmp_path, **changes):
    return Settings(state_path=str(tmp_path/'control.sqlite3'), signing_key='s'*32,
                    identity_secret='i'*32, greeting='', **changes)


def auth(s, tenant='acme', subject='alice'):
    return {'Authorization': 'Bearer '+issue_identity(s.identity_secret, tenant, subject)}


def create(client, headers, key='one'):
    return client.post('/v1/sessions', json={'transport':'websocket'},
                       headers={**headers, 'Idempotency-Key':key})


def test_cross_gateway_ticket_and_native_no_origin(tmp_path):
    s = settings(tmp_path)
    with ExitStack() as stack:
        a=stack.enter_context(TestClient(create_app(s)))
        b=stack.enter_context(TestClient(create_app(s)))
        issued=create(a,auth(s)).json()
        with b.websocket_connect('/ws',subprotocols=['duplex',issued['ticket']]) as ws:
            ws.send_json({'type':'hello','sample_rate':16000})
            ws.receive_json();assert ws.receive_json()['type']=='ready'
            status=a.get('/v1/sessions/'+issued['session_id'],headers=auth(s)).json()
            assert status['state']=='active'
            ws.send_json({'type':'hangup'})
            assert ws.receive()['type']=='websocket.close'
        assert a.get('/v1/sessions/'+issued['session_id'],headers=auth(s)).json()['state']=='closed'
        with pytest.raises(WebSocketDisconnect):
            with a.websocket_connect('/ws',subprotocols=['duplex',issued['ticket']]): pass


def test_cross_gateway_tenant_quota_includes_unconsumed_grants(tmp_path):
    s=settings(tmp_path,tenant_session_limit=1)
    with TestClient(create_app(s)) as a, TestClient(create_app(s)) as b:
        assert create(a,auth(s),'one').status_code==201
        assert create(b,auth(s),'two').status_code==503
        assert create(b,auth(s,tenant='other'),'two').status_code==201


def test_session_read_stop_and_reconnect_are_owner_scoped(tmp_path):
    s=settings(tmp_path)
    with TestClient(create_app(s)) as c:
        issued=create(c,auth(s)).json();sid=issued['session_id']
        for h in (auth(s,'other','alice'),auth(s,'acme','bob')):
            assert c.get('/v1/sessions/'+sid,headers=h).status_code==404
            assert c.delete('/v1/sessions/'+sid,headers=h).status_code==404
            assert c.post('/v1/sessions/'+sid+'/reconnect',headers=h).status_code==404
        assert c.delete('/v1/sessions/'+sid,headers=auth(s)).status_code==202
        assert c.get('/v1/sessions/'+sid,headers=auth(s)).json()['state']=='closed'


def test_identity_tokens_cannot_admin_drain_or_read_operator_metrics(tmp_path):
    s=settings(tmp_path,app_token='operator-secret')
    with TestClient(create_app(s)) as c:
        assert c.post('/admin/drain',headers=auth(s)).status_code==401
        assert c.get('/metrics',headers=auth(s)).status_code==401
        assert c.post('/admin/drain',headers={'Authorization':'Bearer operator-secret'}).json()['draining']
        assert c.get('/healthz').status_code==200 and c.get('/readyz').status_code==503
        assert create(c,auth(s)).status_code==503


def test_browser_origin_still_checked_with_valid_identity(tmp_path):
    s=settings(tmp_path)
    with TestClient(create_app(s)) as c:
        assert create(c,{**auth(s),'Origin':'https://evil.example'}).status_code==403
        issued=create(c,auth(s)).json()
        with pytest.raises(WebSocketDisconnect):
            with c.websocket_connect('/ws',subprotocols=['duplex',issued['ticket']],headers={'Origin':'https://evil.example'}): pass


def test_http_body_byte_limit_covers_missing_content_length(tmp_path):
    s=settings(tmp_path)
    with TestClient(create_app(s)) as c:
        r=c.post('/v1/sessions',headers={**auth(s),'Idempotency-Key':'large','Content-Type':'application/json'},
                 content=(b'x'*1000 for _ in range(101)))
        assert r.status_code==413


async def test_playback_credit_is_bounded_and_epoch_cancellable():
    ledger=PlaybackLedger();epoch=ledger.begin()
    for _ in range(5):ledger.issue(epoch)
    task=asyncio.create_task(ledger.credit(epoch,5,1))
    await asyncio.sleep(.01);assert not task.done()
    ledger.acknowledge(epoch,2);await task
    for _ in range(2):ledger.issue(epoch)
    task=asyncio.create_task(ledger.credit(epoch,5,1))
    await asyncio.sleep(.01);ledger.begin()
    with pytest.raises(asyncio.CancelledError):await task


async def test_missing_receipts_fail_instead_of_growing_playback_queue():
    ledger=PlaybackLedger();epoch=ledger.begin();ledger.issue(epoch)
    with pytest.raises(TimeoutError):await ledger.credit(epoch,1,.02)
    assert ledger.sent-ledger.acked==1


def test_remote_stop_reaches_media_owner(tmp_path):
    s=settings(tmp_path,lease_seconds=3,control_timeout=.5)
    with TestClient(create_app(s)) as a, TestClient(create_app(s)) as b:
        issued=create(a,auth(s)).json()
        with b.websocket_connect('/ws',subprotocols=['duplex',issued['ticket']]) as ws:
            ws.send_json({'type':'hello','sample_rate':16000});ws.receive_json();ws.receive_json()
            assert a.delete('/v1/sessions/'+issued['session_id'],headers=auth(s)).status_code==202
            assert ws.receive()['type']=='websocket.close'
        status=a.get('/v1/sessions/'+issued['session_id'],headers=auth(s)).json()
        assert status['state']=='closed' and status['reason']=='client_stop'
