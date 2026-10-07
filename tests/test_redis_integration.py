"""Run against a disposable loopback Redis: TEST_REDIS_URL=redis://127.0.0.1:6379/0.

These tests never FLUSHDB. Their unique cell/tenant keys are removed on exit.
The script-cache test does SCRIPT FLUSH and therefore requires a disposable server.
"""
import asyncio
import os
import secrets
from urllib.parse import urlsplit

import pytest
from duplex_voice.control.backend import RedisBackend, Capacity, Expired
from duplex_voice.control.service import SessionControl, Principal, Grants, InvalidState, StopRequested

URL = os.environ.get('TEST_REDIS_URL', '')
pytestmark = [pytest.mark.redis, pytest.mark.skipif(not URL, reason='TEST_REDIS_URL not set: real Redis contract not exercised')]
P = Principal('integration', 'test-subject')
G = Grants('test-only-signing-key-' * 3)


@pytest.fixture
async def pair():
    if urlsplit(URL).hostname not in {'localhost', '127.0.0.1', '::1'}:
        pytest.fail('Use a disposable LOOPBACK Redis server, not a production endpoint')
    pytest.importorskip('redis')
    cell = 'test-'+secrets.token_hex(8)
    a, b = RedisBackend(URL, cell), RedisBackend(URL, cell)
    assert await a.healthy()
    yield a, b
    prefix=a.keys(P.tenant,'')[0].split('session:')[0]
    keys=[key async for key in a.client.scan_iter(match=prefix+'*')]
    if keys:await a.client.delete(*keys)
    await a.close();await b.close()


async def test_real_redis_shared_reservation_and_one_use_claim(pair):
    a,b=pair;ca=SessionControl(a,tenant_limit=2);cb=SessionControl(b,tenant_limit=2)
    results=await asyncio.gather(*((ca if i%2 else cb).create(P,'websocket',str(i)) for i in range(20)),return_exceptions=True)
    records=[r for r in results if isinstance(r,dict)]
    assert len(records)==2 and sum(isinstance(r,Capacity) for r in results)==18
    claims=G.verify(G.issue(records[0]))
    outcomes=await asyncio.gather(*((ca if i%2 else cb).claim(claims,str(i)) for i in range(20)),return_exceptions=True)
    assert sum(isinstance(r,dict) for r in outcomes)==1
    assert sum(isinstance(r,InvalidState) for r in outcomes)==19


async def test_real_redis_lease_expiry_fencing_and_history(pair):
    a,b=pair;ca=SessionControl(a,lease_seconds=.3,persist_history=True)
    cb=SessionControl(b,lease_seconds=.3,persist_history=True)
    grant=await ca.create(P,'websocket','reconnect')
    first=await ca.claim(G.verify(G.issue(grant)),'old')
    history=[{'role':'user','content':'Use the corrected name Maya.'}]
    await ca.checkpoint(P.tenant,first['sid'],'old',first['fence'],1,history)
    await asyncio.sleep(.4)
    with pytest.raises(Expired):await ca.renew(P.tenant,first['sid'],'old',first['fence'])
    grant=await cb.reconnect(P,first['sid']);second=await cb.claim(G.verify(G.issue(grant)),'new')
    with pytest.raises(InvalidState):await ca.finish(P.tenant,first['sid'],'old',first['fence'],99,[])
    assert second['fence']==2 and await b.history(P.tenant,first['sid'])==history


async def test_real_redis_script_cache_reload_and_cross_worker_stop(pair):
    a,b=pair;ca=SessionControl(a);cb=SessionControl(b)
    grant=await ca.create(P,'websocket','script-cache')
    await a.client.script_flush()
    first=await cb.claim(G.verify(G.issue(grant)),'owner')
    await ca.stop(P,first['sid'])
    with pytest.raises(StopRequested):await cb.renew(P.tenant,first['sid'],'owner',first['fence'])
    await cb.finish(P.tenant,first['sid'],'owner',first['fence'],1,[],'client_stop')
    assert (await ca.get(P,first['sid']))['state']=='closed'


async def test_real_redis_commit_rechecks_deadline(pair):
    a,_=pair;c=SessionControl(a,lease_seconds=.1)
    grant=await c.create(P,'websocket','commit-clock');first=await c.claim(G.verify(G.issue(grant)),'owner')
    _,doc=await a.read(P.tenant,first['sid']);old=doc['version'];doc['version']+=1
    await asyncio.sleep(.2)
    with pytest.raises(Expired):await a.commit(P.tenant,first['sid'],old,doc,100,first['lease_until'])
