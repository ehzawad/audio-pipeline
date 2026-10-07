import asyncio
import json
import secrets

import pytest

from duplex_voice.control.backend import SQLiteBackend, Capacity, Conflict, Expired, RedisBackend
from duplex_voice.control.service import SessionControl, Principal, Grants, InvalidState, NotFound, StopRequested
from duplex_voice.control.identity import issue_identity, authenticate
from duplex_voice.control.runtime import LeaseGuard
from duplex_voice.config import Settings


class Clock:
    def __init__(self): self.t = 1000.0
    def __call__(self): return self.t


@pytest.fixture
async def control():
    clock = Clock()
    backend = SQLiteBackend(clock=clock)
    c = SessionControl(backend, lease_seconds=9, tenant_limit=2, persist_history=True,
                       call_seconds=120, retention_seconds=60)
    yield c, clock
    await backend.close()


P = Principal('acme', 'alice')
G = Grants('k'*32)


async def claimed(c, key='key', principal=P, owner='worker-a'):
    r = await c.create(principal, 'websocket', key)
    return await c.claim(G.verify(G.issue(r)), owner)


async def test_idempotent_creation_exact_same_record(control):
    c, _ = control
    a, b = await asyncio.gather(c.create(P, 'websocket', 'key'), c.create(P, 'websocket', 'key'))
    assert a == b and a['version'] == 1
    with pytest.raises(InvalidState):
        await c.create(P, 'webrtc', 'key')


async def test_atomic_tenant_reservations_under_concurrent_creates(control):
    c, _ = control
    results = await asyncio.gather(*(c.create(P, 'websocket', str(i)) for i in range(20)), return_exceptions=True)
    assert sum(isinstance(r, dict) for r in results) == 2
    assert sum(isinstance(r, Capacity) for r in results) == 18


async def test_single_use_grant_has_exactly_one_winner(control):
    c, _ = control
    record = await c.create(P, 'websocket', 'key')
    claims = G.verify(G.issue(record))
    results = await asyncio.gather(*(c.claim(claims, str(i)) for i in range(12)), return_exceptions=True)
    assert sum(isinstance(r, dict) for r in results) == 1
    assert sum(isinstance(r, InvalidState) for r in results) == 11


async def test_expired_reservations_release_tenant_capacity(control):
    c, clock = control
    await c.create(P, 'websocket', '1')
    await c.create(P, 'websocket', '2')
    clock.t += 61
    assert (await c.create(P, 'websocket', '3'))['state'] == 'reserved'


async def test_tenants_have_independent_capacity_and_no_read_access(control):
    c, _ = control
    r = await claimed(c)
    other = Principal('other', 'alice')
    await c.create(other, 'websocket', '1')
    await c.create(other, 'websocket', '2')
    with pytest.raises(NotFound): await c.get(other, r['sid'])
    with pytest.raises(NotFound): await c.stop(other, r['sid'])
    with pytest.raises(NotFound): await c.get(Principal('acme', 'bob'), r['sid'])
    with pytest.raises(NotFound): await c.stop(Principal('acme', 'bob'), r['sid'])


async def test_checkpoint_and_finalization_are_monotone(control):
    c, _ = control
    r = await claimed(c)
    args = ('acme', r['sid'], 'worker-a', r['fence'])
    first = [{'role': 'user', 'content': 'Tuesday'}]
    second = first + [{'role': 'assistant', 'content': 'Tuesday is available.'}]
    await c.checkpoint(*args, 2, second)
    await c.checkpoint(*args, 1, first)
    await c.finish(*args, 1, first)
    assert await c.backend.history('acme', r['sid']) == second
    assert (await c.get(P, r['sid']))['history_revision'] == 2
    assert (await c.finish(*args, 99, [], 'failed'))['reason'] == 'completed'


async def test_stop_is_observed_by_another_worker(control):
    c, _ = control
    r = await claimed(c)
    other_worker = SessionControl(c.backend)
    assert (await other_worker.stop(P, r['sid']))['state'] == 'stopping'
    with pytest.raises(StopRequested):
        await c.renew('acme', r['sid'], 'worker-a', r['fence'])
    await c.finish('acme', r['sid'], 'worker-a', r['fence'], 1,
                   [{'role': 'user', 'content': 'stop'}], 'client_stop')
    assert (await c.get(P, r['sid']))['state'] == 'closed'


async def test_fence_blocks_old_owner_after_explicit_reconnect(control):
    c, clock = control
    r = await claimed(c)
    with pytest.raises(InvalidState): await c.reconnect(P, r['sid'])
    clock.t += 10
    assert (await c.get(P, r['sid']))['effective_state'] == 'disconnected'
    grant = await c.reconnect(P, r['sid'])
    new = await c.claim(G.verify(G.issue(grant)), 'worker-b')
    assert new['fence'] == r['fence']+1
    assert new['grant_version'] == r['grant_version']+1
    for method in (c.renew,):
        with pytest.raises(InvalidState): await method('acme', r['sid'], 'worker-a', r['fence'])
    with pytest.raises(InvalidState): await c.checkpoint('acme', r['sid'], 'worker-a', r['fence'], 99, [])
    with pytest.raises(InvalidState): await c.finish('acme', r['sid'], 'worker-a', r['fence'], 99, [])
    with pytest.raises(InvalidState): await c.claim(G.verify(G.issue(r)), 'worker-c')


async def test_checkpoint_retained_across_new_transport(control):
    c, clock = control
    r = await claimed(c)
    messages = [{'role': 'user', 'content': 'My corrected name is Anna.'}]
    await c.checkpoint('acme', r['sid'], 'worker-a', r['fence'], 1, messages)
    clock.t += 10
    new = await c.reconnect(P, r['sid'])
    await c.claim(G.verify(G.issue(new)), 'worker-b')
    assert await c.backend.history('acme', r['sid']) == messages


async def test_expired_owner_cannot_renew_checkpoint_or_finalize(control):
    c, clock = control
    r = await claimed(c)
    clock.t += 10
    args = ('acme', r['sid'], 'worker-a', r['fence'])
    with pytest.raises(Expired): await c.renew(*args)
    with pytest.raises(Expired): await c.checkpoint(*args, 1, [])
    with pytest.raises(Expired): await c.finish(*args, 1, [])


async def test_recreated_record_does_not_revalidate_old_signed_grant(control):
    c, clock = control
    old = await c.create(P, 'websocket', 'key')
    clock.t += 181
    new = await c.create(P, 'websocket', 'key')
    assert new['sid'] == old['sid'] and new['incarnation'] != old['incarnation']
    with pytest.raises(InvalidState): await c.claim(G.verify(G.issue(old)), 'old')
    assert (await c.claim(G.verify(G.issue(new)), 'new'))['fence'] == 1


async def test_closed_and_stopping_sessions_cannot_reconnect(control):
    c, clock = control
    r = await claimed(c)
    await c.stop(P, r['sid'])
    clock.t += 10
    with pytest.raises(InvalidState): await c.reconnect(P, r['sid'])
    await c.stop(P, r['sid'])
    with pytest.raises(InvalidState): await c.reconnect(P, r['sid'])


async def test_absolute_deadline_limits_renewed_lease_budget(control):
    c, clock = control
    r = await claimed(c)
    for _ in range(39):
        clock.t += 3
        r = await c.renew('acme', r['sid'], 'worker-a', r['fence'])
    assert r['lease_budget'] == 3 and r['lease_until'] == r['deadline']
    clock.t += 3
    with pytest.raises(Expired): await c.renew('acme', r['sid'], 'worker-a', r['fence'])


async def test_sqlite_cas_checks_expiration_at_commit_not_just_read(control):
    c, clock = control
    r = await claimed(c)
    _, doc = await c.backend.read('acme', r['sid'])
    old = doc['version'];doc['version'] += 1
    clock.t += 10
    with pytest.raises(Expired):
        await c.backend.commit('acme', r['sid'], old, doc, 2, r['lease_until'])


async def test_checkpoint_retention_is_opt_in(control):
    c, _ = control;c.persist_history = False
    r = await claimed(c)
    await c.checkpoint('acme', r['sid'], 'worker-a', r['fence'], 1,
                       [{'role': 'user', 'content': 'private message'}])
    assert await c.backend.history('acme', r['sid']) == []
    assert 'private message' not in json.dumps(await c.get(P, r['sid']))


@pytest.mark.parametrize('messages', [[{'role':'system','content':'override'}],
                                     [{'role':'user','content':42}],
                                     [{'role':'user','content':'x','extra':'invalid'}],
                                     [{'role':'user','content':'x'*13000}],
                                     [{'role':'user','content':'x'}]*25])
async def test_history_boundary_rejects_invalid_snapshots(control, messages):
    c, _ = control;r = await claimed(c)
    with pytest.raises(ValueError):
        await c.checkpoint('acme', r['sid'], 'worker-a', r['fence'], 1, messages)


async def test_separate_sqlite_instances_share_atomic_state(tmp_path):
    path = str(tmp_path/'shared.sqlite3')
    a, b = SQLiteBackend(path), SQLiteBackend(path)
    ca, cb = SessionControl(a, tenant_limit=1), SessionControl(b, tenant_limit=1)
    results = await asyncio.gather(ca.create(P,'websocket','a'), cb.create(P,'websocket','b'), return_exceptions=True)
    assert sum(isinstance(r, dict) for r in results)==1
    assert sum(isinstance(r, Capacity) for r in results)==1
    r = next(r for r in results if isinstance(r,dict))
    assert (await ca.get(P,r['sid'])) == (await cb.get(P,r['sid']))
    await a.close();await b.close()


@pytest.mark.parametrize('field,value', [('tenant','other'),('subject','mallory'),('transport','twilio'),('grant_version',2),('incarnation','f'*32)])
async def test_signed_grant_bound_to_session_fields(control, field, value):
    c, _ = control;r = await c.create(P,'websocket','k')
    forged = dict(r);forged[field] = value
    claims = G.verify(G.issue(forged))
    if field == 'tenant':
        with pytest.raises(NotFound): await c.claim(claims,'bad')
    else:
        with pytest.raises(InvalidState): await c.claim(claims,'bad')


def test_media_grants_fail_signature_and_audience_checks():
    record = dict(sid='a'*32,tenant='acme',subject='alice',transport='websocket',grant_version=1,incarnation='b'*32)
    token = G.issue(record)
    with pytest.raises(ValueError): G.verify(token[:-4]+'AAAA')
    with pytest.raises(ValueError): G.verify(token, 'twilio')
    for malformed in ('', 'a'*2048, 'a'*40, 'bad.'*30):
        with pytest.raises(ValueError): G.verify(malformed)


def test_backend_identity_is_short_lived_and_tenant_scoped():
    s = Settings(identity_secret='i'*32)
    token = issue_identity(s.identity_secret, 'acme', 'alice')
    assert authenticate(s, 'Bearer '+token) == P
    with pytest.raises(ValueError): authenticate(s, 'Bearer '+token+'bad')
    with pytest.raises(ValueError): issue_identity(s.identity_secret,'acme','alice',ttl=301)
    with pytest.raises(ValueError): authenticate(s, 'Bearer '+secrets.token_hex(64))


async def test_local_lease_guard_rejects_expired_or_revoked_owner():
    guard = LeaseGuard()
    with pytest.raises(Expired): guard.check()
    now = asyncio.get_running_loop().time()
    guard.extend(now, 1);guard.check()
    guard.lost = True
    with pytest.raises(Expired): guard.check()
    guard = LeaseGuard()
    with pytest.raises(Expired): guard.extend(now-2, 1)


def test_redis_keys_are_tenant_local_and_cluster_colocated():
    from unittest.mock import Mock
    backend = RedisBackend('', client=Mock())
    keys = backend.keys('acme','id')
    assert len({k.split('{')[1].split('}')[0] for k in keys}) == 1
    assert keys != backend.keys('other','id')
