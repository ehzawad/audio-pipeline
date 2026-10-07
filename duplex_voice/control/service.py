"""Versioned session lifecycle, tenant quotas, and fenced transcript checkpoints.

A session lease protects a live media owner. A response epoch protects one reply
within that owner. They are intentionally different identifiers.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import re
import secrets
from dataclasses import dataclass

from .backend import Backend, Conflict, Expired


class NotFound(RuntimeError):
    pass


class InvalidState(RuntimeError):
    pass


class StopRequested(RuntimeError):
    pass


@dataclass(frozen=True)
class Principal:
    tenant: str
    subject: str

    def __post_init__(self):
        if not isinstance(self.tenant, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', self.tenant):
            raise ValueError('invalid tenant identifier')
        if not isinstance(self.subject, str) or not self.subject or len(self.subject) > 128:
            raise ValueError('invalid subject')


def _encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip('=')


class Grants:
    """Signed media-only grants, consumed atomically by the session state machine.

    Not an identity token, not an authorization server, and never accepted as an
    HTTP application credential. Expiry is checked by the backend clock on claim.
    """
    def __init__(self, key: str, cell: str = "default"):
        if len(key) < 32:
            raise ValueError('media signing key requires 32 characters')
        self.key, self.cell = key.encode(), cell

    def issue(self, record: dict) -> str:
        claims = {k: record[k] for k in ('sid', 'tenant', 'subject', 'transport', 'grant_version', 'incarnation')}
        claims['cell'] = self.cell
        payload = _encode(json.dumps(claims, sort_keys=True, separators=(',', ':')).encode())
        signature = _encode(hmac.new(self.key, ('media-v1.'+payload).encode(), hashlib.sha256).digest())
        return payload+'.'+signature

    def verify(self, token: str, transport: str | None = None) -> dict:
        if not isinstance(token, str) or not 40 <= len(token) <= 1024:
            raise ValueError('invalid media grant')
        try:
            payload, signature = token.split('.')
            expected = _encode(hmac.new(self.key, ('media-v1.'+payload).encode(), hashlib.sha256).digest())
            if not hmac.compare_digest(signature, expected):
                raise ValueError('invalid media signature')
            claims = json.loads(base64.urlsafe_b64decode(payload+'='*(-len(payload)%4)))
            if claims.get('cell') != self.cell:
                raise ValueError('wrong media cell')
            Principal(claims['tenant'], claims['subject'])
            if not re.fullmatch(r'[a-f0-9]{32}', claims['sid']):
                raise ValueError('invalid session id')
            if not re.fullmatch(r'[a-f0-9]{32}', claims['incarnation']):
                raise ValueError('invalid session incarnation')
            if type(claims['grant_version']) is not int or claims['grant_version'] < 1:
                raise ValueError('invalid grant version')
            if claims['transport'] not in {'websocket', 'webrtc', 'twilio', 'audiosocket'}:
                raise ValueError('invalid transport')
            if transport and claims['transport'] != transport:
                raise ValueError('wrong media audience')
            return claims
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeError) as exc:
            raise ValueError('malformed media grant') from exc


class SessionControl:
    def __init__(self, backend: Backend, *, tenant_limit=100, grant_seconds=60,
                 lease_seconds=9, call_seconds=900, retention_seconds=3600,
                 history_chars=12000, history_messages=24, persist_history=False):
        self.backend = backend
        self.limit, self.grant_s, self.lease_s = tenant_limit, grant_seconds, lease_seconds
        self.call_s, self.retention_s = call_seconds, retention_seconds
        self.history_chars, self.history_messages = history_chars, history_messages
        self.persist_history = persist_history

    @staticmethod
    def _event(doc, kind, now):
        doc['events'] = (doc.get('events', []) + [{'kind': kind, 'at': now, 'version': doc['version']}])[-20:]

    async def create(self, principal: Principal, transport: str, request_id: str):
        if transport not in {'websocket', 'webrtc', 'twilio', 'audiosocket'}:
            raise ValueError('unknown transport')
        if not 1 <= len(request_id) <= 128:
            raise ValueError('idempotency key must contain 1 to 128 characters')
        sid = hashlib.sha256(json.dumps([principal.tenant, principal.subject, request_id],
                                        separators=(',', ':')).encode()).hexdigest()[:32]
        for attempt in range(8):
            now, old = await self.backend.read(principal.tenant, sid)
            if old:
                if old['transport'] != transport or old['subject'] != principal.subject:
                    raise InvalidState('idempotency key reused with different parameters')
                return old
            doc = dict(sid=sid, tenant=principal.tenant, subject=principal.subject,
                       transport=transport, state='reserved', version=1, fence=0,
                       owner='', grant_version=1, incarnation=secrets.token_hex(16), created_at=now, deadline=now+self.call_s,
                       grant_until=now+min(self.grant_s, self.call_s), lease_until=0,
                       slot_until=now+min(self.grant_s, self.call_s),
                       retain_until=now+self.call_s+self.retention_s,
                       history_revision=0, reason='', events=[])
            self._event(doc, 'reserved', now)
            try:
                await self.backend.commit(principal.tenant, sid, 0, doc, self.limit)
                return doc
            except Conflict:
                await asyncio.sleep(min(.002*2**attempt, .05))
        raise Conflict('session creation contention')

    async def get(self, principal: Principal, sid: str):
        if not re.fullmatch(r'[0-9a-f]{32}', sid):
            raise NotFound('session not found')
        now, doc = await self.backend.read(principal.tenant, sid)
        if not doc or doc['subject'] != principal.subject:
            raise NotFound('session not found')
        effective = doc['state']
        if doc['state'] != 'closed' and now >= doc['deadline']:
            effective = 'expired'
        elif doc['state'] == 'reserved' and now >= doc['grant_until']:
            effective = 'expired'
        elif doc['state'] in {'active', 'stopping'} and now >= doc['lease_until']:
            effective = 'disconnected'
        return {**doc, 'effective_state': effective}

    def _ownership(self, doc, owner, fence, now, stopping=False):
        if doc['owner'] != owner or doc['fence'] != fence:
            raise InvalidState('stale media owner')
        if doc['state'] == 'stopping' and not stopping:
            raise StopRequested('stop requested')
        if doc['state'] not in ({'active', 'stopping'} if stopping else {'active'}):
            raise InvalidState('session is not active')
        guard = min(doc['lease_until'], doc['deadline'])
        if guard <= now:
            raise Expired('media lease expired')
        return guard

    async def _change(self, tenant, sid, fn):
        if not re.fullmatch(r'[0-9a-f]{32}', sid):
            raise NotFound('session not found')
        for attempt in range(8):
            now, doc = await self.backend.read(tenant, sid)
            if not doc:
                raise NotFound('session not found')
            expected = doc['version']
            doc['version'] += 1
            guard, checkpoint, changed = fn(doc, now)
            if not changed:
                doc['version'] = expected
                return doc
            try:
                await self.backend.commit(tenant, sid, expected, doc, self.limit, guard, checkpoint)
                return doc
            except Conflict:
                await asyncio.sleep(min(.002*2**attempt, .05))
        raise Conflict('session update contention')

    async def claim(self, claims, owner):
        def apply(doc, now):
            if any(doc[k] != claims[k] for k in ('tenant', 'subject', 'transport', 'grant_version', 'incarnation')):
                raise InvalidState('grant does not match session')
            if doc['state'] != 'reserved':
                raise InvalidState('media grant already consumed')
            guard = min(doc['grant_until'], doc['deadline'])
            if guard <= now:
                raise Expired('media grant expired')
            doc.update(state='active', owner=owner, fence=doc['fence']+1,
                       lease_until=min(now+self.lease_s, doc['deadline']))
            doc['lease_budget'] = doc['lease_until']-now
            doc['slot_until'] = doc['lease_until']
            self._event(doc, 'claimed', now)
            return guard, None, True
        return await self._change(claims['tenant'], claims['sid'], apply)

    async def renew(self, tenant, sid, owner, fence):
        def apply(doc, now):
            guard = self._ownership(doc, owner, fence, now)
            doc['lease_until'] = doc['slot_until'] = min(now+self.lease_s, doc['deadline'])
            doc['lease_budget'] = doc['lease_until']-now
            return guard, None, True
        return await self._change(tenant, sid, apply)

    def _history(self, messages):
        if not isinstance(messages, list) or len(messages) > self.history_messages:
            raise ValueError('history message budget exceeded')
        result = []
        total = 0
        for message in messages:
            if not isinstance(message, dict):
                raise ValueError('history must contain message objects')
            if set(message) != {'role', 'content'} or message['role'] not in {'user', 'assistant'}:
                raise ValueError('invalid history message')
            if not isinstance(message['content'], str):
                raise ValueError('history content must be text')
            total += len(message['content'])
            result.append(dict(message))
        if total > self.history_chars:
            raise ValueError('history character budget exceeded')
        return result if self.persist_history else []

    async def checkpoint(self, tenant, sid, owner, fence, revision, messages):
        saved = self._history(messages)
        def apply(doc, now):
            guard = self._ownership(doc, owner, fence, now, stopping=True)
            if revision <= doc['history_revision']:
                return guard, None, False
            doc['history_revision'] = revision
            self._event(doc, 'checkpoint', now)
            return guard, saved, True
        return await self._change(tenant, sid, apply)

    async def finish(self, tenant, sid, owner, fence, revision, messages, reason='completed'):
        saved = self._history(messages)
        if reason not in {'completed', 'client_stop', 'failed', 'drained', 'cancelled'}:
            raise ValueError('unknown close reason')
        def apply(doc, now):
            if doc['state'] == 'closed' and doc['owner'] == owner and doc['fence'] == fence:
                return 0, None, False
            guard = self._ownership(doc, owner, fence, now, stopping=True)
            update = saved if revision > doc['history_revision'] else None
            doc.update(state='closed', reason=reason, slot_until=0,
                       history_revision=max(revision, doc['history_revision']),
                       retain_until=now+self.retention_s)
            self._event(doc, 'closed', now)
            return guard, update, True
        return await self._change(tenant, sid, apply)

    async def stop(self, principal, sid):
        def apply(doc, now):
            if doc['subject'] != principal.subject:
                raise NotFound('session not found')
            if doc['state'] == 'closed':
                return 0, None, False
            if doc['state'] == 'reserved' or doc['lease_until'] <= now:
                doc.update(state='closed', reason='client_stop', slot_until=0,
                           retain_until=now+self.retention_s)
            else:
                doc['state'] = 'stopping'
            self._event(doc, 'stop_requested', now)
            return 0, None, True
        return await self._change(principal.tenant, sid, apply)

    async def reconnect(self, principal, sid):
        """Explicit fresh transport after owner loss, not transparent audio migration."""
        def apply(doc, now):
            if doc['subject'] != principal.subject:
                raise NotFound('session not found')
            if doc['state'] in {'closed', 'stopping'} or doc['deadline'] <= now:
                raise InvalidState('session cannot reconnect')
            until = doc['grant_until'] if doc['state'] == 'reserved' else doc['lease_until']
            if until > now:
                raise InvalidState('session still has a valid grant or media owner')
            doc.update(state='reserved', owner='', grant_version=doc['grant_version']+1,
                       grant_until=min(now+self.grant_s, doc['deadline']), lease_until=0)
            doc['slot_until'] = doc['grant_until']
            self._event(doc, 'reconnect_reserved', now)
            return doc['deadline'], None, True
        return await self._change(principal.tenant, sid, apply)
