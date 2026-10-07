"""Atomic compare-and-swap storage with tenant admission and optional checkpoint.

SQLite is for a single host. Redis is for a cell of gateways. Both apply the same
Python state machine; the storage transaction closes races and checks expiration
against the storage clock. All Redis keys for a tenant share a Cluster hash tag.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Protocol


class Conflict(RuntimeError):
    pass


class Capacity(RuntimeError):
    pass


class Expired(RuntimeError):
    pass


class Backend(Protocol):
    async def read(self, tenant: str, sid: str) -> tuple[float, dict | None]: ...
    async def commit(self, tenant: str, sid: str, expected: int, record: dict,
                     limit: int, guard_until: float = 0, checkpoint: list | None = None): ...
    async def history(self, tenant: str, sid: str) -> list: ...
    async def healthy(self) -> bool: ...
    async def close(self): ...


class SQLiteBackend:
    def __init__(self, path: str = ':memory:', clock=time.time):
        if path != ':memory:':
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=2)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA busy_timeout=2000')
        self.db.execute('''CREATE TABLE IF NOT EXISTS voice_sessions (
            tenant TEXT NOT NULL, sid TEXT NOT NULL, version INTEGER NOT NULL,
            body TEXT NOT NULL, slot_until REAL NOT NULL, retain_until REAL NOT NULL,
            history TEXT NOT NULL DEFAULT '[]', PRIMARY KEY(tenant,sid))''')
        self.db.execute('CREATE INDEX IF NOT EXISTS voice_slots ON voice_sessions(tenant,slot_until)')
        self.db.execute('CREATE INDEX IF NOT EXISTS voice_retention ON voice_sessions(retain_until)')
        self.lock, self.clock = threading.Lock(), clock

    async def read(self, tenant, sid):
        def run():
            with self.lock:
                now = self.clock()
                row = self.db.execute('SELECT body FROM voice_sessions WHERE tenant=? AND sid=? AND retain_until>?',
                                      (tenant, sid, now)).fetchone()
                return now, json.loads(row[0]) if row else None
        return await asyncio.to_thread(run)

    async def commit(self, tenant, sid, expected, record, limit, guard_until=0, checkpoint=None):
        def run():
            with self.lock:
                now = self.clock()
                self.db.execute('BEGIN IMMEDIATE')
                try:
                    self.db.execute('DELETE FROM voice_sessions WHERE retain_until<=?', (now,))
                    old = self.db.execute('SELECT version,slot_until,history FROM voice_sessions WHERE tenant=? AND sid=?',
                                          (tenant, sid)).fetchone()
                    if (old[0] if old else 0) != expected:
                        raise Conflict('record changed')
                    if guard_until and guard_until <= now:
                        raise Expired('ownership or grant expired during transaction')
                    if record['slot_until'] and record['slot_until'] <= now:
                        raise Expired('new reservation expired before commit')
                    if record['slot_until'] > now and (old is None or old[1] <= now):
                        n = self.db.execute('SELECT count(*) FROM voice_sessions WHERE tenant=? AND slot_until>?',
                                            (tenant, now)).fetchone()[0]
                        if n >= limit:
                            raise Capacity('tenant concurrency limit reached')
                    history = json.dumps(checkpoint) if checkpoint is not None else old[2] if old else '[]'
                    self.db.execute('INSERT OR REPLACE INTO voice_sessions VALUES(?,?,?,?,?,?,?)',
                                    (tenant, sid, expected+1, json.dumps(record, separators=(',', ':')),
                                     record['slot_until'], record['retain_until'], history))
                    self.db.execute('COMMIT')
                except BaseException:
                    self.db.execute('ROLLBACK')
                    raise
        return await asyncio.to_thread(run)

    async def history(self, tenant, sid):
        def run():
            with self.lock:
                row = self.db.execute('SELECT history FROM voice_sessions WHERE tenant=? AND sid=? AND retain_until>?',
                                      (tenant, sid, self.clock())).fetchone()
                return json.loads(row[0]) if row else []
        return await asyncio.to_thread(run)

    async def healthy(self):
        await self.read('__health__', '__health__')
        return True

    async def close(self):
        def run():
            with self.lock:
                self.db.close()
        await asyncio.to_thread(run)


READ = """
local t=redis.call('TIME')
return {t[1],t[2],redis.call('GET',KEYS[1]) or ''}
"""
CAS = """
local t=redis.call('TIME')
local now=tonumber(t[1])+tonumber(t[2])/1000000
local old=redis.call('GET',KEYS[1])
local version=0
if old then version=cjson.decode(old).version end
if version~=tonumber(ARGV[1]) then return 'conflict' end
local guard=tonumber(ARGV[4])
if guard>0 and guard<=now then return 'expired' end
local doc=cjson.decode(ARGV[2])
if doc.slot_until>0 and doc.slot_until<=now then return 'expired' end
redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',now)
if doc.slot_until>now and not redis.call('ZSCORE',KEYS[2],ARGV[6]) then
  if redis.call('ZCARD',KEYS[2])>=tonumber(ARGV[3]) then return 'capacity' end
end
if doc.slot_until>now then redis.call('ZADD',KEYS[2],doc.slot_until,ARGV[6])
else redis.call('ZREM',KEYS[2],ARGV[6]) end
local ttl=math.max(1,math.ceil((doc.retain_until-now)*1000))
redis.call('SET',KEYS[1],ARGV[2],'PX',ttl)
if ARGV[5]~='' then redis.call('SET',KEYS[3],ARGV[5],'PX',ttl)
elseif redis.call('EXISTS',KEYS[3])==1 then redis.call('PEXPIRE',KEYS[3],ttl) end
local last=redis.call('ZREVRANGE',KEYS[2],0,0,'WITHSCORES')
if #last>0 then redis.call('PEXPIRE',KEYS[2],math.max(1000,math.ceil((tonumber(last[2])-now)*1000)+1000)) end
return 'ok'
"""


class RedisBackend:
    def __init__(self, url: str, cell: str = 'default', client=None):
        if client is None:
            from redis.asyncio import Redis
            client = Redis.from_url(url, decode_responses=True, socket_timeout=2,
                                    socket_connect_timeout=2, max_connections=64)
        self.client, self.cell = client, cell
        # register_script uses EVALSHA and reloads after NOSCRIPT/Redis restart.
        self._read_script = client.register_script(READ)
        self._cas_script = client.register_script(CAS)

    def keys(self, tenant, sid):
        tag = hashlib.sha256((self.cell+'\0'+tenant).encode()).hexdigest()[:32]
        prefix = 'voice:{'+tag+'}:'
        return prefix+'session:'+sid, prefix+'slots', prefix+'history:'+sid

    async def read(self, tenant, sid):
        result = await self._read_script(keys=[self.keys(tenant, sid)[0]])
        return int(result[0])+int(result[1])/1e6, json.loads(result[2]) if result[2] else None

    async def commit(self, tenant, sid, expected, record, limit, guard_until=0, checkpoint=None):
        result = await self._cas_script(keys=list(self.keys(tenant, sid)), args=[expected,
            json.dumps(record, separators=(',', ':')), limit, guard_until,
            json.dumps(checkpoint) if checkpoint is not None else '', sid])
        errors = {'conflict': Conflict, 'expired': Expired, 'capacity': Capacity}
        if result in errors:
            raise errors[result](result)
        if result != 'ok':
            raise RuntimeError('unexpected control-store response')

    async def history(self, tenant, sid):
        data = await self.client.get(self.keys(tenant, sid)[2])
        return json.loads(data) if data else []

    async def healthy(self):
        return bool(await self.client.ping())

    async def close(self):
        await self.client.aclose()
