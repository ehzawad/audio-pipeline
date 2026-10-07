"""One live media owner, guarded locally between infrequent shared-store heartbeats."""
from __future__ import annotations

import asyncio
import secrets
from contextlib import suppress

from .backend import Expired
from .service import StopRequested
from ..session import VoiceSession
from ..concurrency import Overloaded


class LeaseGuard:
    def __init__(self):
        self.until = 0.0
        self.lost = False

    def extend(self, request_started: float, seconds: float):
        # Use request START, not response arrival; network latency consumes the lease.
        self.until = request_started + seconds - min(.25, seconds/10)
        self.check()

    def check(self):
        if self.lost or asyncio.get_running_loop().time() >= self.until:
            raise Expired('local media ownership deadline reached')


class GuardedTransport:
    def __init__(self, inner, guard, prepared=False):
        self.inner, self.guard, self.prepared = inner, guard, prepared
        inner.media_guard = guard

    def __getattr__(self, name):
        return getattr(self.inner, name)

    async def ready(self):
        self.guard.check()
        if not self.prepared:
            await self.inner.ready()
        self.guard.check()

    async def write(self, samples, epoch, seq):
        self.guard.check()
        await self.inner.write(samples, epoch, seq)
        self.guard.check()

    async def mark(self, epoch, seq):
        self.guard.check()
        await self.inner.mark(epoch, seq)

    async def close(self):
        # The outer owner finalizes shared state BEFORE sending a socket close.
        # VoiceSession still owns its task tree; OwnedCall owns transport disposal.
        pass

    async def frames(self):
        async for samples in self.inner.frames():
            self.guard.check()
            yield samples


class OwnedCall:
    def __init__(self, runtime, record, owner, slot, started):
        self.rt, self.record, self.owner, self.slot = runtime, record, owner, slot
        self.guard = LeaseGuard()
        self.guard.extend(started, record['lease_budget'])
        self.closed = False
        self.running = False

    async def run(self, transport, prepared=False):
        self.running = True
        rt, doc = self.rt, self.record
        task = asyncio.current_task()
        rt.active.add(task)
        updates = asyncio.Queue(maxsize=1)
        session = None
        reason = 'completed'
        failure = None

        def snapshot(messages, revision):
            if updates.full():
                updates.get_nowait()  # latest snapshot wins; this is not an event log
            updates.put_nowait((revision, messages))

        async def heartbeat():
            nonlocal reason, failure
            try:
                while True:
                    remaining = self.guard.until-asyncio.get_running_loop().time()
                    await asyncio.sleep(min(rt.control.lease_s/3, max(.001, remaining)))
                    self.guard.check()
                    started = asyncio.get_running_loop().time()
                    async with asyncio.timeout(rt.settings.control_timeout):
                        renewed = await rt.control.renew(doc['tenant'], doc['sid'], self.owner, doc['fence'])
                    self.guard.extend(started, renewed['lease_budget'])
            except asyncio.CancelledError:
                raise
            except StopRequested:
                reason = 'client_stop'
                self.guard.lost = True
                task.cancel()
            except Exception as exc:
                reason, failure = 'failed', exc
                self.guard.lost = True
                rt.metrics.count('control.lease_lost')
                task.cancel()

        async def checkpoints():
            nonlocal reason, failure
            try:
                while True:
                    revision, messages = await updates.get()
                    async with asyncio.timeout(rt.settings.control_timeout):
                        await rt.control.checkpoint(doc['tenant'], doc['sid'], self.owner,
                                                    doc['fence'], revision, messages)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reason, failure = 'failed', exc
                self.guard.lost = True
                rt.metrics.count('control.checkpoint_failed')
                task.cancel()

        hb = asyncio.create_task(heartbeat())
        cp = asyncio.create_task(checkpoints())
        try:
            async with asyncio.timeout(rt.settings.control_timeout):
                history = await rt.control.backend.history(doc['tenant'], doc['sid']) if rt.control.persist_history else []
            self.guard.check()
            session = VoiceSession(GuardedTransport(transport, self.guard, prepared),
                rt.providers, rt.settings, rt.metrics, history=history,
                history_revision=doc['history_revision'], on_history=snapshot)
            await session.run()
        except asyncio.CancelledError:
            if reason == 'completed':
                reason = 'drained' if rt.cap.draining else 'cancelled'
            # Ownership has one cleanup owner. Re-raise cancellation only after cleanup.
        except Exception as exc:
            reason, failure = 'failed', exc
            rt.metrics.count('call.failure')
            with suppress(Exception):
                await transport.event({'type': 'error', 'message': 'Call ended: media, capacity or provider failure.'})
        finally:
            async def cleanup():
                hb.cancel()
                cp.cancel()
                await asyncio.gather(hb, cp, return_exceptions=True)
                self.guard.lost = True
                try:
                    await asyncio.wait_for(rt.control.finish(doc['tenant'], doc['sid'], self.owner,
                        doc['fence'], session.history_revision if session else doc['history_revision'],
                        session.history if session else [], reason), rt.settings.control_timeout)
                except Exception:
                    # Expired or superseded owners must not overwrite a new owner.
                    rt.metrics.count('control.finalize_failed')
                finally:
                    with suppress(Exception):
                        await asyncio.wait_for(transport.close(), rt.settings.io_timeout)
                    await self.release()
                    rt.active.discard(task)
                    if failure:
                        rt.metrics.count('control.session_failed')
            finalizer = asyncio.create_task(cleanup())
            rt.finalizers.add(finalizer)
            def finalized(done):
                rt.finalizers.discard(done)
                if not done.cancelled():
                    done.exception()
            finalizer.add_done_callback(finalized)
            await asyncio.shield(finalizer)

    async def abort(self, reason='failed'):
        if self.running:
            return  # a tracked finalizer already owns disposal
        try:
            async with asyncio.timeout(self.rt.settings.control_timeout):
                await self.rt.control.finish(self.record['tenant'], self.record['sid'], self.owner,
                    self.record['fence'], self.record['history_revision'], [], reason)
        finally:
            await self.release()

    async def release(self):
        if not self.closed:
            self.closed = True
            await self.slot.__aexit__(None, None, None)


class GatewayRuntime:
    def __init__(self, control, settings, metrics, cap, providers):
        self.control, self.settings, self.metrics = control, settings, metrics
        self.cap, self.providers = cap, providers
        self.active = set()
        self.finalizers = set()
        self.worker = settings.worker_id or secrets.token_hex(12)

    async def acquire(self, claims, slot=None):
        slot = slot or self.cap.slot()
        if slot is not None and not getattr(slot, '_voice_entered', False):
            await slot.__aenter__()
            slot._voice_entered = True
        try:
            if self.cap.draining:
                raise Overloaded('gateway began draining during transport setup')
            owner = self.worker+':'+secrets.token_hex(16)
            started = asyncio.get_running_loop().time()
            async with asyncio.timeout(self.settings.control_timeout):
                record = await self.control.claim(claims, owner)
            return OwnedCall(self, record, owner, slot, started)
        except BaseException:
            await slot.__aexit__(None, None, None)
            raise

    async def drain(self):
        self.cap.draining = True
        if self.active:
            _, pending = await asyncio.wait(list(self.active), timeout=self.settings.drain_seconds)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        if self.finalizers:
            await asyncio.gather(*list(self.finalizers), return_exceptions=True)
