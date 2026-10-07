"""Fail-fast provider bulkheads and generation-safe circuit breakers.

No waiting room and no automatic replay after partial speech. Human cancellation
is not a provider fault. Old in-flight completions cannot close a newer circuit.
"""
from __future__ import annotations
import asyncio
import logging
import time
from contextlib import asynccontextmanager, aclosing
from .concurrency import Overloaded

log = logging.getLogger("duplex.resilience")


class Circuit:
    def __init__(self, capacity=8, threshold=5, reset_seconds=10, clock=time.monotonic):
        if capacity < 1 or threshold < 1 or reset_seconds <= 0:
            raise ValueError('positive circuit limits required')
        self.capacity, self.threshold, self.reset_seconds, self.clock = capacity, threshold, reset_seconds, clock
        self.active, self.failures, self.epoch = 0, 0, 0
        self.state, self.until = 'closed', 0.0

    def trip(self):
        self.state, self.until = 'open', self.clock()+self.reset_seconds
        self.epoch += 1

    @asynccontextmanager
    async def request(self):
        # All state changes are synchronous on the gateway's one event loop.
        if self.active >= self.capacity:
            raise Overloaded('provider bulkhead has no capacity')
        if self.state == 'half_open' or (self.state == 'open' and self.clock() < self.until):
            raise Overloaded('provider circuit is open')
        if self.state == 'open':
            self.state = 'half_open'
        epoch, probing = self.epoch, self.state == 'half_open'
        self.active += 1
        try:
            yield
        except (asyncio.CancelledError, GeneratorExit):
            if epoch == self.epoch and probing:
                self.state, self.until = 'open', self.clock()  # neutral: next request may probe
            raise
        except Exception:
            if epoch == self.epoch:
                self.failures += 1
                if probing or self.failures >= self.threshold:
                    self.trip()
            raise
        else:
            if epoch == self.epoch:
                self.state, self.failures = 'closed', 0
        finally:
            self.active -= 1


class GuardedASR:
    def __init__(self, inner, gate): self.inner, self.gate = inner, gate
    async def recognize(self, audio, on_partial):
        async with self.gate.request():
            return await self.inner.recognize(audio, on_partial)


class GuardedLLM:
    def __init__(self, inner, gate): self.inner, self.gate = inner, gate
    async def stream(self, messages, system):
        async with self.gate.request():
            async with aclosing(self.inner.stream(messages, system)) as stream:
                async for token in stream:
                    yield token


class GuardedTTS:
    def __init__(self, inner, gate):
        self.inner, self.gate, self.sample_rate = inner, gate, inner.sample_rate
    async def synthesize(self, text):
        async with self.gate.request():
            async with aclosing(self.inner.synthesize(text)) as stream:
                async for chunk in stream:
                    yield chunk


class HedgedTTS:
    """Prebuffer each phrase and race one duplicate request when the first is slow.

    Hosted TTS can stream a short head chunk and then pause, or stall for many seconds.
    Nothing of a phrase is yielded until one attempt holds `prebuffer_s` of audio (or has
    finished), so a losing duplicate is discarded unheard. After that point the winner
    is streamed as is: audio that may have been heard is never requested again.
    """
    def __init__(self, inner, prebuffer_s=0., hedge_after_s=0.):
        self.inner, self.sample_rate = inner, inner.sample_rate
        self.prebuffer = max(1, int(prebuffer_s * inner.sample_rate))
        self.hedge_after = hedge_after_s or None

    async def synthesize(self, text):
        queues, tasks, failed = [], [], set()
        ready = asyncio.Queue()

        async def attempt(i, q):
            have, signalled = 0, False
            try:
                async with aclosing(self.inner.synthesize(text)) as stream:
                    async for samples in stream:
                        q.put_nowait(samples)
                        have += len(samples)
                        if not signalled and have >= self.prebuffer:
                            signalled = True
                            ready.put_nowait(i)
                q.put_nowait(None)
            except Exception as exc:
                failed.add(i)
                q.put_nowait(exc)
            if not signalled:
                ready.put_nowait(i)

        def launch():
            queues.append(asyncio.Queue())
            tasks.append(asyncio.create_task(attempt(len(tasks), queues[-1])))

        launch()
        try:
            while True:
                try:
                    async with asyncio.timeout(self.hedge_after if len(tasks) == 1 else None):
                        winner = await ready.get()
                except TimeoutError:
                    log.info('{"type":"tts_hedge"}')
                    launch()
                    continue
                # A failed attempt only decides the outcome when no other attempt remains.
                if winner not in failed or all(t.done() for t in tasks):
                    break
            for i, task in enumerate(tasks):
                if i != winner:
                    task.cancel()
            q = queues[winner]
            while (item := await q.get()) is not None:
                if isinstance(item, Exception):
                    raise item
                yield item
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


class RateBudget:
    """Bound audio-time or message bursts against a monotonic wall clock."""
    def __init__(self, rate, burst, clock=time.monotonic):
        if rate <= 0 or burst <= 0:
            raise ValueError('positive rate and burst required')
        self.rate, self.burst, self.clock = rate, burst, clock
        self.tokens, self.updated = float(burst), clock()

    def consume(self, amount=1):
        now=self.clock()
        self.tokens=min(self.burst, self.tokens+max(0,now-self.updated)*self.rate)
        self.updated=now
        if amount < 0 or amount > self.tokens:
            raise Overloaded('input exceeds its real-time rate budget')
        self.tokens-=amount
