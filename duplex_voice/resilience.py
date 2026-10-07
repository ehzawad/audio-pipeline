"""Fail-fast provider bulkheads and generation-safe circuit breakers.

No waiting room and no automatic replay after partial speech. Human cancellation
is not a provider fault. Old in-flight completions cannot close a newer circuit.
"""
from __future__ import annotations
import asyncio
import time
from contextlib import asynccontextmanager, aclosing
from .concurrency import Overloaded


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
