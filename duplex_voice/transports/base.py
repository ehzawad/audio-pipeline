from __future__ import annotations
from abc import ABC, abstractmethod
import asyncio
from ..playback import PlaybackLedger


class Transport(ABC):
    name = "base"
    phone = False
    evidence = "unknown"
    in_rate = 16000
    out_rate = 24000

    def __init__(self):
        self.ledger = PlaybackLedger()
        self._timers: set[asyncio.TimerHandle] = set()

    def check_owner(self):
        guard = getattr(self, "media_guard", None)
        if guard is not None:
            guard.check()

    async def ready(self):
        pass

    @abstractmethod
    def frames(self): ...

    @abstractmethod
    async def write(self, samples, epoch: int, seq: int): ...

    async def mark(self, epoch: int, seq: int):
        pass

    @abstractmethod
    async def clear(self, epoch: int): ...

    async def event(self, data: dict):
        pass

    def estimate_receipt(self, epoch, seq, delay=.2):
        loop = asyncio.get_running_loop()
        def receipt():
            self._timers.discard(handle)
            self.ledger.acknowledge(epoch, seq)
        handle = loop.call_later(delay, receipt)
        self._timers.add(handle)

    async def close(self):
        for h in self._timers:
            h.cancel()
        self._timers.clear()
