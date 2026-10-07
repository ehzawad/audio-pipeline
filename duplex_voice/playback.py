"""Text generation, media dispatch and playout receipts are three different facts.

Only whole completed phrases are retained after an interruption. Without word/audio
alignment, proportional word counts would invent precision we do not possess.
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass


@dataclass
class Phrase:
    text: str
    end_seq: int


class PlaybackLedger:
    def __init__(self):
        self.epoch = 0
        self.sent = self.acked = 0
        self.phrases: list[Phrase] = []
        self._changed = asyncio.Event()

    def begin(self) -> int:
        self.epoch += 1
        self.sent = self.acked = 0
        self.phrases = []
        self._changed.set()
        return self.epoch

    def issue(self, epoch: int) -> int:
        if epoch != self.epoch:
            raise asyncio.CancelledError("stale output epoch")
        self.sent += 1
        return self.sent

    def acknowledge(self, epoch: int, seq: int) -> bool:
        if epoch != self.epoch or not self.acked < seq <= self.sent:
            return False
        self.acked = seq
        self._changed.set()
        return True

    def phrase_sent(self, epoch: int, text: str, end_seq: int):
        if epoch == self.epoch:
            self.phrases.append(Phrase(text, end_seq))

    def completed_text(self) -> str:
        return " ".join(p.text for p in self.phrases if p.end_seq <= self.acked)

    async def credit(self, epoch: int, window: int, timeout: float):
        """Do not let a slow renderer accumulate unbounded unacknowledged speech."""
        async with asyncio.timeout(timeout):
            while epoch == self.epoch and self.sent - self.acked >= window:
                self._changed.clear()
                await self._changed.wait()
        if epoch != self.epoch:
            raise asyncio.CancelledError

    async def drain(self, epoch: int, timeout: float):
        async with asyncio.timeout(timeout):
            while epoch == self.epoch and self.acked < self.sent:
                self._changed.clear()
                await self._changed.wait()
        if epoch != self.epoch:
            raise asyncio.CancelledError
