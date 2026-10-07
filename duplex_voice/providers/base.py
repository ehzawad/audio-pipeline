from __future__ import annotations
from typing import AsyncIterator, Callable, Protocol
import numpy as np

Audio = AsyncIterator[np.ndarray]  # mono float32, 16 kHz for ASR
Partial = Callable[[str], None]


class ASR(Protocol):
    async def recognize(self, audio: Audio, on_partial: Partial) -> str: ...


class LLM(Protocol):
    def stream(self, messages: list[dict], system: str) -> AsyncIterator[str]: ...


class TTS(Protocol):
    sample_rate: int
    def synthesize(self, text: str) -> Audio: ...
