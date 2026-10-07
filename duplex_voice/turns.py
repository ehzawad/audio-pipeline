"""A sample-clock state machine. Speech detection is not semantic turn detection."""
from __future__ import annotations
from collections import deque
from dataclasses import dataclass
import math
import numpy as np


@dataclass
class Start:
    preroll: list[np.ndarray]


@dataclass
class End:
    silence_ms: float


class UtteranceLimit(RuntimeError):
    pass


class TurnDetector:
    def __init__(self, *, threshold=.5, start_ms=160, end_ms=480,
                 barge_ms=224, preroll_ms=256, max_seconds=30):
        self.threshold, self.start_ms, self.end_ms = threshold, start_ms, end_ms
        self.barge_ms, self.max_seconds = barge_ms, max_seconds
        self.ring = deque(maxlen=math.ceil((preroll_ms + max(start_ms, barge_ms)) / 32))
        self.speaking = False
        self.run = self.silence = self.length = 0

    def push(self, frame: np.ndarray, prob: float, bot_active: bool) -> Start | End | None:
        if frame.shape != (512,) or not 0 <= prob <= 1:
            raise ValueError("invalid VAD frame or probability")
        if not self.speaking:
            self.ring.append(frame.copy())
            self.run = self.run + 32 if prob >= self.threshold else 0
            if self.run < (self.barge_ms if bot_active else self.start_ms):
                return None
            self.speaking = True
            self.silence = self.length = self.run = 0
            frames = list(self.ring)
            self.ring.clear()
            return Start(frames)
        self.length += 32
        self.silence = self.silence + 32 if prob < max(0, self.threshold - .15) else 0
        if self.length >= self.max_seconds * 1000:
            # Do not invent an endpoint while the person is still speaking.
            raise UtteranceLimit("continuous speech exceeded configured limit")
        if self.silence >= self.end_ms:
            self.speaking = False
            actual = self.silence
            self.silence = self.run = 0
            return End(actual)
        return None
