"""Audio primitives.

Recognizer input is mono float32 at INTERNAL_RATE (16 kHz). TTS and media
retain explicit rates; no module should assume that every stream is 16 kHz.
Transports convert to and from their wire formats at the edges:
  browser WS -> native-rate PCM16 in, PCM16 24 kHz out
  browser RTC -> Opus/RTP in, Opus/RTP out via aiortc
  Twilio   -> G.711 mu-law 8 kHz, base64 inside JSON, both directions
  Asterisk -> signed linear PCM16 8 kHz (AudioSocket), both directions
"""
from __future__ import annotations

import numpy as np

INTERNAL_RATE = 16_000
VAD_FRAME = 512  # samples per VAD frame at 16 kHz = 32 ms (Silero's required size)


# --------------------------------------------------------------------------- PCM16
def pcm16_to_float(b: bytes) -> np.ndarray:
    """Little-endian signed 16-bit PCM bytes -> float32 in [-1, 1)."""
    if len(b) % 2:
        raise ValueError("PCM16 payload must end on a sample boundary")
    return np.frombuffer(b, dtype="<i2").astype(np.float32) / 32768.0


def float_to_pcm16(x: np.ndarray) -> bytes:
    """float32 in [-1, 1] -> little-endian signed 16-bit PCM bytes (clipped)."""
    x = np.asarray(x, dtype=np.float32)
    if x.ndim != 1 or not np.isfinite(x).all():
        raise ValueError("audio must be finite mono samples")
    return np.clip(np.rint(x * 32768.0), -32768, 32767).astype("<i2").tobytes()


# --------------------------------------------------------------------------- G.711 mu-law
# Telephone audio: 8 bits per sample, logarithmic. Small amplitudes get fine steps,
# loud ones coarse steps, which matches how hearing works. 64 kbit/s = 8000 samples * 8 bits.
_BIAS = 0x84   # 132 (33 in the 14-bit domain): offsets magnitudes so segments line up
_SEG_END = np.array([0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF])


def mulaw_encode(x: np.ndarray) -> bytes:
    """Bit-exact with the ITU/Sun reference (and Python's old audioop.lin2ulaw)."""
    v = np.frombuffer(float_to_pcm16(x), dtype="<i2").astype(np.int32) >> 2   # 14-bit, arithmetic shift
    neg = v < 0
    mag = np.minimum(np.where(neg, -v, v), 8159) + (_BIAS >> 2)    # 33 .. 8192
    seg = np.searchsorted(_SEG_END, mag)                           # which of 8 log segments
    mant = (mag >> (np.minimum(seg, 7) + 1)) & 0x0F                # 4-bit step inside it
    u = np.where(seg >= 8, 0x7F, (np.minimum(seg, 7) << 4) | mant)
    u = u ^ np.where(neg, 0x7F, 0xFF)                              # sign + bit inversion on the wire
    return u.astype(np.uint8).tobytes()


def mulaw_decode(b: bytes) -> np.ndarray:
    u = ~np.frombuffer(b, dtype=np.uint8).astype(np.int32) & 0xFF
    sign = u & 0x80
    exponent = (u >> 4) & 0x07
    mantissa = u & 0x0F
    mag = (((mantissa << 3) + _BIAS) << exponent) - _BIAS
    return np.where(sign != 0, -mag, mag).astype(np.float32) / 32768.0


# --------------------------------------------------------------------------- resampling
class Resampler:
    """Streaming sample-rate converter (identity when rates match).

    Streaming matters: resampling each chunk independently creates clicks at chunk
    boundaries because the filter has no history. soxr's ResampleStream keeps state.
    """

    def __init__(self, in_rate: int, out_rate: int):
        self.in_rate, self.out_rate = in_rate, out_rate
        self._rs = None
        if in_rate != out_rate:
            import soxr

            self._rs = soxr.ResampleStream(in_rate, out_rate, 1, dtype="float32", quality="HQ")

    def process(self, x: np.ndarray) -> np.ndarray:
        x = np.ascontiguousarray(x, dtype=np.float32)
        return x if self._rs is None else self._rs.resample_chunk(x)

    def flush(self) -> np.ndarray:
        if self._rs is None:
            return np.zeros(0, np.float32)
        return self._rs.resample_chunk(np.zeros(0, np.float32), last=True)


# --------------------------------------------------------------------------- framing
class Framer:
    """Re-chunks an arbitrary-size stream into fixed-size frames (e.g. 512 samples for VAD)."""

    def __init__(self, size: int):
        if size <= 0:
            raise ValueError("frame size must be positive")
        self.size = size
        self._buf = np.zeros(0, np.float32)

    def push(self, x: np.ndarray) -> list[np.ndarray]:
        self._buf = np.concatenate([self._buf, x]) if self._buf.size else np.asarray(x, np.float32)
        n = self._buf.size // self.size
        frames = [self._buf[i * self.size:(i + 1) * self.size].copy() for i in range(n)]
        self._buf = self._buf[n * self.size:]
        return frames


class PCMDecoder:
    """HTTP/WebSocket boundaries need not align to 16-bit samples."""
    def __init__(self):
        self.pending = b""

    def push(self, chunk: bytes) -> np.ndarray:
        data = self.pending + chunk
        end = len(data) - len(data) % 2
        self.pending = data[end:]
        return pcm16_to_float(data[:end])

    def finish(self):
        if self.pending:
            raise ValueError("truncated PCM16 stream")


class Packetizer:
    """Pad only the final frame of a phrase, never every provider chunk."""
    def __init__(self, rate: int):
        self.size = rate // 50
        self.framer = Framer(self.size)

    def push(self, samples):
        return self.framer.push(samples)

    def finish(self):
        buf = self.framer._buf
        self.framer._buf = np.zeros(0, np.float32)
        return [np.pad(buf, (0, self.size-len(buf)))] if len(buf) else []
