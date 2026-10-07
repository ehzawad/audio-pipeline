"""Browser: native-rate PCM upstream; framed PCM24k downstream, with playback receipts."""
from __future__ import annotations
import asyncio
import json
import struct
from ..audio import float_to_pcm16, pcm16_to_float
from .base import Transport
from ..resilience import RateBudget

# epoch and sequence are uint32 little-endian. Remaining bytes are mono PCM16 at 24 kHz.
HEADER = struct.Struct("<II")


class WebSocketTransport(Transport):
    name = "websocket"
    evidence = "browser_renderer_ack"

    def __init__(self, ws):
        super().__init__()
        self.ws = ws
        self.lock = asyncio.Lock()
        self.message_budget = RateBudget(100, 200)

    async def ready(self):
        raw = await self.ws.receive_text()
        if len(raw) > 2048:
            raise ValueError("oversized hello")
        hello = json.loads(raw)
        if type(hello.get("sample_rate")) is not int or hello.get("type") != "hello" or hello.get("sample_rate") not in {16000, 24000, 32000, 44100, 48000}:
            raise ValueError("expected hello with supported native microphone sample rate")
        self.in_rate = int(hello["sample_rate"])
        await self.event({"type": "audio_format", "sample_rate": self.out_rate,
                          "header_bytes": 8})

    async def frames(self):
        while True:
            msg = await self.ws.receive()
            self.message_budget.consume()
            if msg["type"] == "websocket.disconnect":
                return
            if msg.get("bytes") is not None:
                data = msg["bytes"]
                if len(data) > self.in_rate // 5 * 2:
                    raise ValueError("audio messages must not exceed 200 ms")
                if data:
                    yield pcm16_to_float(data)
            elif msg.get("text"):
                if len(msg["text"]) > 2048:
                    raise ValueError("oversized control event")
                event = json.loads(msg["text"])
                if event.get("type") == "hangup":
                    return
                if event.get("type") == "played":
                    if any(type(event.get(k)) is not int or not 1 <= event[k] <= 0xffffffff for k in ('epoch', 'seq')):
                        raise ValueError('invalid playback receipt')
                    self.ledger.acknowledge(event["epoch"], event["seq"])

    async def write(self, samples, epoch, seq):
        async with self.lock:
            self.check_owner()
            if epoch != self.ledger.epoch:
                raise asyncio.CancelledError
            await self.ws.send_bytes(HEADER.pack(epoch, seq) + float_to_pcm16(samples))

    async def clear(self, epoch):
        await self.event({"type": "clear", "epoch": epoch})

    async def event(self, data):
        async with self.lock:
            await self.ws.send_text(json.dumps(data, separators=(",", ":")))

    async def close(self):
        await super().close()
        try:
            await self.ws.close()
        except (RuntimeError, OSError):
            pass
