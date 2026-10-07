"""Private-network Asterisk AudioSocket. No remote clear or playout ACK exists here."""
from __future__ import annotations
import asyncio
import uuid
from ..audio import float_to_pcm16, pcm16_to_float
from .base import Transport


class AudioSocketTransport(Transport):
    name = "audiosocket"
    phone = True
    evidence = "paced_dispatch_estimate"
    in_rate = out_rate = 8000

    def __init__(self, reader, writer):
        super().__init__()
        self.reader, self.writer = reader, writer
        self.lock = asyncio.Lock()

    async def read(self):
        header = await self.reader.readexactly(3)
        n = int.from_bytes(header[1:], "big")
        if n > 3200:
            raise ValueError("AudioSocket frame exceeds 200 ms")
        return header[0], await self.reader.readexactly(n)

    async def ready(self):
        kind, b = await self.read()
        if kind != 0x01 or len(b) != 16:
            raise ValueError("AudioSocket first packet must contain a binary UUID")
        self.call_uuid = str(uuid.UUID(bytes=b))

    async def frames(self):
        try:
            while True:
                kind, b = await self.read()
                if kind == 0x10:
                    if b:
                        yield pcm16_to_float(b)
                elif kind in {0, 255}:
                    return
                elif kind == 0x03 and len(b) == 1:
                    pass  # purpose-specific DTMF handler intentionally not implemented
                else:
                    raise ValueError("only 8 kHz PCM AudioSocket is negotiated by this adapter")
        except asyncio.IncompleteReadError:
            return

    async def write(self, samples, epoch, seq):
        b = float_to_pcm16(samples)
        if len(b) != 320:
            raise ValueError("outbound AudioSocket frames must be exactly 20 ms")
        async with self.lock:
            self.check_owner()
            if epoch != self.ledger.epoch:
                raise asyncio.CancelledError
            self.writer.write(bytes([0x10]) + len(b).to_bytes(2, "big") + b)
            await self.writer.drain()
        # Not an acknowledgment: kernel, Asterisk, RTP and handset buffering remain.
        self.estimate_receipt(epoch, seq, .2)

    async def clear(self, epoch):
        # Core invalidates its bounded output queue through epoch checks. Bytes already
        # in TCP/Asterisk/RTP cannot be retracted. Sending an invented clear opcode is wrong.
        pass

    async def close(self):
        await super().close()
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except OSError:
            pass
