from __future__ import annotations
import asyncio
import base64
import json
from xml.etree import ElementTree as ET
from ..audio import mulaw_encode, mulaw_decode
from .base import Transport


class TwilioTransport(Transport):
    name = "twilio"
    phone = True
    evidence = "twilio_playback_mark"
    in_rate = out_rate = 8000

    def __init__(self, ws, validate_start):
        super().__init__()
        self.ws, self.validate_start = ws, validate_start
        self.sid = ""
        self.lock = asyncio.Lock()

    async def receive(self):
        raw = await self.ws.receive_text()
        if len(raw) > 32768:
            raise ValueError("oversized Twilio event")
        return json.loads(raw)

    async def ready(self):
        first = await self.receive()
        if first.get("event") == "connected":
            first = await self.receive()
        if first.get("event") != "start":
            raise ValueError("Twilio must send start before media")
        start = first["start"]
        fmt = start.get("mediaFormat", {})
        if fmt != {"encoding": "audio/x-mulaw", "sampleRate": 8000, "channels": 1}:
            raise ValueError("unsupported Twilio media format")
        self.validate_start(start)
        self.sid = start["streamSid"]
        if not self.sid or len(self.sid) > 128:
            raise ValueError("invalid Stream SID")

    async def frames(self):
        while True:
            msg = await self.receive()
            if msg.get("streamSid", self.sid) != self.sid:
                raise ValueError("cross-stream media rejected")
            ev = msg.get("event")
            if ev == "stop":
                return
            if ev == "media":
                media = msg["media"]
                if media.get("track", "inbound") != "inbound":
                    continue
                b = base64.b64decode(media["payload"], validate=True)
                if len(b) > 1600:
                    raise ValueError("Twilio audio payload exceeds 200 ms")
                if b:
                    yield mulaw_decode(b)
            elif ev == "mark":
                try:
                    epoch, seq = map(int, msg["mark"]["name"].split(":"))
                    self.ledger.acknowledge(epoch, seq)
                except (ValueError, KeyError):
                    raise ValueError("invalid playback marker")
            elif ev == "dtmf":
                # No raw digit logging. This baseline does not collect PINs or execute
                # transactions on digits. Add a purpose-specific control-plane handler.
                pass

    async def send(self, data, epoch=None):
        async with self.lock:
            if epoch is not None:
                self.check_owner()
            if epoch is not None and epoch != self.ledger.epoch:
                raise asyncio.CancelledError
            await self.ws.send_text(json.dumps({"streamSid": self.sid, **data}, separators=(",", ":")))

    async def write(self, samples, epoch, seq):
        await self.send({"event": "media", "media": {
            "payload": base64.b64encode(mulaw_encode(samples)).decode()}}, epoch)

    async def mark(self, epoch, seq):
        await self.send({"event": "mark", "mark": {"name": f"{epoch}:{seq}"}}, epoch)

    async def clear(self, epoch):
        # Old epoch has ALREADY been invalidated locally before this message is sent.
        await self.send({"event": "clear"})

    async def close(self):
        await super().close()
        try:
            await self.ws.close()
        except (RuntimeError, OSError):
            pass


def twiml(url: str, token: str) -> str:
    root = ET.Element("Response")
    connect = ET.SubElement(root, "Connect")
    stream = ET.SubElement(connect, "Stream", {"url": url})
    ET.SubElement(stream, "Parameter", {"name": "ticket", "value": token})
    ET.SubElement(root, "Say").text = "The assistant is unavailable. Please try again later."
    return ET.tostring(root, encoding="unicode")
