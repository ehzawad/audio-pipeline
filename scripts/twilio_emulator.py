"""Local stand-in for Twilio's media edge, so the gateway's Twilio route runs on open-source telephony.

Asterisk hands each call to this process over AudioSocket (extension 7002 in the lab). For
that call it does what Twilio does for an inbound call to a number whose webhook is the
gateway: a signed form POST to /twilio/voice, TwiML parsing, a signed Media Streams WebSocket
to the <Stream> URL carrying its <Parameter> ticket, mu-law media in both directions, mark
echoes once the audio queued before them has been played out, clear (which drops queued
audio and returns the pending marks at once), DTMF and stop.

This is an emulator written against Twilio's public Media Streams protocol and run with lab
credentials. It is not Twilio: no PSTN, Twilio network, Twilio jitter buffer or Twilio-issued
signature is involved, and it signs the stream URL in the form the gateway validates. Never
give it a real account's Auth Token. Its marks mean "played out by the emulator", which is
the same playout boundary Twilio reports, but the measurements describe this host only.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import secrets
import time
import uuid
from collections import deque
from xml.etree import ElementTree as ET

import httpx
import numpy as np
import websockets
from dotenv import dotenv_values

from duplex_voice.audio import float_to_pcm16, mulaw_decode, mulaw_encode, pcm16_to_float
from duplex_voice.security import twilio_signature

log = logging.getLogger("twilio_emulator")
FRAME = 160  # 20 ms at 8 kHz


class Playout:
    """Twilio-like outbound buffer: audio and marks in arrival order, drained in real time."""

    def __init__(self):
        self.items: deque = deque()  # np.ndarray samples or str mark names

    def media(self, samples):
        self.items.append(samples)

    def mark(self, name):
        self.items.append(name)

    def clear(self):
        marks = [x for x in self.items if isinstance(x, str)]
        self.items.clear()
        return marks

    def take(self):
        """Marks whose preceding audio already went out, then up to one 20 ms frame."""
        marks = []
        while self.items and isinstance(self.items[0], str):
            marks.append(self.items.popleft())
        out, need = [], FRAME
        while need and self.items and not isinstance(self.items[0], str):
            head = self.items[0]
            out.append(head[:need])
            if len(head) > need:
                self.items[0] = head[need:]
            else:
                self.items.popleft()
            need -= len(out[-1])
        if not out:
            return None, marks
        frame = np.concatenate(out)
        return np.pad(frame, (0, FRAME - len(frame))), marks


async def handle(reader, writer, cfg, client):
    stats = {"media_in": 0, "media_out": 0, "marks_echoed": 0, "clears": 0, "dtmf": 0}
    started = time.monotonic()
    try:
        header = await reader.readexactly(3)
        body = await reader.readexactly(int.from_bytes(header[1:], "big"))
        if header[0] != 0x01 or len(body) != 16:
            raise ValueError("AudioSocket must start with the call UUID")
        call_sid = "CA" + uuid.UUID(bytes=body).hex
        stream_sid = "MZ" + secrets.token_hex(16)
        form = [("AccountSid", cfg["account"]), ("ApiVersion", "2010-04-01"), ("CallSid", call_sid),
                ("CallStatus", "ringing"), ("Direction", "inbound"), ("From", cfg["caller"]), ("To", cfg["called"])]
        signature = twilio_signature(cfg["public"] + "/twilio/voice", form, cfg["token"])
        r = await client.post(cfg["gateway"] + "/twilio/voice", data=dict(form),
                              headers={"X-Twilio-Signature": signature})
        r.raise_for_status()
        stream = ET.fromstring(r.text).find("Connect/Stream")
        if stream is None:
            stats["declined"] = True  # the gateway answered with <Say> only (busy/unavailable)
            return
        url = stream.attrib["url"]
        params = {p.attrib["name"]: p.attrib["value"] for p in stream.findall("Parameter")}
        query = url.partition("?")[2]
        signed = cfg["public"] + "/twilio/stream" + ("?" + query if query else "")
        async with websockets.connect(url, additional_headers={
                "X-Twilio-Signature": twilio_signature(signed, [], cfg["token"])}, max_size=65536) as ws:
            async def event(name, **fields):
                await ws.send(json.dumps({"event": name, "streamSid": stream_sid, **fields}))

            await ws.send(json.dumps({"event": "connected", "protocol": "Call", "version": "1.0.0"}))
            await event("start", sequenceNumber="1", start={
                "accountSid": cfg["account"], "streamSid": stream_sid, "callSid": call_sid,
                "tracks": ["inbound"], "customParameters": params,
                "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": 8000, "channels": 1}})
            playout, seq = Playout(), 1

            async def uplink():
                nonlocal seq
                chunk = 0
                while True:
                    try:
                        h = await reader.readexactly(3)
                        data = await reader.readexactly(int.from_bytes(h[1:], "big"))
                    except (asyncio.IncompleteReadError, ConnectionError):
                        break
                    if h[0] == 0x10 and data:
                        seq, chunk = seq + 1, chunk + 1
                        stats["media_in"] += 1
                        await event("media", sequenceNumber=str(seq), media={
                            "track": "inbound", "chunk": str(chunk),
                            "timestamp": str(int((time.monotonic() - started) * 1000)),
                            "payload": base64.b64encode(mulaw_encode(pcm16_to_float(data))).decode()})
                    elif h[0] == 0x03 and len(data) == 1:
                        seq += 1
                        stats["dtmf"] += 1
                        await event("dtmf", sequenceNumber=str(seq),
                                    dtmf={"track": "inbound_track", "digit": data.decode()})
                    elif h[0] in (0x00, 0xFF):
                        break
                seq += 1
                await event("stop", sequenceNumber=str(seq),
                            stop={"accountSid": cfg["account"], "callSid": call_sid})

            async def downlink():
                nonlocal seq
                async for raw in ws:
                    msg = json.loads(raw)
                    kind = msg.get("event")
                    if kind == "media":
                        playout.media(mulaw_decode(base64.b64decode(msg["media"]["payload"])))
                    elif kind == "mark":
                        playout.mark(msg["mark"]["name"])
                    elif kind == "clear":
                        stats["clears"] += 1
                        for name in playout.clear():
                            seq += 1
                            stats["marks_echoed"] += 1
                            await event("mark", sequenceNumber=str(seq), mark={"name": name})

            async def play():
                nonlocal seq
                deadline = time.monotonic()
                while True:
                    frame, marks = playout.take()
                    if frame is not None:
                        writer.write(b"\x10" + (FRAME * 2).to_bytes(2, "big") + float_to_pcm16(frame))
                        await writer.drain()
                        stats["media_out"] += 1
                    for name in marks:
                        seq += 1
                        stats["marks_echoed"] += 1
                        await event("mark", sequenceNumber=str(seq), mark={"name": name})
                    deadline = max(deadline + .02, time.monotonic() - .1)
                    await asyncio.sleep(max(0, deadline - time.monotonic()))

            tasks = [asyncio.create_task(x()) for x in (uplink, downlink, play)]
            try:
                # Either side ending ends the call: caller hangup or gateway closing the stream.
                await asyncio.wait(tasks[:2], return_when=asyncio.FIRST_COMPLETED)
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as exc:
        stats["error"] = type(exc).__name__
    finally:
        try:
            writer.write(b"\x00\x00\x00")  # hang up the Asterisk channel
            await writer.drain()
        except (ConnectionError, RuntimeError):
            pass
        writer.close()
        stats["seconds"] = round(time.monotonic() - started, 2)
        log.info(json.dumps({"type": "emulated_call", **stats}))


async def main(args):
    env = {**dotenv_values(".env"), **os.environ}
    cfg = {"gateway": args.gateway.rstrip("/"), "public": (args.public_url or args.gateway).rstrip("/"),
           "token": env.get("TWILIO_AUTH_TOKEN", ""), "account": env.get("TWILIO_ACCOUNT_SID", ""),
           "caller": args.caller_id, "called": args.called}
    if len(cfg["token"]) < 24 or not cfg["account"].startswith("AC"):
        raise SystemExit("set lab TWILIO_ACCOUNT_SID (AC...) and TWILIO_AUTH_TOKEN (24+) in .env")
    async with httpx.AsyncClient(timeout=5) as client:
        server = await asyncio.start_server(lambda r, w: handle(r, w, cfg, client), args.host, args.port)
        log.info(json.dumps({"type": "listening", "audiosocket": f"{args.host}:{args.port}"}))
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--gateway", default="http://127.0.0.1:8000", help="where requests are sent")
    p.add_argument("--public-url", help="the gateway's PUBLIC_BASE_URL, used for signatures")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9094, help="AudioSocket port Asterisk dials")
    p.add_argument("--caller-id", default="+15005550006", help="From; a reserved test-style value")
    p.add_argument("--called", default="+15005550001", help="To; a reserved test-style value")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    asyncio.run(main(p.parse_args()))
