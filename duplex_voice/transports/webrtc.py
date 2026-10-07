"""Optional aiortc transport. Media uses RTP/Opus; JSON travels on a data channel.

RTP send-time is NOT remote playout-time. This adapter deliberately labels its
playback ledger as estimated. Already transmitted RTP cannot be unsent by clear().
"""
from __future__ import annotations
import asyncio
import json
from collections import deque
from fractions import Fraction

import numpy as np
from aiortc import MediaStreamTrack
from aiortc.mediastreams import MediaStreamError
from av import AudioFrame, AudioResampler
from .base import Transport
from ..audio import float_to_pcm16


class OutgoingTrack(MediaStreamTrack):
    kind = "audio"

    def __init__(self, owner):
        super().__init__()
        self.owner = owner
        self.queue = asyncio.Queue(maxsize=4)
        self.pts = 0
        self.deadline = None

    async def recv(self):
        loop = asyncio.get_running_loop()
        if self.deadline is None:
            self.deadline = loop.time()
        await asyncio.sleep(max(0, self.deadline-loop.time()))
        self.deadline = max(self.deadline+.02, loop.time())
        samples = np.zeros(960, np.float32)
        self.owner.check_owner()
        while not self.queue.empty():
            candidate, epoch, seq = self.queue.get_nowait()
            if epoch == self.owner.ledger.epoch:
                samples = candidate
                self.owner.estimate_receipt(epoch, seq, self.owner.playout_delay)
                break
        frame = AudioFrame(format="s16", layout="mono", samples=960)
        frame.planes[0].update(float_to_pcm16(samples))
        frame.sample_rate = 48000
        frame.pts = self.pts
        frame.time_base = Fraction(1, 48000)
        self.pts += 960
        return frame


class WebRTCTransport(Transport):
    name = "webrtc"
    evidence = "rtp_dispatch_estimate"
    in_rate = 16000
    out_rate = 48000

    def __init__(self, pc, settings):
        super().__init__()
        self.pc = pc
        self.channel = None
        self.playout_delay = settings.rtc_estimated_playout_ms / 1000
        self.connected = asyncio.Event()
        self.incoming = asyncio.Queue(settings.ingress_frames * 2)
        self.events = deque(maxlen=32)
        self.receivers = set()
        self.done = False
        self.outgoing = OutgoingTrack(self)
        pc.addTrack(self.outgoing)

        @pc.on("track")
        def on_track(track):
            if track.kind != "audio":
                track.stop()
                return
            task = asyncio.create_task(self.receive_track(track))
            self.receivers.add(task)
            task.add_done_callback(self.receivers.discard)

        @pc.on("datachannel")
        def on_channel(channel):
            self.channel = channel
            @channel.on("message")
            def message(raw):
                try:
                    if isinstance(raw, str) and len(raw) < 2048 and json.loads(raw).get("type") == "hangup":
                        self.end_input()
                except ValueError:
                    self.end_input()
            @channel.on("open")
            def opened():
                self.flush_events()
            if channel.readyState == "open":
                self.flush_events()

        @pc.on("connectionstatechange")
        async def state():
            if pc.connectionState == "connected":
                self.connected.set()
            elif pc.connectionState in {"failed", "closed", "disconnected"}:
                self.end_input()

    def end_input(self):
        if self.done:
            return
        self.done = True
        while not self.incoming.empty():
            self.incoming.get_nowait()
        self.incoming.put_nowait(None)

    async def receive_track(self, track):
        rs = AudioResampler(format="s16", layout="mono", rate=16000)
        try:
            while not self.done:
                incoming = await track.recv()
                for f in rs.resample(incoming):
                    samples = f.to_ndarray().reshape(-1).astype(np.float32) / 32768.0
                    self.incoming.put_nowait(samples)
        except (MediaStreamError, asyncio.QueueFull):
            self.end_input()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.end_input()

    async def ready(self):
        await self.connected.wait()

    async def frames(self):
        while (samples := await self.incoming.get()) is not None:
            yield samples

    async def write(self, samples, epoch, seq):
        if len(samples) != 960:
            raise ValueError("WebRTC output must be 20 ms at 48 kHz")
        if epoch != self.ledger.epoch:
            raise asyncio.CancelledError
        await self.outgoing.queue.put((samples, epoch, seq))

    async def clear(self, epoch):
        while not self.outgoing.queue.empty():
            self.outgoing.queue.get_nowait()
        await self.event({"type": "clear", "epoch": epoch,
                          "residual_rtp": "already sent packets cannot be revoked"})

    def flush_events(self):
        if self.channel and self.channel.readyState == "open":
            while self.events:
                if self.channel.bufferedAmount > 65536:
                    self.events.clear()
                    return
                self.channel.send(self.events.popleft())

    async def event(self, data):
        self.events.append(json.dumps(data, separators=(",", ":")))
        self.flush_events()

    async def close(self):
        await super().close()
        self.end_input()
        for task in list(self.receivers):
            task.cancel()
        await asyncio.gather(*self.receivers, return_exceptions=True)
        self.outgoing.stop()
        await self.pc.close()
