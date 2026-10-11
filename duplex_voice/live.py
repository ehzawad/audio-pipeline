"""GPT-Live speech-to-speech: one full-duplex OpenAI Live session per call.

The live model owns turn-taking, interruption and wording. The gateway still owns the call:
admission, lease fencing, transport writes, real-time pacing, playback credit and cleanup.
GPT-Live streams its output as continuous real-time audio (silence included) in short
chunks, so a barge-in stops new speech at the source and at most one playback window of
already-received audio remains here. There is nothing stale to clear, so no clear is sent.

Transcripts are the model's own timed input and output transcripts. Output text is speech
the model generated; whether it was heard is still the transport's evidence.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
from contextlib import suppress

import websockets

from .audio import Packetizer, Resampler, float_to_pcm16, pcm16_to_float
from .metrics import Trace
from .resilience import RateBudget
from .text import VOICE_SYSTEM_PROMPT

log = logging.getLogger("duplex.session")
RATE = 24000  # audio/pcm at 24 kHz in both directions


class LiveSession:
    """Same contract as VoiceSession for OwnedCall: run(), history, history_revision."""

    def __init__(self, transport, providers, settings, metrics, *, history=None, history_revision=0, on_history=None):
        self.t, self.s, self.metrics = transport, settings, metrics
        self.ledger = transport.ledger
        self.history: list[dict] = list(history or [])
        self.history_revision = history_revision
        self.on_history = on_history
        self.closed = False
        # One open turn per speaker: [text, first start_ms, last arrival]. Deltas from the two
        # sides interleave (the caller's transcript lags its audio), so turns are cut by quiet
        # time, not by alternation, and emitted in order of when each speaker started.
        self.turns = {"user": ["", 0, 0.0], "assistant": ["", 0, 0.0]}

    def session_config(self):
        # A paraphrased greeting must still disclose that the caller is talking to an AI.
        instructions = VOICE_SYSTEM_PROMPT + "- Whenever you greet the caller, say that you are an AI voice assistant.\n"
        config = {"model": self.s.live_model, "instructions": instructions,
                  "audio": {"format": {"type": "audio/pcm", "rate": RATE},
                            "output": {"voice": self.s.live_voice}}}
        if self.s.live_delegate_model:
            config["delegation"] = {"type": "responses", "responses": {"model": self.s.live_delegate_model}}
        return config

    async def run(self):
        trace = Trace()
        try:
            async with asyncio.timeout(self.s.max_call_seconds):
                await asyncio.wait_for(self.t.ready(), self.s.rtc_connect_timeout if self.t.name == "webrtc" else self.s.io_timeout)
                await self.t.event({"type": "ready", "mode": self.s.voice_mode,
                                    "transport": self.t.name, "playback_evidence": self.t.evidence})
                await self.converse(trace)
        finally:
            self.closed = True
            self.flush_turns()
            await self.t.close()

    async def converse(self, trace):
        started, spoke = asyncio.Event(), asyncio.Event()
        packets = asyncio.Queue(maxsize=self.s.output_queue_frames)
        epoch = self.ledger.begin()
        ws = None

        async def uplink():
            # Read from the first frame while the live session is still connecting, so the
            # transport never queues a burst; forward nothing until the session exists.
            budget = RateBudget(rate=1.1, burst=self.s.ingress_burst_seconds)
            rs = Resampler(self.t.in_rate, RATE)
            async for chunk in self.t.frames():
                budget.consume(len(chunk) / self.t.in_rate)
                out = rs.process(chunk)
                if started.is_set() and out.size:
                    await ws.send(json.dumps({"type": "session.input_audio.append",
                                              "audio": base64.b64encode(float_to_pcm16(out)).decode()}))
            # The caller hung up: close the live session gracefully, bounded by io_timeout.
            if ws is not None:
                with suppress(Exception):
                    await ws.send(json.dumps({"type": "session.close"}))

        async def downlink():
            nonlocal ws
            try:
                ws = await websockets.connect(self.s.live_url,
                    additional_headers={"Authorization": "Bearer " + self.s.openai_api_key},
                    open_timeout=self.s.io_timeout, close_timeout=self.s.io_timeout,
                    max_size=1 << 20, max_queue=64)
            except Exception:
                await self.unavailable()
                raise
            async with ws:
                await ws.send(json.dumps({"type": "session.start", "session": self.session_config()}))
                await receive(ws)

        async def receive(ws):
            rs, slicer = Resampler(RATE, self.t.out_rate), Packetizer(self.t.out_rate)
            async for raw in ws:
                event = json.loads(raw)
                kind = event.get("type")
                if kind == "session.started":
                    started.set()
                    trace.mark("live_started_s")
                elif kind == "session.output_audio.delta":
                    samples = pcm16_to_float(base64.b64decode(event["delta"]))
                    for frame in slicer.push(rs.process(samples)):
                        await packets.put(frame)
                elif kind == "session.input_transcript.delta":
                    await self.transcript("user", event)
                elif kind == "session.output_transcript.delta":
                    spoke.set()
                    trace.mark("first_output_text_s")
                    await self.transcript("assistant", event)
                elif kind == "session.usage.updated":
                    pass
                elif kind == "session.closed":
                    usage = event.get("usage", {}).get("seconds")
                    if isinstance(usage, (int, float)):
                        self.metrics.observe("live.billed_seconds", usage)
                    self.metrics.count(f"live.closed.{event.get('reason', 'unknown')}"
                                       if event.get("reason") in {"close_requested", "expired", "content",
                                                                  "remote_hangup", "connection_lost"} else "live.closed.other")
                    return
                elif kind == "error":
                    # Errors refer to one client command; the session continues unless closed.
                    self.metrics.count("live.error_event")
            raise ConnectionError("live session ended without session.closed")

        async def render():
            loop = asyncio.get_running_loop()
            deadline = loop.time()
            while True:
                frame = await packets.get()
                deadline = max(deadline, loop.time())  # pace from arrival; no catch-up after a gap
                await asyncio.sleep(max(0, deadline - loop.time()))
                await self.ledger.credit(epoch, self.s.playback_window_frames, self.s.playback_timeout)
                seq = self.ledger.issue(epoch)
                await asyncio.wait_for(self.t.write(frame, epoch, seq), self.s.io_timeout)
                if seq == 1:
                    trace.mark("first_media_dispatched_s")
                if seq % 5 == 0:
                    await self.t.mark(epoch, seq)
                deadline += .02

        async def greeter():
            # GPT-Live does not reliably speak first. Measured on one day: a greeting in the start
            # instructions was often skipped; session.instructions.append (the documented route)
            # was answered after 3.7-12.7 s or not at all; session.commentary.append was spoken
            # 6 of 6 times within 2.3-2.6 s, sometimes paraphrased. Commentary goes first and
            # the instruction is the fallback.
            await started.wait()
            if not self.s.greeting:
                return
            for attempt, kind, content in (
                    (0, "session.commentary.append", self.s.greeting),
                    (1, "session.instructions.append", "Speak first now, in English. Greet the caller once "
                     f"by saying exactly: \"{self.s.greeting}\" Then stop and listen.")):
                await ws.send(json.dumps({"type": kind, "event_id": f"greeting-{attempt}",
                                          "delegation_id": None, "content": content}))
                try:
                    await asyncio.wait_for(spoke.wait(), self.s.live_greeting_retry_seconds)
                    return
                except TimeoutError:
                    self.metrics.count("live.greeting_retry" if attempt == 0 else "live.greeting_unanswered")

        async def settle_turns():
            # The live API has no end-of-turn event. An assistant turn whose transcript has
            # been quiet for a moment is finished for the client and the history.
            loop = asyncio.get_running_loop()
            while True:
                await asyncio.sleep(.25)
                text, _, last = self.turns["assistant"]
                if text and loop.time() - last > 1.2:
                    for event in self.flush_turns():
                        await self.t.event(event)

        async def start_timeout():
            try:
                await asyncio.wait_for(started.wait(), self.s.response_timeout)
            except TimeoutError:
                raise ConnectionError("live session did not start") from None

        tasks = [asyncio.create_task(x) for x in (uplink(), downlink(), render(), start_timeout(), greeter(), settle_turns())]
        up, down = tasks[:2]
        try:
            # The call ends when the live session closes or any part fails. A caller hangup
            # ends uplink after it sends session.close; session.closed then ends downlink.
            # Once the session itself has ended, only its own outcome counts: the uplink
            # may fail on the socket the server just closed.
            pending = set(tasks)
            while down in pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                if down in done:
                    down.result()
                    break
                for task in done:
                    task.result()  # raises the first failure
                if up in done:
                    await asyncio.wait({down}, timeout=self.s.io_timeout)
                    break
        except Exception as exc:
            log.warning(json.dumps({"type": "live_failure", "transport": self.t.name,
                                    "errors": [type(exc).__name__]}, separators=(",", ":")))
            self.metrics.count("live.failure")
            raise
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            report = trace.report(self.metrics, self.t.name, "live")
            with suppress(Exception):
                await self.t.event(report)

    async def transcript(self, role, event):
        turn = self.turns[role]
        if not turn[0]:
            turn[1] = event.get("start_ms") if isinstance(event.get("start_ms"), int) else 0
        turn[0] = (turn[0] + str(event.get("delta", "")))[:8192]
        turn[2] = asyncio.get_running_loop().time()
        if role == "user":
            await self.t.event({"type": "partial", "text": turn[0].strip()})

    def flush_turns(self):
        """Close both open turns in speaking order; record history; return client events."""
        events = []
        for role in sorted(self.turns, key=lambda r: self.turns[r][1]):
            text = self.turns[role][0].strip()
            self.turns[role] = ["", 0, 0.0]
            if not text:
                continue
            self.history.append({"role": role, "content": text})
            events.append({"type": "user", "text": text, "committed": True} if role == "user"
                          else {"type": "assistant_generated", "text": text})
        if events:
            while len(self.history) > self.s.max_history_messages or sum(len(m["content"]) for m in self.history) > self.s.max_history_chars:
                self.history.pop(0)
            self.history_revision += 1
            if self.on_history:
                self.on_history([dict(m) for m in self.history], self.history_revision)
        return events

    async def unavailable(self):
        """The live service is the only speaker in this mode; say so with the bundled prompt."""
        from .providers.recorded import ServiceUnavailable
        epoch = self.ledger.begin()
        rs = Resampler(ServiceUnavailable.sample_rate, self.t.out_rate)
        slicer = Packetizer(self.t.out_rate)
        loop = asyncio.get_running_loop()
        deadline = loop.time()
        async def play(frames):
            nonlocal deadline
            for frame in frames:
                await asyncio.sleep(max(0, deadline - loop.time()))
                await self.t.write(frame, epoch, self.ledger.issue(epoch))
                deadline = max(deadline + .02, loop.time())

        with suppress(Exception):
            async for samples in ServiceUnavailable().synthesize(""):
                await play(slicer.push(rs.process(samples)))
            await play(slicer.push(rs.flush()) + slicer.finish())
            await asyncio.sleep(.5)

