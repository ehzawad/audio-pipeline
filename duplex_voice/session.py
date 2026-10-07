"""Conversation ownership lives here, never inside a provider callback.

Input keeps running while an output task tree is canceled. ASR utterances are
isolated, pending text merges before first dispatch, and every output has an epoch.
"""
from __future__ import annotations
import asyncio
from contextlib import suppress, aclosing
from dataclasses import dataclass
import numpy as np

from .audio import Framer, Resampler, Packetizer
from .concurrency import Overloaded
from .resilience import RateBudget
from .metrics import Trace
from .text import SentenceChunker, VOICE_SYSTEM_PROMPT, clean_for_tts
from .turns import Start, End, TurnDetector


@dataclass
class Utterance:
    frames: asyncio.Queue
    final: asyncio.Future
    task: asyncio.Task | None = None


@dataclass
class AudioPacket:
    samples: np.ndarray | None
    text: str | None = None  # phrase-complete marker; follows all of that phrase's audio


class VoiceSession:
    def __init__(self, transport, providers, settings, metrics, *, history=None, history_revision=0, on_history=None):
        self.t, self.p, self.s, self.metrics = transport, providers, settings, metrics
        self.ledger = transport.ledger
        self.history: list[dict] = list(history or [])
        self.history_revision = history_revision
        self.on_history = on_history
        self.pending: list[Utterance] = []
        self.current: Utterance | None = None
        self.response: asyncio.Task | None = None
        self.tasks: set[asyncio.Task] = set()
        self.user_revision = 0
        self.vad = providers.make_vad()
        self.detector = TurnDetector(threshold=settings.vad_threshold,
            start_ms=settings.vad_start_ms,
            end_ms=settings.vad_end_ms_phone if transport.phone else settings.vad_end_ms_web,
            barge_ms=settings.barge_in_start_ms, preroll_ms=settings.preroll_ms,
            max_seconds=settings.max_utterance_seconds)
        self.closed = False
        self.recorded_epoch = -1
        self.trim_history()

    def spawn(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        def done(t):
            self.tasks.discard(t)
            if not t.cancelled():
                exc = t.exception()
                if exc:
                    self.metrics.count("background.failure")
        task.add_done_callback(done)
        return task

    async def run(self):
        inbox = asyncio.Queue(maxsize=self.s.ingress_frames)
        try:
            async with asyncio.timeout(self.s.max_call_seconds):
                await asyncio.wait_for(self.t.ready(), self.s.rtc_connect_timeout if self.t.name == "webrtc" else self.s.io_timeout)
                await self.t.event({"type": "ready", "mode": self.s.voice_mode,
                    "transport": self.t.name, "playback_evidence": self.t.evidence})
                if self.s.greeting:
                    self.response = self.spawn(self.speak(self._one(self.s.greeting), None, Trace()))

                async def receive():
                    budget = RateBudget(rate=1.1, burst=self.s.ingress_burst_seconds)
                    rs = Resampler(self.t.in_rate, 16000)
                    framer = Framer(512)
                    async for chunk in self.t.frames():
                        budget.consume(len(chunk)/self.t.in_rate)
                        for frame in framer.push(rs.process(chunk)):
                            try:
                                inbox.put_nowait(frame)
                            except asyncio.QueueFull:
                                raise Overloaded("ingress is late; refusing to accumulate stale speech")
                    # Hang-up should terminate, not synthesize a partial last utterance.
                    await inbox.put(None)

                async def consume():
                    while (frame := await inbox.get()) is not None:
                        await self.on_frame(frame)

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(receive())
                    tg.create_task(consume())
        finally:
            self.closed = True
            with suppress(Exception):
                await self.interrupt()
            for task in list(self.tasks):
                task.cancel()
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
            await self.t.close()

    async def on_frame(self, frame):
        ev = self.detector.push(frame, self.vad(frame), self.response is not None and not self.response.done())
        if isinstance(ev, Start):
            self.user_revision += 1
            await self.interrupt()
            if len(self.pending) >= self.s.max_pending_utterances:
                raise Overloaded("too many uncommitted utterance fragments")
            u = Utterance(asyncio.Queue(self.s.asr_queue_frames),
                          asyncio.get_running_loop().create_future())
            # The final may fail while a newer turn is in progress: retrieve its exception.
            u.final.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
            self.current = u
            self.pending.append(u)
            u.task = self.spawn(self.recognize(u))
            for f in ev.preroll:
                self.feed_asr(u, f)
            await self.t.event({"type": "speech_start"})
        elif self.current is not None:
            self.feed_asr(self.current, frame)
            if isinstance(ev, End):
                u, self.current = self.current, None
                self.feed_asr(u, None)
                self.response = self.spawn(self.respond(self.user_revision, list(self.pending),
                                                        ev.silence_ms / 1000))

    def feed_asr(self, u, frame):
        try:
            u.frames.put_nowait(frame)
        except asyncio.QueueFull:
            raise Overloaded("ASR input queue exceeded its audio-time budget")

    async def recognize(self, u):
        async def frames():
            while (f := await u.frames.get()) is not None:
                yield f
        partial = ""
        def on_partial(text):
            nonlocal partial
            partial = text  # overwritten, not appended to a queue; revisions are not facts
        async def publish():
            old = ""
            while True:
                await asyncio.sleep(.15)
                if partial != old and self.current is u and not self.closed:
                    old = partial
                    await self.t.event({"type": "partial", "text": partial})
        publisher = asyncio.create_task(publish())
        try:
            text = await self.p.asr.recognize(frames(), on_partial)
            if not u.final.done():
                u.final.set_result(text.strip())
        except asyncio.CancelledError:
            if not u.final.done():
                u.final.cancel()
            raise
        except Exception as exc:
            if not u.final.done():
                u.final.set_exception(exc)
        finally:
            publisher.cancel()
            await asyncio.gather(publisher, return_exceptions=True)

    async def interrupt(self):
        task, self.response = self.response, None
        if task is not None and not task.done():
            # Snapshot BEFORE begin() discards the old ledger. clear-induced Twilio marks
            # and late browser ACKs then carry an invalid epoch and cannot change history.
            self.commit_assistant(interrupted=True)
            new_epoch = self.ledger.begin()
            task.cancel()
            await asyncio.wait_for(self.t.clear(new_epoch), self.s.io_timeout)
            self.metrics.count("interruption")
            # Do not join model cleanup on the input path. The task remains tracked and
            # will be joined at session shutdown; every writer checks the epoch.

    def commit_assistant(self, interrupted=False):
        if self.recorded_epoch == self.ledger.epoch:
            return
        self.recorded_epoch = self.ledger.epoch
        text = self.ledger.completed_text()
        if text:
            self.history.append({"role": "assistant",
                                 "content": text + (" [interrupted]" if interrupted else "")})
        elif interrupted and self.ledger.sent:
            self.history.append({"role": "assistant", "content": "[interrupted; no complete phrase confirmed]"})
        self.trim_history()
        self.history_changed()

    def history_changed(self):
        self.history_revision += 1
        if self.on_history:
            self.on_history([dict(m) for m in self.history], self.history_revision)

    def trim_history(self):
        while len(self.history) > self.s.max_history_messages or sum(len(m["content"]) for m in self.history) > self.s.max_history_chars:
            self.history.pop(0)
        while self.history and self.history[0]["role"] == "assistant":
            self.history.pop(0)

    async def respond(self, revision, utterances, endpoint_s):
        trace = Trace(endpoint_s)
        try:
            async with asyncio.timeout(self.s.asr_timeout):
                # Canceling this response must NOT cancel the shared ASR futures; they
                # may still belong to the next merged user turn.
                texts = await asyncio.gather(*(asyncio.shield(u.final) for u in utterances))
            if revision != self.user_revision or self.current is not None:
                return
            text = " ".join(x for x in texts if x).strip()
            trace.mark("asr_final_s")
            if not text:
                self.pending = [u for u in self.pending if u not in utterances]
                self.metrics.count("asr.empty")
                return
            text = text[:8192]
            await self.t.event({"type": "user", "text": text, "committed": False})
            messages = list(self.history) + [{"role": "user", "content": text}]
            await self.speak(self.p.llm.stream(messages, VOICE_SYSTEM_PROMPT),
                             (text, utterances), trace)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Failure is explicit. No replay of a partially spoken model reply.
            for u in utterances:
                if u.task and not u.task.done():
                    u.task.cancel()
            self.pending = [u for u in self.pending if u not in utterances]
            self.metrics.count("turn.failure")
            await self.t.event({"type": "error", "message": "A speech or model service failed. Please try again."})
            if not self.closed and revision == self.user_revision and self.current is None:
                # This prompt does not depend on the failed LLM/TTS provider. Once only;
                # it remains interruptible inside the existing response task tree.
                from .providers.recorded import ServiceUnavailable
                fallback = ServiceUnavailable()
                with suppress(Exception):
                    await self.speak(self._one(fallback.text), None, Trace(), tts=fallback)

    async def speak(self, tokens, user, trace, tts=None):
        tts = tts or self.p.tts
        epoch = self.ledger.begin()
        phrases = asyncio.Queue(maxsize=self.s.phrase_queue_size)
        packets = asyncio.Queue(maxsize=self.s.output_queue_frames)
        committed = False
        status = "failed"

        async def produce_text():
            chunker, count = SentenceChunker(), 0
            async for token in tokens:
                trace.mark("llm_first_token_s")
                count += len(token)
                if count > self.s.max_reply_chars:
                    raise ValueError("model reply exceeded character budget")
                for text in chunker.push(token):
                    if cleaned := clean_for_tts(text):
                        trace.mark("first_phrase_s")
                        await phrases.put(cleaned)
            for text in chunker.flush():
                if cleaned := clean_for_tts(text):
                    trace.mark("first_phrase_s")
                    await phrases.put(cleaned)
            await phrases.put(None)  # only on normal completion, never in finally

        async def synthesize():
            while (text := await phrases.get()) is not None:
                rs = Resampler(tts.sample_rate, self.t.out_rate)
                slicer = Packetizer(self.t.out_rate)
                total = 0
                async with aclosing(tts.synthesize(text)) as stream:
                    async for samples in stream:
                        if not np.isfinite(samples).all() or samples.ndim != 1:
                            raise ValueError("TTS returned invalid samples")
                        total += len(samples)
                        if total > tts.sample_rate * 30:
                            raise ValueError("TTS exceeded per-phrase audio budget")
                        for frame in slicer.push(rs.process(samples)):
                            await packets.put(AudioPacket(frame))
                if not total:
                    raise ValueError("TTS returned no audio")
                for frame in slicer.push(rs.flush()) + slicer.finish():
                    await packets.put(AudioPacket(frame))
                await packets.put(AudioPacket(None, text))
            await packets.put(None)

        async def render():
            nonlocal committed
            loop = asyncio.get_running_loop()
            deadline = loop.time()
            while (packet := await packets.get()) is not None:
                if epoch != self.ledger.epoch:
                    raise asyncio.CancelledError
                if packet.samples is None:
                    self.ledger.phrase_sent(epoch, packet.text, self.ledger.sent)
                    await self.t.mark(epoch, self.ledger.sent)
                    await self.t.event({"type": "assistant_generated", "text": packet.text})
                    continue
                await asyncio.sleep(max(0, deadline - loop.time()))
                if epoch != self.ledger.epoch:
                    raise asyncio.CancelledError
                await self.ledger.credit(epoch, self.s.playback_window_frames, self.s.playback_timeout)
                seq = self.ledger.issue(epoch)
                await asyncio.wait_for(self.t.write(packet.samples, epoch, seq), self.s.io_timeout)
                if epoch != self.ledger.epoch:
                    raise asyncio.CancelledError
                if not committed:
                    committed = True
                    trace.mark("first_media_dispatched_s")
                    if user is not None:
                        text, utterances = user
                        self.history.append({"role": "user", "content": text})
                        self.pending = [u for u in self.pending if u not in utterances]
                        self.trim_history()
                        self.history_changed()
                        await self.t.event({"type": "user", "text": text, "committed": True})
                if seq % 5 == 0:
                    await self.t.mark(epoch, seq)
                deadline = max(deadline + .02, loop.time())  # no burst catch-up after a stall

        try:
            async with asyncio.timeout(self.s.response_timeout):
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(produce_text())
                    tg.create_task(synthesize())
                    tg.create_task(render())
                await self.ledger.drain(epoch, self.s.playback_timeout)
            if epoch == self.ledger.epoch:
                self.commit_assistant()
                status = "completed"
        except asyncio.CancelledError:
            status = "cancelled"
            raise
        except Exception:
            if epoch == self.ledger.epoch:
                self.commit_assistant(interrupted=True)
                new_epoch = self.ledger.begin()
                await self.t.clear(new_epoch)
            raise
        finally:
            # Closing an async generator is explicit; do not leave provider streams
            # alive waiting for garbage collection after canceling a consumer.
            if hasattr(tokens, "aclose"):
                with suppress(Exception):
                    await tokens.aclose()
            report = trace.report(self.metrics, self.t.name, status)
            if not self.closed:
                with suppress(Exception):
                    await self.t.event(report)

    @staticmethod
    async def _one(text):
        yield text
