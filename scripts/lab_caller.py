"""Scripted, reactive synthetic caller for the telephony labs (scripts/pbx_lab.py).

A lab supplies a Line (receive and send 8 kHz mono float frames, hang up). The caller waits
for the greeting, speaks each scenario turn after the assistant goes quiet or barges in at a
fixed offset, and records the arrival time of everything it hears. Timing is measured at the
caller's side of the telephone path; "heard" never means a handset loudspeaker.
"""
from __future__ import annotations

import asyncio
import io
import json
import time
import uuid
import wave
from collections import deque
from pathlib import Path
from typing import Protocol

import numpy as np

from duplex_voice.audio import Resampler, float_to_pcm16, pcm16_to_float

RATE = 8000
FRAME = 160  # 20 ms at 8 kHz
VOICED_RMS = 0.01  # about -40 dBFS
DEFAULT_SCENARIO = [
    {"say": "Hi. Can you tell me a little about the history of Paris?"},  # interrupted below
    {"say": "Sorry to cut in. Just tell me the capital of Japan.", "barge_after": 1.5, "expect": "Tokyo"},
    {"say": "Great, thanks. That is all for now. Goodbye."},
]


class Line(Protocol):
    async def recv(self) -> np.ndarray | None: ...  # None once the far end hangs up
    async def send(self, frame: np.ndarray) -> None: ...  # exactly FRAME samples
    async def hangup(self) -> None: ...


class Ear:
    """Arrival-time record of what the caller hears; gaps in delivery count as silence."""

    def __init__(self, t0):
        self.t0 = t0
        self.frames: list[tuple[float, np.ndarray]] = []
        self.last_voiced = None
        self.changed = asyncio.Event()

    def push(self, samples):
        now = time.monotonic() - self.t0
        self.frames.append((now, samples))
        if np.sqrt(np.mean(samples * samples)) > VOICED_RMS:
            self.last_voiced = now
        self.changed.set()

    def voiced_times(self, start, end=float("inf")):
        return [t for t, x in self.frames if start <= t < end and np.sqrt(np.mean(x * x)) > VOICED_RMS]

    async def onset_after(self, start, timeout):
        """First voiced arrival after `start` (seconds since answer)."""
        async with asyncio.timeout(timeout):
            while True:
                hits = self.voiced_times(start)
                if hits:
                    return hits[0]
                self.changed.clear()
                await self.changed.wait()

    async def quiet_for(self, seconds, timeout, not_before=0.):
        """Wait until nothing voiced has arrived for `seconds`; return the last voiced time."""
        async with asyncio.timeout(timeout):
            while True:
                now = time.monotonic() - self.t0
                if now >= not_before and (self.last_voiced is None or now - self.last_voiced >= seconds):
                    return self.last_voiced
                await asyncio.sleep(.02)

    def audio(self, start, end):
        out = np.zeros(max(0, int((end - start) * RATE)), np.float32)
        for t, x in self.frames:
            i = int((t - start) * RATE)
            if 0 <= i < len(out):
                out[i:i + len(x)] = x[:len(out) - i]
        return out


def gaps(times, minimum=.25):
    return [round(b - a, 3) for a, b in zip(times, times[1:]) if b - a >= minimum]


def wav_bytes(samples, rate=RATE):
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(float_to_pcm16(samples))
    return buf.getvalue()


async def synthesize(client, env, text, cache: Path):
    """Caller speech uses a different voice from the assistant so the two are distinguishable."""
    key = uuid.uuid5(uuid.NAMESPACE_URL, env["CALLER_VOICE"] + "\0" + text).hex
    path = cache / f"{key}.pcm"
    if not path.exists():
        r = await client.post(env["OPENAI_BASE_URL"].rstrip("/") + "/audio/speech",
            headers={"Authorization": "Bearer " + env["OPENAI_API_KEY"]},
            json={"model": "gpt-4o-mini-tts", "voice": env["CALLER_VOICE"], "input": text,
                  "response_format": "pcm", "instructions": "A caller speaking on a phone."})
        r.raise_for_status()
        path.write_bytes(r.content)
    rs = Resampler(24000, RATE)
    x = pcm16_to_float(path.read_bytes())
    return np.concatenate([rs.process(x), rs.flush()])


async def utterance(client, env, spec, cache: Path):
    """A turn's caller audio: a recorded WAV ("audio") or OpenAI TTS of "say"."""
    if "audio" not in spec:
        return await synthesize(client, env, spec["say"], cache)
    with wave.open(spec["audio"], "rb") as w:
        if w.getsampwidth() != 2 or w.getnchannels() != 1:
            raise ValueError("caller WAVs must be mono 16-bit PCM")
        rate, x = w.getframerate(), pcm16_to_float(w.readframes(w.getnframes()))
    rs = Resampler(rate, RATE)
    return np.concatenate([rs.process(x), rs.flush()])


async def transcribe(client, env, samples):
    if len(samples) < RATE // 2:
        return ""
    r = await client.post(env["OPENAI_BASE_URL"].rstrip("/") + "/audio/transcriptions",
        headers={"Authorization": "Bearer " + env["OPENAI_API_KEY"]},
        data={"model": "gpt-4o-transcribe", "language": "en"},
        files={"file": ("heard.wav", wav_bytes(samples), "audio/wav")})
    r.raise_for_status()
    return r.json()["text"].strip()


async def converse(line: Line, scenario, utterances, args):
    """Run the scenario on an answered line. Returns (greeting, turns, ear, said, seconds)."""
    t0 = time.monotonic()
    ear, mouth, said = Ear(t0), deque(), []

    async def listen():
        while (frame := await line.recv()) is not None:
            ear.push(frame)

    async def speak():
        deadline = time.monotonic()
        while True:
            frame = mouth.popleft() if mouth else np.zeros(FRAME, np.float32)
            said.append(frame)
            await line.send(frame)
            deadline += .02
            await asyncio.sleep(max(0, deadline - time.monotonic()))

    async def say(samples):
        """Queue an utterance; return (start, end) in seconds since answer."""
        start = time.monotonic() - t0 + len(mouth) * .02
        for i in range(0, len(samples), FRAME):
            chunk = samples[i:i + FRAME]
            mouth.append(np.pad(chunk, (0, FRAME - len(chunk))))
        end = start + len(samples) / RATE
        await asyncio.sleep(max(0, end - (time.monotonic() - t0)))
        return start, end

    turns = []
    rx, tx = asyncio.create_task(listen()), asyncio.create_task(speak())
    try:
        try:
            greeting_onset = await ear.onset_after(0, args.greeting_timeout)
            greeting_end = await ear.quiet_for(args.hold, args.reply_timeout, not_before=args.settle)
            greeting = {"onset_s": round(greeting_onset, 3), "end_s": round(greeting_end, 3)}
        except TimeoutError:
            greeting = None  # like a person after a silent pickup: start talking anyway
        for i, (spec, samples) in enumerate(zip(scenario, utterances)):
            turn = {"say": spec["say"]}
            if "barge_after" in spec:
                if not turns or "reply_onset_s" not in turns[-1]:
                    raise ValueError("barge_after needs a preceding answered turn")
                await asyncio.sleep(max(0, turns[-1]["reply_onset_s"] + spec["barge_after"] - (time.monotonic() - t0)))
                start, end = await say(samples)
                # No new reply can begin before this utterance ends, so anything voiced
                # meanwhile is the interrupted reply still arriving.
                old = ear.voiced_times(start, end)
                turn["barge_start_s"] = round(start, 3)
                turn["assistant_stop_after_barge_s"] = round(max(0, old[-1] + .02 - start) if old else 0, 3)
            else:
                await ear.quiet_for(args.hold, args.reply_timeout)
                start, end = await say(samples)
                # Assistant audio heard while the caller is still talking: an endpoint taken
                # inside a human pause, or a full-duplex model choosing to overlap.
                turn["assistant_voiced_during_caller_s"] = round(len(ear.voiced_times(start, end)) * .02, 2)
            turn["said_end_s"] = round(end, 3)
            try:
                onset = await ear.onset_after(end, args.reply_timeout)
            except TimeoutError:
                turns.append(turn)
                break
            turn["reply_onset_s"] = round(onset, 3)
            turn["said_end_to_heard_reply_s"] = round(onset - end, 3)
            if i + 1 < len(scenario) and "barge_after" in scenario[i + 1]:
                turns.append(turn)  # interrupted on purpose by the next turn
                continue
            last = await ear.quiet_for(args.hold, args.reply_timeout)
            turn["reply_end_s"] = round(last, 3)
            turn["reply_gaps_s"] = gaps(ear.voiced_times(onset, last + .01))
            turns.append(turn)
        await line.hangup()
    finally:
        tx.cancel()
        await asyncio.gather(tx, return_exceptions=True)
        try:
            async with asyncio.timeout(3):
                await rx
        except Exception:
            rx.cancel()
            await asyncio.gather(rx, return_exceptions=True)
    duration = time.monotonic() - t0
    return greeting, turns, ear, np.concatenate(said) if said else np.zeros(0, np.float32), duration


async def analyze(client, env, scenario, turns, ear, duration, greeting=True):
    """Independently transcribe each heard reply and check the scenario's expectations."""
    failures = []
    for i, turn in enumerate(turns):
        if "reply_onset_s" not in turn:
            continue
        stop = turn.get("reply_end_s")
        if stop is None and i + 1 < len(turns):
            nxt = turns[i + 1]
            stop = nxt["barge_start_s"] + nxt["assistant_stop_after_barge_s"]
        turn["reply_transcript"] = await transcribe(
            client, env, ear.audio(turn["reply_onset_s"] - .1, (stop or duration) + .3))
        expect = scenario[i].get("expect")
        if expect and expect.lower() not in turn["reply_transcript"].lower():
            failures.append(f"turn {i + 1}: expected '{expect}' in the heard reply")
    if len(turns) < len(scenario) or any("reply_onset_s" not in t for t in turns):
        failures.append("not every turn received a reply")
    if greeting is None:
        failures.append("no greeting was heard")
    return failures


def add_arguments(p):
    p.add_argument("--scenario", type=Path, help="JSON list of {say, expect?, barge_after?}")
    p.add_argument("--recordings", type=Path, help="where heard/said WAVs and report.json go")
    p.add_argument("--hold", type=float, default=2.5, help="silence that ends an assistant reply")
    p.add_argument("--settle", type=float, default=0,
                   help="never treat the greeting as finished before this many seconds")
    p.add_argument("--reply-timeout", type=float, default=30)
    p.add_argument("--greeting-timeout", type=float, default=10, help="then speak without a greeting")
    p.add_argument("--call-timeout", type=float, default=240)


def load_scenario(args):
    return json.loads(args.scenario.read_text()) if args.scenario else DEFAULT_SCENARIO


def save(out: Path, report, ear, said, duration):
    out.mkdir(parents=True, exist_ok=True)
    (out / "heard.wav").write_bytes(wav_bytes(ear.audio(0, duration)))
    (out / "said.wav").write_bytes(wav_bytes(said))
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
