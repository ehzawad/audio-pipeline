"""Source-aligned HTTP/WebSocket adapters; model names remain operator configuration.

No automatic retry after partial output. Every streaming context closes on cancellation.
Each ASR utterance owns its connection, so a late final cannot corrupt a later turn.
"""
from __future__ import annotations
import asyncio
import json
from urllib.parse import urlencode
import httpx
import numpy as np
import websockets

from ..audio import PCMDecoder, float_to_pcm16


def ws_url(http_url: str) -> str:
    if http_url.startswith("https://"):
        return "wss://" + http_url[8:]
    if http_url.startswith("http://"):
        return "ws://" + http_url[7:]
    raise ValueError("expected HTTP(S) speech service URL")


async def sse_objects(response):
    """SSE frames may span several data lines; HTTP chunks are not event boundaries."""
    data = []
    async for line in response.aiter_lines():
        if len(line) > 262144:
            raise ValueError("oversized SSE line")
        if not line:
            if data:
                body = "\n".join(data)
                data.clear()
                if body == "[DONE]":
                    return
                yield json.loads(body)
        elif line.startswith("data:"):
            data.append(line[5:].lstrip(" "))
        if sum(map(len, data)) > 262144:
            raise ValueError("oversized SSE event")
    if data:
        body = "\n".join(data)
        if body != "[DONE]":
            yield json.loads(body)


class CompatibleLLM:
    def __init__(self, settings, client: httpx.AsyncClient):
        self.s, self.client = settings, client

    async def stream(self, messages, system):
        s = self.s
        payload = {"model": s.llm_model, "stream": True,
                   "messages": [{"role": "system", "content": system}] + messages,
                   s.llm_token_parameter: s.llm_max_tokens}
        if s.llm_send_temperature:
            payload["temperature"] = s.llm_temperature
        headers = {"Authorization": f"Bearer {s.llm_api_key}"} if s.llm_api_key else {}
        async with self.client.stream("POST", s.llm_base_url.rstrip("/") + "/chat/completions",
                                      headers=headers, json=payload) as response:
            response.raise_for_status()
            async for event in sse_objects(response):
                if "error" in event:
                    raise RuntimeError("LLM stream reported an error")
                for choice in event.get("choices", [])[:1]:
                    delta = choice.get("delta", {})
                    if delta.get("tool_calls"):
                        raise RuntimeError("This speech profile has no authorized tool executor")
                    content = delta.get("content")
                    if isinstance(content, str) and content:
                        yield content


class RemoteASR:
    def __init__(self, settings):
        self.s = settings

    async def recognize(self, audio, on_partial):
        headers = {"Authorization": f"Bearer {self.s.speech_token}"} if self.s.speech_token else {}
        async with websockets.connect(ws_url(self.s.asr_base_url or self.s.speech_base_url) + "/asr",
                additional_headers=headers, open_timeout=self.s.io_timeout,
                max_size=16384, max_queue=16) as ws:
            result = asyncio.get_running_loop().create_future()

            async def send():
                async for samples in audio:
                    await ws.send(float_to_pcm16(samples))
                await ws.send(json.dumps({"type": "finish"}))

            async def receive():
                async for raw in ws:
                    event = json.loads(raw)
                    if event.get("type") == "partial":
                        on_partial(str(event.get("text", ""))[:8192])
                    elif event.get("type") == "final":
                        result.set_result(str(event.get("text", ""))[:8192])
                        return
                    elif event.get("type") == "error":
                        raise RuntimeError("speech service recognition failed")
                if not result.done():
                    raise RuntimeError("ASR closed without final transcript")

            async with asyncio.TaskGroup() as tg:
                tx = tg.create_task(send())
                rx = tg.create_task(receive())
                text = await result
                await tx
                await rx
            return text.strip()


class DeepgramASR:
    """True streaming input. CloseStream finalizes one isolated utterance.

    This intentionally trades a per-utterance handshake for an unambiguous ownership
    boundary. The handshake overlaps speech. It is not a persistent-per-call design.
    """
    def __init__(self, settings):
        self.s = settings
        if not settings.deepgram_api_key:
            raise ValueError("DEEPGRAM_API_KEY is required")

    async def recognize(self, audio, on_partial):
        params = {"model": self.s.deepgram_model, "language": "en", "encoding": "linear16",
                  "sample_rate": 16000, "channels": 1, "interim_results": "true",
                  "punctuate": "true", "endpointing": "false"}
        async with websockets.connect(self.s.deepgram_url + "?" + urlencode(params),
                additional_headers={"Authorization": "Token " + self.s.deepgram_api_key},
                open_timeout=self.s.io_timeout, max_size=262144, max_queue=16) as ws:
            finals: list[str] = []
            finished = asyncio.Event()
            metadata_seen = False

            async def send():
                async for frame in audio:
                    await ws.send(float_to_pcm16(frame))
                finished.set()
                await ws.send(json.dumps({"type": "CloseStream"}))

            async def receive():
                nonlocal metadata_seen
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("type") == "Error":
                        raise RuntimeError("Deepgram returned an error")
                    if msg.get("type") == "Metadata" and finished.is_set():
                        metadata_seen = True
                    if msg.get("type") != "Results":
                        continue
                    alts = msg.get("channel", {}).get("alternatives", [])
                    text = str(alts[0].get("transcript", "")) if alts else ""
                    if msg.get("is_final") and text:
                        finals.append(text)
                        on_partial(" ".join(finals))
                    elif text:
                        on_partial(" ".join(finals + [text]))
                if not finished.is_set() or not metadata_seen:
                    raise RuntimeError("Deepgram closed without its end-of-stream metadata fence")

            async with asyncio.TaskGroup() as tg:
                tg.create_task(send())
                tg.create_task(receive())
            return " ".join(finals).strip()


class RemoteTTS:
    sample_rate = 24000
    def __init__(self, settings, client):
        self.s, self.client = settings, client

    async def synthesize(self, text):
        headers = {"Authorization": f"Bearer {self.s.speech_token}"} if self.s.speech_token else {}
        decoder = PCMDecoder()
        async with self.client.stream("POST", (self.s.tts_base_url or self.s.speech_base_url).rstrip("/") + "/tts",
                                      json={"text": text}, headers=headers) as response:
            response.raise_for_status()
            if response.headers.get("x-sample-rate") != "24000":
                raise ValueError("speech service returned an unexpected sample rate")
            async for b in response.aiter_bytes():
                if len(b) > 2_000_000:
                    raise ValueError("oversized PCM chunk")
                out = decoder.push(b)
                if out.size:
                    yield out
            decoder.finish()


class ElevenLabsTTS:
    sample_rate = 24000
    def __init__(self, settings, client):
        self.s, self.client = settings, client
        if not settings.elevenlabs_api_key or not settings.elevenlabs_voice_id:
            raise ValueError("ELEVENLABS_API_KEY and ELEVENLABS_VOICE_ID are required")
        if not settings.elevenlabs_voice_id.isalnum():
            raise ValueError("expected an alphanumeric ElevenLabs voice ID")

    async def synthesize(self, text):
        decoder = PCMDecoder()
        url = "https://api.elevenlabs.io/v1/text-to-speech/" + self.s.elevenlabs_voice_id + "/stream"
        async with self.client.stream("POST", url, params={"output_format": "pcm_24000"},
                headers={"xi-api-key": self.s.elevenlabs_api_key},
                json={"text": text, "model_id": self.s.elevenlabs_model}) as response:
            response.raise_for_status()
            async for b in response.aiter_bytes():
                out = decoder.push(b)
                if out.size:
                    yield out
            decoder.finish()


class DemoASR:
    async def recognize(self, audio, on_partial):
        n = 0
        async for frame in audio:
            n += len(frame)
            if n % 8192 == 0:
                on_partial("[Demo: receiving audio; no speech recognition]")
        return "Demonstrate the audio pipeline."


class DemoLLM:
    async def stream(self, messages, system):
        for part in ["This is the offline transport test. ", "Its audio is a tone, not synthesized speech."]:
            await asyncio.sleep(.03)
            yield part


class DemoTTS:
    sample_rate = 24000
    async def synthesize(self, text):
        # Every phrase is a quiet 0.24-second tone; never claim this is a real voice model.
        n = 5760
        x = .04 * np.sin(2 * np.pi * 440 * np.arange(n) / self.sample_rate)
        x *= np.minimum(1, np.arange(n)/200) * np.minimum(1, (n-np.arange(n))/200)
        for i in range(0, n, 480):
            await asyncio.sleep(0)
            yield x[i:i+480].astype(np.float32)
