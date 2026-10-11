from __future__ import annotations
import httpx
from .clients import (CompatibleLLM, ResponsesLLM, RemoteASR, RemoteTTS, DeepgramASR, ElevenLabsTTS,
                      OpenAIRealtimeASR, OpenAITTS, DemoASR, DemoLLM, DemoTTS)
from ..vad import SileroFactory, EnergyVAD
from ..resilience import Circuit, GuardedASR, GuardedLLM, GuardedTTS, HedgedTTS


class Providers:
    def __init__(self, s):
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(15, connect=s.io_timeout),
                                       limits=httpx.Limits(max_connections=64, max_keepalive_connections=16))
        self.silero = None
        if s.voice_mode == "demo":
            self.asr, self.llm, self.tts = DemoASR(), DemoLLM(), DemoTTS()
        elif s.voice_mode == "live":
            self.asr = self.llm = self.tts = None  # one GPT-Live session per call instead
        else:
            self.asr = {"speech": RemoteASR, "deepgram": DeepgramASR,
                        "openai": OpenAIRealtimeASR}[s.asr_provider](s)
            self.llm = (ResponsesLLM if s.llm_api == "responses" else CompatibleLLM)(s, self.client)
            self.tts = {"speech": RemoteTTS, "elevenlabs": ElevenLabsTTS,
                        "openai": OpenAITTS}[s.tts_provider](s, self.client)
            self.asr = GuardedASR(self.asr, Circuit(s.asr_concurrency, s.breaker_failures, s.breaker_reset_seconds))
            self.llm = GuardedLLM(self.llm, Circuit(s.llm_concurrency, s.breaker_failures, s.breaker_reset_seconds))
            self.tts = GuardedTTS(self.tts, Circuit(s.tts_concurrency, s.breaker_failures, s.breaker_reset_seconds))
            if s.tts_prebuffer_ms or s.tts_hedge_seconds:
                self.tts = HedgedTTS(self.tts, s.tts_prebuffer_ms / 1000, s.tts_hedge_seconds)
        if s.effective_vad == "silero":
            self.silero = SileroFactory(s.vad_model_path)

    def make_vad(self):
        return self.silero.make() if self.silero else EnergyVAD()

    async def close(self):
        await self.client.aclose()
