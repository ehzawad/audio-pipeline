from __future__ import annotations
import httpx
from .clients import (CompatibleLLM, RemoteASR, RemoteTTS, DeepgramASR, ElevenLabsTTS,
                      DemoASR, DemoLLM, DemoTTS)
from ..vad import SileroFactory, EnergyVAD
from ..resilience import Circuit, GuardedASR, GuardedLLM, GuardedTTS


class Providers:
    def __init__(self, s):
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(15, connect=s.io_timeout),
                                       limits=httpx.Limits(max_connections=64, max_keepalive_connections=16))
        self.silero = None
        if s.voice_mode == "demo":
            self.asr, self.llm, self.tts = DemoASR(), DemoLLM(), DemoTTS()
        else:
            self.asr = RemoteASR(s) if s.asr_provider == "speech" else DeepgramASR(s)
            self.llm = CompatibleLLM(s, self.client)
            self.tts = RemoteTTS(s, self.client) if s.tts_provider == "speech" else ElevenLabsTTS(s, self.client)
            self.asr = GuardedASR(self.asr, Circuit(s.asr_concurrency, s.breaker_failures, s.breaker_reset_seconds))
            self.llm = GuardedLLM(self.llm, Circuit(s.llm_concurrency, s.breaker_failures, s.breaker_reset_seconds))
            self.tts = GuardedTTS(self.tts, Circuit(s.tts_concurrency, s.breaker_failures, s.breaker_reset_seconds))
        if s.effective_vad == "silero":
            self.silero = SileroFactory(s.vad_model_path)

    def make_vad(self):
        return self.silero.make() if self.silero else EnergyVAD()

    async def close(self):
        await self.client.aclose()
