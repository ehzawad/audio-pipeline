"""Separately deployable local ASR/TTS worker. Run ONE process per model allocation.

WebSocket /asr: PCM16/16k mono -> partial JSON -> finish -> final JSON.
HTTP POST /tts: phrase -> raw PCM16/24k. Kokoro is phrase inference, not causal TTS.
"""
from __future__ import annotations
import asyncio
import json
import os
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, WebSocket
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field

from .audio import pcm16_to_float, float_to_pcm16
from .concurrency import Admission, ModelRunner, Overloaded
from .security import check_bearer
from .http_limits import BodyLimit


class LeasedStreamingResponse(StreamingResponse):
    """Hold admission until the HTTP stream actually ends, including disconnects."""
    def __init__(self, *args, lease, **kwargs):
        super().__init__(*args, **kwargs)
        self.lease = lease

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self.lease.__aexit__(None, None, None)


class SpeechRequest(BaseModel):
    text: str = Field(min_length=1, max_length=260)


class StreamingASR:
    def __init__(self):
        import sherpa_onnx
        root = Path(os.environ.get("ASR_MODEL_DIR", "models/zipformer"))
        prefix = "epoch-99-avg-1-chunk-16-left-128.int8.onnx"
        paths = {k: str(root / (f"{k}-" + prefix)) for k in ["encoder", "decoder", "joiner"]}
        paths["tokens"] = str(root / "tokens.txt")
        for filename in paths.values():
            if not Path(filename).is_file():
                raise FileNotFoundError(f"Missing {filename}; run scripts/download_models.py")
        self.recognizer = sherpa_onnx.OnlineRecognizer.from_transducer(
            **paths, sample_rate=16000, feature_dim=80, num_threads=1,
            decoding_method="greedy_search", enable_endpoint_detection=False, provider="cpu")
        stream = self.recognizer.create_stream()
        self.decode(stream, np.zeros(16000, np.float32), finished=True)

    def decode(self, stream, samples, finished=False):
        if len(samples):
            stream.accept_waveform(16000, samples)
        if finished:
            stream.accept_waveform(16000, np.zeros(8000, np.float32))
            stream.input_finished()
        while self.recognizer.is_ready(stream):
            self.recognizer.decode_stream(stream)
        return self.recognizer.get_result(stream)

class PhraseTTS:
    def __init__(self):
        from kokoro import KPipeline, KModel
        tts_root = Path(os.environ.get("TTS_MODEL_DIR", "models/kokoro"))
        voice_name = os.environ.get("KOKORO_VOICE", "af_heart")
        tts_config, tts_weights = tts_root / "config.json", tts_root / "kokoro-v1_0.pth"
        voice_path = tts_root / "voices" / (voice_name + ".pt")
        for filename in (tts_config, tts_weights, voice_path):
            if not filename.is_file():
                raise FileNotFoundError(f"Missing {filename}; run scripts/download_models.py")
        model = KModel(repo_id="hexgrad/Kokoro-82M", config=str(tts_config),
                       model=str(tts_weights)).to(os.environ.get("TTS_DEVICE", "cpu")).eval()
        self.tts = KPipeline(lang_code="a", repo_id="hexgrad/Kokoro-82M", model=model)
        self.voice = str(voice_path)
        # Third-party debug logs may contain graphemes/phonemes; leave them disabled.
        from loguru import logger
        logger.disable("kokoro")
        logger.disable("misaki")
        self.speed = float(os.environ.get("KOKORO_SPEED", "1"))
        self.synthesize("Ready.")


    def synthesize(self, text):
        chunks, n = [], 0
        for result in self.tts(text, voice=self.voice, speed=self.speed):
            audio = result.audio
            if audio is None:
                continue
            if hasattr(audio, "detach"):
                audio = audio.detach().cpu().numpy()
            audio = np.asarray(audio, dtype=np.float32)
            n += len(audio)
            if n > 30 * 24000:
                raise ValueError("Kokoro exceeded per-phrase audio budget")
            chunks.append(audio)
        if not chunks:
            raise ValueError("Kokoro returned no audio")
        return np.concatenate(chunks)



class NativeModels:
    """Composition facade: ASR and TTS can occupy independent worker deployments."""
    def __init__(self):
        role = os.environ.get('SPEECH_ROLE', 'all')
        if role not in {'all', 'asr', 'tts'}:
            raise ValueError('SPEECH_ROLE must be all, asr or tts')
        self.recognition = StreamingASR() if role in {'all', 'asr'} else None
        self.synthesis = PhraseTTS() if role in {'all', 'tts'} else None
        self.asr = self.recognition.recognizer if self.recognition else None

    def decode(self, stream, samples, finished=False):
        if self.recognition is None:
            raise RuntimeError('ASR is not enabled in this worker')
        return self.recognition.decode(stream, samples, finished)

    def synthesize(self, text):
        if self.synthesis is None:
            raise RuntimeError('TTS is not enabled in this worker')
        return self.synthesis.synthesize(text)

def create_speech_app(model_factory=NativeModels):
    @asynccontextmanager
    async def lifespan(app):
        load_dotenv()
        app.state.role = os.environ.get('SPEECH_ROLE', 'all')
        if app.state.role not in {'all', 'asr', 'tts'}:
            raise ValueError('invalid SPEECH_ROLE')
        app.state.token = os.environ.get("SPEECH_TOKEN", "")
        if os.environ.get("DEPLOYMENT") == "production" and not app.state.token:
            raise RuntimeError("production speech service requires SPEECH_TOKEN")
        app.state.models = await asyncio.to_thread(model_factory)
        app.state.asr_runner, app.state.tts_runner = ModelRunner(1), ModelRunner(1)
        app.state.cap = Admission(int(os.environ.get("SPEECH_MAX_REQUESTS", "8")))
        yield
        app.state.cap.draining = True
        await app.state.asr_runner.close()
        await app.state.tts_runner.close()

    app = FastAPI(title="Duplex local speech worker", lifespan=lifespan)

    app.add_middleware(BodyLimit, limit=16384)

    def authorize(value):
        if app.state.token and not check_bearer(value, app.state.token):
            raise HTTPException(401, "invalid speech service token")

    @app.get("/healthz")
    async def health():
        return {"ok": True}

    @app.get("/readyz")
    async def ready():
        return JSONResponse({"ready": not app.state.cap.draining, "role": app.state.role,
                "asr": "streaming-zipformer" if app.state.role != "tts" else None,
                "tts": "phrase-kokoro" if app.state.role != "asr" else None, "active": app.state.cap.active}, status_code=503 if app.state.cap.draining else 200)

    @app.websocket("/asr")
    async def asr(ws: WebSocket):
        try:
            authorize(ws.headers.get("authorization", ""))
            if app.state.role == "tts":
                raise HTTPException(404, "ASR disabled in this worker")
            async with app.state.cap.slot():
                await ws.accept()
                stream = app.state.models.asr.create_stream()
                last, count = "", 0
                async with asyncio.timeout(150):
                    while True:
                        msg = await ws.receive()
                        if msg["type"] == "websocket.disconnect":
                            return
                        if msg.get("bytes") is not None:
                            b = msg["bytes"]
                            if len(b) > 6400:
                                raise ValueError("ASR frames must not exceed 200 ms")
                            samples = pcm16_to_float(b)
                            count += len(samples)
                            if count > 122 * 16000:
                                raise ValueError("ASR utterance exceeded limit")
                            text = await app.state.asr_runner.run(app.state.models.decode, stream, samples)
                            if text != last:
                                last = text
                                await ws.send_json({"type": "partial", "text": text})
                        elif msg.get("text"):
                            if len(msg["text"]) > 1024:
                                raise ValueError("oversized ASR control")
                            if json.loads(msg["text"]).get("type") != "finish":
                                raise ValueError("unexpected ASR control")
                            text = await app.state.asr_runner.run(app.state.models.decode, stream,
                                                                 np.zeros(0, np.float32), True)
                            await ws.send_json({"type": "final", "text": text})
                            return
        except (HTTPException, Overloaded):
            with suppress(Exception):
                await ws.close(code=1008)
        except Exception:
            with suppress(Exception):
                await ws.send_json({"type": "error", "message": "recognition failed"})
        finally:
            with suppress(Exception):
                await ws.close()

    @app.post("/tts")
    async def tts(body: SpeechRequest, authorization: str = Header(default="")):
        authorize(authorization)
        if app.state.role == "asr":
            raise HTTPException(404, "TTS disabled in this worker")
        lease = app.state.cap.slot()
        try:
            await lease.__aenter__()
        except Overloaded:
            raise HTTPException(503, "speech capacity exhausted")
        try:
            # Native work owns its compute slot until the native call really finishes.
            async with asyncio.timeout(45):
                samples = await app.state.tts_runner.run(app.state.models.synthesize, body.text)
        except BaseException:
            await lease.__aexit__(None, None, None)
            raise
        async def body_stream():
            for start in range(0, len(samples), 480):
                yield float_to_pcm16(samples[start:start+480])
                await asyncio.sleep(0)
        return LeasedStreamingResponse(body_stream(), lease=lease, media_type="application/octet-stream",
                                 headers={"X-Sample-Rate": "24000", "X-Audio-Format": "pcm_s16le_mono"})
    return app


app = create_speech_app()
