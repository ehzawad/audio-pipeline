"""Only operators choose providers, model IDs, URLs and trust boundaries."""
from __future__ import annotations

import os
from typing import Literal
from urllib.parse import urlsplit

from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, model_validator


class Settings(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)
    state_backend: Literal["sqlite", "redis"] = "sqlite"
    state_path: str = ":memory:"
    redis_url: str = "redis://127.0.0.1:6379/0"
    cell_id: str = Field("default", pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    worker_id: str = ""
    default_tenant: str = Field("default", pattern=r"^[A-Za-z0-9_-]{1,64}$")
    identity_secret: str = ""
    identity_issuer: str = "audio-pipeline-app"
    tenant_session_limit: int = Field(100, ge=1, le=100000)
    lease_seconds: float = Field(9, ge=3, le=60)
    control_timeout: float = Field(2, ge=.1, le=5)
    retention_seconds: int = Field(3600, ge=60, le=604800)
    persist_history: bool = False
    drain_seconds: float = Field(30, ge=0, le=3600)
    playback_window_frames: int = Field(25, ge=5, le=100)
    asr_concurrency: int = Field(16, ge=1, le=256)
    llm_concurrency: int = Field(8, ge=1, le=256)
    tts_concurrency: int = Field(8, ge=1, le=256)
    breaker_failures: int = Field(5, ge=1, le=100)
    breaker_reset_seconds: float = Field(10, ge=.1, le=120)
    asr_base_url: str = ""
    tts_base_url: str = ""
    voice_mode: Literal["demo", "local", "cloud"] = "demo"
    deployment: Literal["development", "production"] = "development"
    app_token: str = ""
    signing_key: str = ""
    public_base_url: str = "http://localhost:8000"
    allowed_origins: str = "http://localhost:8000,http://127.0.0.1:8000"
    max_sessions: int = Field(4, ge=1, le=1000)
    max_call_seconds: float = Field(900, ge=10, le=14400)
    max_utterance_seconds: float = Field(30, ge=1, le=120)
    max_pending_utterances: int = Field(4, ge=1, le=16)
    ingress_burst_seconds: float = Field(.5, ge=.2, le=3)
    ingress_frames: int = Field(32, ge=4, le=128)
    asr_queue_frames: int = Field(64, ge=8, le=256)
    phrase_queue_size: int = Field(3, ge=1, le=8)
    output_queue_frames: int = Field(20, ge=2, le=100)
    response_timeout: float = Field(60, ge=1, le=180)
    asr_timeout: float = Field(6, ge=0.1, le=30)
    playback_timeout: float = Field(8, ge=0.1, le=30)
    io_timeout: float = Field(3, ge=0.1, le=30)
    max_reply_chars: int = Field(1000, ge=32, le=8000)
    max_history_chars: int = Field(12000, ge=128, le=100000)
    max_history_messages: int = Field(24, ge=2, le=100)
    greeting: str = "Hello. I am an AI voice assistant. How can I help?"
    vad_backend: Literal["auto", "silero", "energy"] = "auto"
    vad_model_path: str = "models/silero_vad.onnx"
    vad_threshold: float = Field(0.5, ge=0, le=1)
    vad_start_ms: int = Field(160, ge=32, le=1000)
    vad_end_ms_web: int = Field(480, ge=64, le=3000)
    vad_end_ms_phone: int = Field(640, ge=64, le=3000)
    barge_in_start_ms: int = Field(224, ge=32, le=1500)
    preroll_ms: int = Field(256, ge=32, le=1000)
    asr_provider: Literal["speech", "deepgram"] = "speech"
    tts_provider: Literal["speech", "elevenlabs"] = "speech"
    speech_base_url: str = "http://127.0.0.1:8001"
    speech_token: str = ""
    deepgram_api_key: str = ""
    deepgram_model: str = "nova-3"
    deepgram_url: str = "wss://api.deepgram.com/v1/listen"
    elevenlabs_api_key: str = ""
    elevenlabs_voice_id: str = ""
    elevenlabs_model: str = "eleven_flash_v2_5"
    llm_base_url: str = "http://127.0.0.1:8002/v1"
    llm_api_key: str = ""
    llm_model: str = "voice-llm"
    llm_max_tokens: int = Field(256, ge=8, le=2048)
    llm_token_parameter: Literal["max_tokens", "max_completion_tokens"] = "max_tokens"
    llm_temperature: float = Field(0.4, ge=0, le=2)
    llm_send_temperature: bool = True
    twilio_enabled: bool = False
    twilio_validate: bool = True
    twilio_auth_token: str = ""
    twilio_account_sid: str = ""
    twilio_from_number: str = ""
    outbound_enabled: bool = False
    outbound_allowlist: str = ""
    outbound_db: str = "data/outbound.sqlite3"
    audiosocket_enabled: bool = False
    audiosocket_host: str = "127.0.0.1"
    audiosocket_port: int = Field(9092, ge=1, le=65535)
    audiosocket_allowed_cidrs: str = "127.0.0.1/32,::1/128"
    rtc_connect_timeout: float = Field(20, ge=3, le=60)
    rtc_enabled: bool = True
    rtc_ice_servers_json: str = "[]"
    rtc_estimated_playout_ms: int = Field(200, ge=0, le=2000)

    @model_validator(mode="after")
    def validate_security(self):
        if self.state_backend == "redis" and len(self.signing_key) < 32:
            raise ValueError("Shared state requires the same SIGNING_KEY (32+) on every gateway")
        if self.identity_secret and len(self.identity_secret) < 32:
            raise ValueError("IDENTITY_SECRET requires at least 32 characters")
        if self.control_timeout >= self.lease_seconds / 2:
            raise ValueError("CONTROL_TIMEOUT must be below half LEASE_SECONDS")
        u = urlsplit(self.public_base_url)
        if u.scheme not in {"http", "https"} or not u.netloc or u.path not in {"", "/"}:
            raise ValueError("PUBLIC_BASE_URL must be an HTTP(S) origin without a path")
        if u.query or u.fragment or u.username or u.password:
            raise ValueError("PUBLIC_BASE_URL must not contain credentials, query or fragment")
        if self.voice_mode != "demo" and self.vad_backend == "energy":
            raise ValueError("Energy VAD is permitted only in the explicitly labeled demo")
        if self.deployment == "production":
            if (len(self.app_token) < 24 and len(self.identity_secret) < 32) or len(self.signing_key) < 32:
                raise ValueError("Production requires APP_TOKEN (24+) or IDENTITY_SECRET (32+), and SIGNING_KEY (32+)")
            if not self.speech_token and self.voice_mode == "local":
                raise ValueError("Production local mode requires SPEECH_TOKEN")
            if u.scheme != "https" or "*" in self.allowed_origins:
                raise ValueError("Production requires HTTPS and exact ALLOWED_ORIGINS")
            if self.voice_mode == "demo" or not self.twilio_validate:
                raise ValueError("Production forbids demo mode and disabled Twilio validation")
        if self.deployment == "production" and self.state_backend != "redis":
            raise ValueError("Production requires the shared Redis control backend")
        if self.twilio_enabled and self.twilio_validate and not self.twilio_auth_token:
            raise ValueError("TWILIO_AUTH_TOKEN is required when Twilio is enabled")
        if self.outbound_enabled and not (self.twilio_enabled and self.outbound_allowlist):
            raise ValueError("Outbound requires enabled Twilio and an explicit destination allowlist")
        return self

    @classmethod
    def from_env(cls, filename: str = ".env") -> "Settings":
        env = {**dotenv_values(filename), **os.environ}
        return cls(**{name: env[name.upper()] for name in cls.model_fields
                      if env.get(name.upper()) is not None})

    @property
    def origins(self) -> set[str]:
        return {x.strip() for x in self.allowed_origins.split(",") if x.strip()}

    @property
    def effective_vad(self) -> str:
        if self.vad_backend != "auto":
            return self.vad_backend
        return "energy" if self.voice_mode == "demo" else "silero"
