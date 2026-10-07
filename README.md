# Audio Pipeline 3.0

An English streaming voice gateway for browsers, native-client integrations, and telephone media. Evolved from the supplied `voicebot` implementation and Duplex Voice 2.0. The v3 change is **explicit shared session ownership**, not a claim that a newer model automatically improves conversation quality.

**Release boundary:** runnable source, deterministic failure tests, a Redis control adapter, and deployment templates. This is not a million-concurrent-call benchmark, a carrier-certified telco switch, a complete mobile application, or a native speech-to-speech foundation model. See [verification](verification/README.md) for executed versus unexecuted checks.

## Architecture

```text
Browser / native PCM client          PSTN -> Twilio       SIP -> Asterisk
          | WebSocket / WebRTC           | WebSocket            | AudioSocket
          +------------------------------+----------------------+
                                         |
                       one live gateway owner per media session
                      VAD -> streaming ASR -> LLM -> phrase TTS
                             epoch + playback receipts + history
                              |                 |             |
                         ASR worker         LLM server     TTS worker
                              |
                  Redis cell: reservations, leases, fences, checkpoints
                  (metadata at call/turn/heartbeat cadence, NOT audio frames)
```

A session may be reserved through gateway A and its one-use grant consumed on B. Redis atomically enforces tenant capacity and ownership. An owner that cannot renew its lease stops emitting media. Recovery starts a **new** media connection with a higher fence; it cannot resurrect packets, native ASR state, or unconfirmed speech from the dead process.

## Quick start: inspect the control path without model downloads

```sh
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
cp .env.example .env
python -m pytest -q
uvicorn duplex_voice.main:app --host 127.0.0.1 --port 8000 --workers 1 \
  --ws-max-size 65536 --ws-max-queue 16 --no-access-log
```

Open `http://localhost:8000`. **Demo mode uses scripted recognition and quiet tones. It does not understand your speech.** It exercises capture, wire framing, interruption, playback, and cleanup without model variability. Use headphones when testing real speech; no server-side acoustic echo canceller is included.

Python 3.11–3.13 is supported by the declared package contract. No API key is required for the demo. The source dependency ranges are intentional: resolve, freeze, scan, and pin image/model digests in your release pipeline; this repository does not pretend an offline-generated lock is a reproducible hardware environment.

## Real English speech: local/Hugging Face profile

The existing-model baseline is Silero VAD, streaming sherpa-onnx Zipformer, a separately served text LLM, and Kokoro phrase synthesis. The download script resolves Hugging Face revisions and records file hashes. It does **not** bundle neural weights or claim these checkpoints lead current speech benchmarks. [Model provenance](docs/SOURCES.md) and [the startup runbook](docs/RUNBOOK.md) explain the trade-offs.

```sh
cp .env.local.example .env
python -m pip install -e '.[dev,vad,speech]'
python -m scripts.download_models
# Fill SPEECH_TOKEN privately in .env. Install the OS speech prerequisites first.
# Terminal 1: both speech roles on a small development machine
uvicorn duplex_voice.speech_service:app --host 127.0.0.1 --port 8001 --workers 1
# Terminal 2, Linux/NVIDIA: use a separate environment for the inference engine
./scripts/serve_llm.sh
# macOS uses ./scripts/serve_macos.sh instead; it requires an operator-selected GGUF file.
# Terminal 3
uvicorn duplex_voice.main:app --host 127.0.0.1 --port 8000 --workers 1 \
  --ws-max-size 65536 --ws-max-queue 16 --no-access-log
```

For independent ASR/TTS processes, set `SPEECH_ROLE=asr` and `SPEECH_ROLE=tts` on two workers, then set `ASR_BASE_URL` and `TTS_BASE_URL` on the gateway. The roles load only their own models. `deploy/compose.local.yaml` supplies a CPU reference topology; it is not a measured GPU deployment. Kokoro is synthesized per complete phrase; byte streaming afterward does not make its model causal token-to-audio streaming.

The hosted profile `.env.cloud.example` uses Deepgram and ElevenLabs plus a configurable compatible streaming LLM endpoint. Commercial services require accounts and are not represented as downloadable Hugging Face weights. No new API keys or paid phone calls were created for this release.

## Shared control and native-client integration

For more than one gateway, install `.[cluster]`, use `STATE_BACKEND=redis`, and share the same cell, signing key, issuer configuration, and Redis endpoint. The SQLite adapter is a single-host reference/testing backend, not a horizontally distributed store. Production configuration refuses SQLite, anonymous identity, and demo models.

```sh
cp .env.cell.example .env
# Generate THREE distinct values, one per SIGNING_KEY, IDENTITY_SECRET and APP_TOKEN:
python -c 'import secrets; print(secrets.token_urlsafe(32))'
# Fill .env privately, then:
docker compose --env-file .env -f deploy/compose.cell.yaml up --build
```

This local cell is two HTTP/PCM gateways behind Caddy with a private, persistent Redis. Its localhost HTTP endpoint is deliberate; use TLS and private Redis ACLs in deployment. This example is **not highly available Redis**. `RTC_ENABLED=false` avoids implying that an ordinary HTTP load balancer solves ICE/UDP routing.

The application's authenticated backend issues a short-lived JWT scoped to `tenant`, `sub`, and `aud=audio-pipeline:<cell>`. The client uses it to `POST /v1/sessions` with an `Idempotency-Key`. The returned media grant is a different credential: one-use, session/transport/cell bound. Never distribute `IDENTITY_SECRET`, `SIGNING_KEY`, `SPEECH_TOKEN`, or the operator `APP_TOKEN` in an application binary.

| Integration | Included code | Explicit boundary |
|---|---|---|
| Browser | AudioWorklet capture/playback, PCM WebSocket, optional aiortc WebRTC | Physical AEC, network loss and device behavior require live testing |
| JavaScript | HTTP control client, PCM socket driver, strict parser and playback cursor | Renderer callback must represent actual render completion, not packet arrival |
| Swift | Swift package with actor-isolated HTTP control, WebSocket task creation, packet/receipt core | AVAudioEngine/voice-processing audio session and UI remain app-specific |
| Kotlin | JVM/Android-compatible PCM parser and playback cursor | AudioRecord/AudioTrack and HTTP/WebSocket driver remain app-specific |
| Twilio | Signed webhook + signed media upgrade + call-bound grant, marks and clear | Real phone/account/region tests required |
| Asterisk | Private-network AudioSocket, paced PCM and DTMF input | Existing PBX/trunk/SBC owns SIP, RTP, TLS/SRTP and carrier integration |

See [client protocol](docs/PROTOCOLS.md). Mobile code is an integration core, not a finished iOS/Android call UI.

## What is stronger in v3

Shared CAS state, expiring reservations, bounded tenant quotas, monotonic fences, reincarnation-safe grants, and explicit reconnect replace process-local ownership assumptions. A local monotonic media guard enforces lease loss at transport write boundaries. Checkpoints retain bounded dialogue history only when `PERSIST_HISTORY=true`; the default stores no transcript. Metadata retention and transcript consent still need an application policy.

Playback credit bounds the gateway's unacknowledged output. Input audio has a real-time rate budget, not only a byte-size cap. ASR, LLM and TTS have independent fail-fast concurrency limits and circuit breakers; canceled work is not treated as a provider failure, and no spoken output is blindly retried. Native model capacity remains occupied until the native job actually finishes.

Draining stops admission, waits for active calls up to a bound, then cancels and finalizes owned state before closing sockets. Prometheus histograms and counters have bounded stage/event labels, without tenant IDs, transcripts, or call IDs. This is observability infrastructure, not a promised latency SLO.

## Scale by workload, not by adjectives

```sh
python -m scripts.capacity --sessions-per-day 1000000 --duration-seconds 180 \
  --peak-factor 3 --calls-per-gateway 100 --target-utilization .6
```

Here **100 calls per gateway is an assumption you supplied**, not a measured result. One million three-minute sessions/day averages about 2,083 simultaneous sessions; a threefold peak gives 6,250. One million **simultaneous** sessions is a different system. The script separates arrivals, concurrency, packet cadence, bandwidth, and control-store work. Model capacity, carrier limits, failure reserves, regional placement, and cost are intentionally not inferred from a fake-model test.

Partition deployments into independently budgeted cells. The trusted identity issuer assigns a tenant/session to a home cell. Quotas in this implementation are cell-local; there is no hidden global scheduler. `deploy/kubernetes/gateway.yaml` is an operator-filled HTTP/PCM template, not a proof that those resource requests support a particular call count.

## Verification and research

```sh
python -m pytest -q
node --test clients/javascript/voice.test.mjs
swift test --package-path clients/swift
python -m scripts.check_clients  # also requires kotlinc and a JRE
# Real Redis tests require a DISPOSABLE local test server; never use a production database.
TEST_REDIS_URL=redis://127.0.0.1:6379/0 python -m pytest tests/test_redis_integration.py -q
# Cross-gateway synthetic traffic; configure both gateways with the same control state:
python -m scripts.load_ws --url http://127.0.0.1:8010 \
  --media-url http://127.0.0.1:8011 --calls 32
```

[Research notes](docs/SOURCES.md) connect ACL 2026 Full-Duplex-Bench-v2, Interspeech 2025 FD-Bench, and EACL 2026 turn-taking analysis to concrete evaluation boundaries. They do not claim that this repository reproduced those papers' scores. [The mental-model guide](docs/BOOK.md) connects timing, state, recovery, and scale. [The runbook](docs/RUNBOOK.md) covers deployment and failure drills. [Verification](verification/README.md) records the actual checks and omissions.

Outbound calling is opt-in and protected, but its SQLite effect ledger is deliberately **single-host**. Keep it disabled in replicated media gateways; a dedicated single-host dialer can use the supplied route. Multi-host durable dialing, human transfer/contact-center routing, emergency calling, recording compliance, active-active control-store failover, native speech-to-speech models, and semantic turn models are not implemented.
