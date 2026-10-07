# Operating the cell

## Before the first real conversation

Start with the deterministic suite, then one real model probe, then browser headphones, then a real phone. Record exact model/runtime/artifact versions and the audio channel. Do not infer a GPU capacity from a scripted tone demo.

Linux local prerequisites: Python 3.11+, `espeak-ng`, a supported ONNX runtime, and the optional speech dependencies. For Kokoro English, install the English spaCy resource when required by the selected package: `python -m spacy download en_core_web_sm`. The CPU speech Dockerfile includes it. Download with `python -m scripts.download_models`; inspect `models/manifest.json`, resolved revisions and licenses. The LLM launchers are source-aligned recipes, not executed GPU/macOS inference measurements. Keep vLLM dependencies in a separate environment. The macOS llama.cpp launcher requires an existing `LLM_GGUF_PATH` path and an installed `llama-server`; it does not silently download an arbitrary model.

Use `.env.local.example` for the gateway and speech worker. Start speech on 8001 and an LLM endpoint on 8002, then `python -m scripts.doctor --probe`. Run `python -m scripts.probe question.wav` to exercise one real pipeline turn. Record its stage timings; dispatch timing is not speaker timing. Split speech roles only after the combined single-host path is verified. Restart/warm workers before admitting calls; the readiness endpoint alone is not a periodic semantic model health test.

## Secrets and tenancy

Use three different secrets: SIGNING_KEY for media grants, IDENTITY_SECRET shared only with the trusted authenticated issuer, and APP_TOKEN for operator endpoints. Add SPEECH_TOKEN and provider credentials privately. Never put any long-lived secret in JavaScript, Swift, Kotlin, Git, query parameters or logs. The bundled browser token input is for a backend-issued short-lived identity or private local operator testing; it is not a production user-login design.

Issue only the tenant/subject and home-cell audience the logged-in principal is authorized to use. This gateway does not implement your login or tenant provisioning system. The HMAC JWT integration assumes a trusted issuer; OIDC discovery/JWKS, signing-key rotation overlap, identity revocation and global home-cell directories are deployment extensions, not present features.

Twilio/PBX routes map a configured provider account or private PBX to DEFAULT_TENANT. To host independent carrier accounts securely, isolate edges/configuration or add a reviewed account-to-tenant registry. Never trust a client-provided tenant header on the telephone ingress.

## GPT-Live mode

`VOICE_MODE=live` gives each call one OpenAI GPT-Live session (`wss://api.openai.com/v1/live/sessions`), and no local VAD, ASR, LLM or TTS runs. The model owns turn-taking, wording and when to yield to a barge-in. The gateway still owns admission, lease fencing, transport writes, real-time pacing, playback credit, history checkpoints and cleanup.

- **Billing and capacity.** GPT-Live is billed per session second, plus the delegated backend model (`LIVE_DELEGATE_MODEL`). OpenAI documents concurrent-session limits per account tier; set `MAX_SESSIONS` and `TENANT_SESSION_LIMIT` below them.
- **Audio before the session starts.** The gateway reads caller audio from the first frame but drops it until `session.started`. Reading late would turn a slow session start into a burst that the ingress rate budget must reject.
- **Greeting.** GPT-Live does not reliably speak first. A greeting only in the start instructions was often skipped, and `session.instructions.append` (the documented route) was answered after 3.7–12.7 s or not at all. The gateway therefore sends `session.commentary.append` once the session starts, falls back to the instruction after `LIVE_GREETING_RETRY_SECONDS`, and counts `live.greeting_retry` and `live.greeting_unanswered`. Commentary may paraphrase, so the session instructions require any greeting to disclose the AI. In the final round of 11 calls, 10 greetings arrived about 2.5 s after answer and one session never spoke first.
- **No `clear`.** Output arrives as continuous real-time audio, silence included, so at most one playback window is queued locally and nothing stale needs clearing. How fast speech stops after a barge-in is the model's behavior; compare it with the cascaded pipeline before choosing a mode.
- **Failure.** If the live service is unreachable, the caller hears the bundled unavailable prompt. Watch `live.closed.<reason>`, `live.failure`, `live.error_event` and the `live.billed_seconds` histogram. `scripts.doctor --probe` checks that the live and delegate models are available to the key.

## Redis and recovery

Run the cell Redis on a private endpoint with ACLs, TLS where needed, noeviction, memory alarms, backups and a tested persistence policy. Shared keys use one cell/tenant hash tag; no audio frame is written to Redis. A tenant's metadata mutations serialize briefly in Lua. This is not an unlimited quota index or an automatic global Redis Cluster routing client.

Durable checkpoints are optional (`PERSIST_HISTORY=false` by default). With true, only bounded user text and playback-supported assistant phrases are stored. Set retention intentionally and protect storage/backups. A final uncheckpointed fragment or unconfirmed phrase can be lost on process death. Recovery restores conversational context, not media buffers, ASR decoder state or exactly the same voice stream.

Lease safety assumes the store does not roll back its authoritative state. Redis asynchronous replication/failover can lose acknowledged writes. On a suspected rollback/failover, **stop admission and media in the affected cell, wait for old leases/absolute media credentials to become unusable, and reopen with a new cell/signing namespace after reconciliation**. Do not fail over into a second independent Redis and claim both are authoritative. This implementation does not turn Redis into consensus or promise active-active zero-loss control.

If a gateway cannot renew before its local monotonic deadline, it fails closed. This can end otherwise healthy audio during a control outage; that is the explicit availability-versus-ownership choice. Transport queues still in an OS or carrier cannot be unsent; the write guard prevents newly authorized stale media, not time travel.

## Capacity and overload

Set process MAX_SESSIONS below the measured call/SLO boundary. TENANT_SESSION_LIMIT counts unexpired reserved and active slots per cell, not merely accepted WebSockets. Reservations expire rather than leak forever. Keep grant duration short and rate-limit authenticated create attempts at the edge.

ASR_CONCURRENCY, LLM_CONCURRENCY and TTS_CONCURRENCY are per-gateway dependency limits; they do not automatically reserve provider-wide capacity. Worker admission and measured service capacity remain authoritative. Multiple gateways must not multiply a fixed provider quota accidentally. Circuit breakers fail fast and permit one half-open probe. They do not retry partial spoken answers. Test provider timeout, cancellation and recovery at realistic occupancy.

Hosted TTS latency shapes the speech, not only time-to-first-audio. With `TTS_LOOKAHEAD=0` the next phrase is requested only after the current one is queued, so its first-byte time becomes silence mid-reply. `TTS_PREBUFFER_MS` trades a later start for no stutter inside the phrase. `TTS_HEDGE_SECONDS` spends a duplicate request to escape a stalled stream; every launch logs `{"type":"tts_hedge"}` on `duplex.resilience`. A failed turn logs its exception class names, never messages, on `duplex.session`.

Inspect `audio_pipeline_stage_seconds` histograms by stage and `audio_pipeline_events_total` by event. Aggregate histogram buckets across replicas before calculating percentiles; do not average per-pod p95. Active/draining gauges, loop lag, control failures and queue overflows are distinct signals. Keep tenant/call IDs out of metric labels. A bootstrap CPU HPA is not voice-SLO autoscaling: add active calls, event-loop lag, model queue time, dependency quota and failure headroom.

The sample cell's single Redis is intentionally not HA. The Kubernetes image, Secret, PVC, TLS ingress, network policies, Redis topology and model URLs are operator-provided. Validate deployment manifests with your cluster version and admission policy. Generic HTTP load balancing does not solve WebRTC UDP/ICE/TURN routing; those templates disable RTC.

## Draining and rollouts

With an operator token in the gateway environment, `python -m scripts.drain` calls the local drain endpoint. The server immediately refuses new calls, becomes unready, then waits DRAIN_SECONDS for active calls. Remaining calls are canceled and their guarded finalizers complete within additional control/I/O bounds. A 30-second drain can intentionally cut off a longer conversation; choose a business-appropriate window and a larger orchestrator termination grace. The templates reserve 50–60 seconds for the 30-second drain plus cleanup; that is a reference budget, not universal availability.

Roll out a small canary, compare correct task completion and turn timings under matched channels, then expand. Do not delete a gateway while it owns live calls and call the system stateless. A crash requires an authenticated explicit reconnect after expiry; clients must not play cached audio into the new call. No lossless media migration is promised.

## Telephony and outbound effects

Expose Twilio HTTP/WebSocket endpoints through the exact HTTPS PUBLIC_BASE_URL used for signature validation. Preserve required headers across proxies. Keep validation enabled. Verify real `start`, media, marks, clear, stop, DTMF and disconnect against a test number. Test 8-kHz mu-law degradation separately from wideband browser audio.

Keep AudioSocket behind an authenticated PBX/network boundary with strict source CIDRs; do not expose port 9092 to the Internet. Asterisk/SBC owns SIP registration, trunks, RTP, encryption and carrier interoperation. The source does not implement a mobile network, SS7/IMS integration, emergency routing or lawful-intercept/regulatory functions. Deployment compliance requires local expertise, not a repository claim.

Before a carrier test, run `scripts.pbx_lab` against the loopback Asterisk lab on both routes. On 7001 the call reaches AudioSocket. On 7002 it reaches the gateway's Twilio webhook and Media Streams path through `scripts.twilio_emulator`. A reactive synthetic caller speaks, waits, barges in and times what comes back on its own media leg. The emulator follows Twilio's public protocol, but it does not prove Twilio interoperability. Real Twilio still needs a signed request from Twilio itself, including the exact stream-URL signing form, plus marks over the real network and a test number. For a human check, register a LAN softphone (see `deploy/asterisk/README.md`) and dial either route.

OUTBOUND_ENABLED defaults false and is forced false in replicated gateway templates. The supplied SQLite DialStore protects one host, with explicit unknown outcomes rather than blind retries. **Use a dedicated single-host dialer with a durable volume**, or replace it with a reviewed distributed effect ledger before multi-host outbound use. All outbound requests require operator authentication, an exact destination allowlist, consent_confirmed=true and an idempotency key. A timeout after provider acceptance must be reconciled, never automatically redialed. This release made no paid calls.

## Live acceptance gate

Run a labeled English corpus with names, digits, corrections, hesitation, interruptions and multi-turn references. Evaluate browser headphones, speakerphone and telephone channels separately. Record raw audio only with appropriate notice/consent and retention controls. Measure actual last-user-sample to first-rendered-agent-sample, interruption-to-silence, intelligibility, task success and entity/correction accuracy. Include slow networks, jitter, loss, reconnect, control outage, rolling restart and a model provider failure. Set targets before the test, repeat at multiple concurrency levels, and report the full environment. The research ledger explains why one latency number or one WER cannot certify conversational quality.
