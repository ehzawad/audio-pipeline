# Application, media and worker contracts

## Two credentials, two purposes

The trusted application backend issues an HS256 identity JWT with `tenant`, `sub`, `iss`, `aud`, `iat`, `exp`. Audience is `audio-pipeline:<CELL_ID>`; maximum lifetime is 300 seconds. The gateway verifies the fixed algorithm, issuer, audience, required claims and tenant format. `APP_TOKEN` is an operator/service credential, not a mobile distribution mechanism. `scripts.issue_identity` is a development helper for the trusted backend, not an anonymous signing endpoint.

`POST /v1/sessions` takes `Authorization: Bearer <identity>`, `Idempotency-Key: <1–128 character opaque key>`, and `{"transport":"websocket"}` or `webrtc`. The response is `{session_id,ticket,grant_expires_at,state}` plus authenticated ICE configuration for WebRTC. The grant has an absolute server-time expiry; callers must not interpret replayed creation as a fresh 60-second allowance. Same key/subject/transport replays the existing reservation; conflicting transport or closed/expired state is explicit. Identity is tenant/subject scoped. CORS is not a substitute for authentication; browser Origin must be allowlisted when present. Native clients may omit Origin with valid credentials.

`GET /v1/sessions/{sid}` returns scoped status, bounded metadata events, revision and effective state. `DELETE` requests stop; active ownership transitions to stopping and the worker notices through its next heartbeat. `POST /reconnect` issues a fresh grant only after the old reservation/lease expired. Completed/stopped calls are not silently redialed. A new user call needs a new idempotency key. Grant and identity are cell-bound: the trusted backend must direct the client to the correct home-cell endpoint.

The HMAC media grant binds tenant, subject, session, transport, grant version, incarnation and cell. A successful claim consumes it by state transition; expiry is rechecked atomically by the control backend. A new incarnation prevents old credentials matching a recreated record after retention. Identity JWTs cannot be supplied as media grants and media grants cannot authorize HTTP control.

## PCM WebSocket

Connect to `/ws` with WebSocket subprotocols `duplex` and `<ticket>`; the server selects only `duplex`. Never put credentials in query strings or logs. Client first sends `{"type":"hello","sample_rate":16000}`. Input is mono little-endian signed PCM16, with no WAV header. Supported input rates: 16000, 24000, 32000, 44100, 48000 Hz. Each binary message contains at most 100 ms and an even byte count. Send at real-time cadence; a bounded jitter allowance is not permission to upload a long recording instantly. Too-old/too-fast media ends the session rather than accumulating unlimited latency.

Server audio messages have an 8-byte header followed by PCM16 at 24 kHz:

| Offset | Type | Meaning |
|---|---|---|
| 0 | uint32 little-endian | Response epoch, positive |
| 4 | uint32 little-endian | Packet sequence within epoch, starts at 1 |
| 8 | int16 little-endian array | Mono PCM samples; last packet may be shorter |

After the renderer has completed a contiguous prefix, send `{"type":"played","epoch":E,"seq":S}`. Receipt of the network packet is NOT render completion. All bundled client cursors validate monotonic sequences, ignore old epochs, and track out-of-order completion until the prefix closes. A clear event must stop current sources, empty unscheduled audio, reset the cursor to the new epoch, and prevent stale completion callbacks sending new receipts. Do not ACK stopped sources merely because an `onended` callback fired. The browser implementation handles this distinction.

Playback credit is bounded by `PLAYBACK_WINDOW_FRAMES`; default 25 ordinary 20-ms frames is roughly 500 ms of unacknowledged audio in the application path. This is not a cap on unknown OS/speaker/network buffers. A timed-out receipt aborts output. A receipt says the renderer reports completion; it does not prove the user physically heard, understood, or acknowledged the content.

Send `{"type":"hangup"}` to end. Partial transcripts, committed user events, response state, timing and errors are JSON control events. Do not persist transient partial transcripts as final user facts. The fixture `clients/fixtures/protocol.json` is used by Python/JS/Swift/Kotlin checks. The Kotlin runner consumes an equivalent generated TSV, not a separately maintained binary convention.

## WebRTC

Obtain a `webrtc` grant and POST an SDP offer plus ticket to `/api/rtc/offer`. The gateway holds a local setup slot, negotiates, then atomically claims the grant. The aiortc audio path is optional. ICE/TURN reachability, UDP firewall policy, IP advertisement, regional media placement and native-client audio session integration remain deployment responsibilities. The generic cell/Kubernetes HTTP templates disable RTC deliberately. Playback completion is estimated here; there is no sample-exact physical handset receipt. WebRTC support is not a claim of acoustic echo cancellation implemented by this repository.

## Twilio and Asterisk

`POST /twilio/voice` validates the signature against the configured PUBLIC_BASE_URL, binds AccountSid when configured, and creates a call-specific media grant in TwiML custom parameters. `/twilio/stream` validates the WebSocket signature and consumes the ticket only after validated `start` metadata. The stream is 8-kHz mu-law/base64 JSON. Outbound media is paced; marks identify epoch/sequence. Invalidate the epoch before clear so the clear-triggered return of old marks cannot promote discarded speech into history. DTMF input events are surfaced, not interpreted as arbitrary tool authority.

AudioSocket stays a private TCP interface with standard UUID/audio/DTMF/hangup framing. The peer must satisfy configured CIDRs. It is not encrypted or Internet-authenticated by inventing a protocol opcode. The PBX owns the SIP/RTP leg; the gateway continuously resamples and packetizes 20-ms PCM. Completion is a pacing estimate, not a handset acknowledgment. TCP stalls and a final partially sent packet cannot be revoked.

## Private inference workers

`/asr` uses a bearer service token, PCM16 at 16 kHz in bounded chunks, and `{"type":"finish"}`. Each utterance owns a distinct stream; responses are `partial`, `final`, or a generic error. `/tts` takes bounded JSON text and returns PCM16 at 24 kHz with rate/format headers. TTS is phrase-based. `SPEECH_ROLE=asr` disables TTS; `tts` disables ASR. Internal service credentials never reach the end client. A mesh/private TLS boundary is needed outside a trusted host/network.

Errors are bounded and do not expose credentials or transcripts. HTTP session-control overload uses 503/Retry-After; state conflicts use 409; missing tenant-scoped state uses 404. WebSocket handshake rejection does not provide a portable rich JSON error. Clients should inspect authenticated session status, not blindly replay a consumed ticket or continuously redial.
