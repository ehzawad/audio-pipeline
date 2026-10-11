# Release verification — Audio Pipeline 3.0

Executed locally on 7 October 2026. See `environment.json` for exact Python/package versions and `tests.txt` for the test result. This record is not a live carrier, GPU, browser microphone, handset or fleet certification.

## Executed

- **135 Python tests passed; six dependency/environment-specific tests skipped.** The suite covers shared state/CAS, quota races, grant reuse/incarnation, tenant/subject/cell isolation, lease deadlines, stale-owner checkpoint rejection, revision ordering, cross-gateway routes, stop propagation, bounded HTTP input, playback credit, native compute cancellation, provider stream cleanup, circuit-breaker races, audio codecs and packet boundaries.
- **32 of 32 synthetic sessions completed over real local HTTP and WebSocket sockets.** Two independent uvicorn processes shared a SQLite file; gateway A created tickets and B consumed media. Both processes returned to zero active sessions. This tests cross-process ownership/cleanup, not Redis performance or real speech. `cross-gateway-load.json` records the count, cleanup state, evidence hash and synthetic receipt boundary.
- JavaScript: **three Node tests passed**. Swift: **three XCTest cases passed**, compiled with Swift 6.2.1 on Linux. Kotlin: shared wire fixtures, contiguous receipts and stale-epoch cancellation checks passed through a compiled JVM executable. These are protocol/control tests, not mobile audio-device tests. `clients.txt` contains compiler/test output.
- Python compilation, browser/SDK JavaScript syntax, YAML syntax and local Markdown links were checked. A wheel was built without dependencies/network; installation on a new device remains environment-dependent.
- Measured **83% line coverage over `duplex_voice`** in the local deterministic suite (`coverage.txt`). This is not proof of correctness and includes unexecuted optional/provider paths. The test count is not a product-quality metric.

## Not executed locally

Four `tests/test_redis_integration.py` cases require `TEST_REDIS_URL` pointing to a **disposable loopback Redis**. They test concurrent quota/claim behavior across clients, lease takeover/fencing, script-cache reload and stop/finalization. No Redis server binary or external download was available in this container. CI is configured with a disposable Redis 8.2.9 service; do not report those tests as passed until an actual workflow result exists.

One optional test needs aiortc/PyAV; one needs real downloaded neural models. The runtime adapters remain source-aligned implementations, not locally executed neural inference. No WER, MOS, GPU latency, sustained real-model concurrency, provider billing or real PSTN results are claimed. Docker/Kubernetes configurations were syntax-read, not deployed here. Native device capture/rendering, AEC, Bluetooth routes, mobile audio focus, TURN reachability and telco interoperability remain live acceptance gates.

## Reproduce

`python -m pytest -q` runs the software suite without weights or keys. Run `python -m scripts.check_clients` with Node, Swift, Kotlin and Java installed for cross-language protocol checks. Set a disposable `TEST_REDIS_URL` to enable Redis cases; one deliberately clears the Redis script cache, so never use a production/shared database. `scripts.load_ws` accepts separate control/media URLs for a configured two-gateway test.

`capacity-example.json` is planning arithmetic with **100 calls/gateway supplied as an assumption**, not a load-test measurement. It must not be presented as proof of million-request throughput. The software supports partitioning by cell; your measured SLO, hardware, provider and network budgets determine useful capacity.

## Update: OpenAI providers, TTS pacing and telephony labs (7 October 2026)

Executed on one Ubuntu 26.04 development host (Python 3.13.5 via uv, Asterisk 22.5.2, Redis 8.0.5, Node 26.3.0). Model calls went to the public OpenAI API from that host's network. Every latency below is specific to that network and day, and a handful of samples is not a provider benchmark.

**Deterministic.** 161 Python tests passed and one skipped (at that point) (the downloaded-neural-model test), with `TEST_REDIS_URL` pointing at a disposable loopback Redis. That makes the four Redis contracts locally executed for the first time; see `tests.txt`. The JavaScript client tests pass 3 of 3. Swift and Kotlin clients are unchanged and were not rerun. The new adapters are tested against local protocol servers and mock HTTP transports. Those tests cover one commit per utterance, rejection of a final for any item other than the committed one, provider errors and closes without a final, the PCM media type and the request body. TTS lookahead, in-order failure after earlier audio, prebuffer, hedging and the cancellation of every attempt are also tested deterministically.

**Live OpenAI provider path.** `scripts.probe` on a 4.5-second synthetic question produced the correct transcript and a sensible two-sentence reply. Through `/v1/realtime?intent=transcription`, the time from end of audio to final transcript was, over 4 runs each:

| Model | Seconds after end of audio |
|---|---|
| `gpt-realtime-whisper` | 0.61–0.64 |
| `gpt-live-transcribe` | 0.66–0.80 |
| `gpt-transcribe` | 0.71–0.94 |
| `gpt-4o-transcribe` | 0.79–0.96 |

The two streaming models emitted partials during speech. A trivial authenticated API request took 1.0–1.3 s to its first byte from this host, and that cost recurs in the LLM and TTS stages. The OpenAI server answered the WebSocket close handshake only after about 2 s, so the adapter no longer waits for it (`close_timeout=0`).

**Measured TTS behavior.** `gpt-4o-mini-tts` PCM streaming sends a 0.20-second head chunk, then usually pauses 0.3–0.8 s. Across the sampled requests, roughly one in ten to one in four stalled for several seconds, either before the first byte or after the head chunk. One stalled past a 60-second read timeout. Before this change that showed up in calls as a failed turn with the fallback prompt, a greeting cut off after 0.2 s, or a 2.3-second pause mid-reply. `TTS_LOOKAHEAD`, `TTS_PREBUFFER_MS` and `TTS_HEDGE_SECONDS` are the responses; the README describes their trade-offs.

**Asterisk loopback lab.** These are three consecutive synthetic calls with the final OpenAI profile; `asterisk-lab.json` holds every number. The path was PJSIP/G.711 RTP on 127.0.0.1, then AudioSocket, then the gateway. All three calls completed without failures:

- 9 replies answered;
- from the caller's last sample to the first heard reply sample, 3.2–5.4 s (median 4.3);
- the interrupted reply stopped 0.56–0.60 s after the caller began barging in;
- the largest pause inside any reply was 0.42 s;
- zero sessions remained afterwards.

The caller hears audio on its own AudioSocket after the RTP loopback, not through a loudspeaker. The loopback path has no carrier jitter, loss or NAT.

**Twilio route, emulated.** No Twilio account was available, so `scripts.twilio_emulator` stood in for Twilio's media edge with lab-only credentials. It played Twilio's part: a signed `/twilio/voice` POST, TwiML, a signed `/twilio/stream` WebSocket with the call-bound ticket, mu-law media, marks echoed at emulated playout, `clear`, and `stop`. Three synthetic calls through Asterisk extension 7002 completed without failures; `twilio-emulated-lab.json` holds every number:

- 9 replies answered;
- caller-to-heard-reply 3.7–6.9 s (median 4.4);
- every barge-in sent a `clear`, and the interrupted reply stopped 0.52–0.64 s after the caller began;
- 108–125 marks were echoed per call;
- zero sessions remained afterwards.

This exercises the gateway's Twilio code with real models. It is not Twilio interoperability evidence: the PSTN, Twilio's network and buffering, and Twilio-issued signatures were absent.

**LAN softphone.** baresip 1.1.0, an open-source SIP client, registered with digest authentication to the opt-in softphone account over this host's LAN address. It dialed 7001 with a recorded question as its microphone. It heard the full greeting and then "The capital of Japan is Tokyo." over RTP on the LAN interface. That is one call made from the same host; a phone on Wi-Fi was not tested here.

**Not executed in this update.** No real Twilio or PSTN call was made, and no handset was used. The browser microphone and WebRTC through TURN were not retested. No WER, MOS or concurrency figure is claimed.

## Update: newest OpenAI models, GPT-Live mode and recorded human callers (7 October 2026, later)

Same host, the same day, and the public OpenAI API. `realtime-matrix.json` holds every number; recordings stay under `data/`.

**Deterministic.** 171 Python tests passed and one skipped. The new tests cover:

- the Responses API adapter: request body, text deltas, error, failure and tool-call refusal;
- transcription session fields per model family (the 4o models reject `languages`; `delay` is `gpt-live-transcribe` only);
- the GPT-Live session against a local protocol server: start config, no audio sent before `session.started`, commentary greeting with instruction fallback, paced output at the transport rate, timestamp-ordered transcript turns, graceful close, and the unavailable prompt.

**Model choice.** The choice was made from OpenAI's model list and current docs, then measured on this host:

- `gpt-5.6-luna` over Responses returned its first token in a median 0.79 s (n=6); `gpt-6-luna` took a median 4.1 s over Responses and 1.7 s over Chat.
- `gpt-6.1-sol` rejects `reasoning_effort: none`.
- Over 12 requests, `gpt-4o-mini-tts-2025-12-15` had one failure and no stalls over 2 s. The unpinned alias had two failures and four stalls of up to 11 s.
- A later sample of eight requests showed one multi-second stall from both the snapshot and `tts-1`, so the TTS stage stays variable.

**Recorded human callers.** 28 recordings come from MInDS-14 en-US (PolyAI, CC BY 4.0, revision 40ce77cb): real customers phoning a bank, G.711 8 kHz, 2 per intent across 14 intents, grouped into 9 three-turn calls.

- `gpt-live-transcribe`, `gpt-transcribe` and `gpt-4o-transcribe` each made 71 edits against the 483 reference words, and disagree with each other on 0.8–2.5% of words. The dataset's own references contain truncations and recognition errors, so this is agreement, not WER.
- `gpt-live-transcribe` finished a median 0.71 s after the audio ended, against 0.85–0.87 s for the other two.

**Phone calls, both pipelines.** Calls went through the loopback Asterisk lab on 7001 (AudioSocket) and 7002 (the emulated Twilio route).

- **Cascaded:** 15 calls placed, all with reports. 45 turns answered, but 3 of the 36 human turns ended in the bundled unavailable prompt after about 7–30 s; their cause was not captured. Caller-to-heard-reply was a median 6.2–6.6 s (p90 8.3–9.4 s). A follow-up profile put TTS at 1.3–12.6 s of that. The interrupted reply stopped 0.42–0.44 s after a barge-in.
- **Live, final greeting method:** 11 calls placed, and the scripted caller gave up after 30 s of silence on one, where both greeting requests went unanswered. Of the other 10 calls, every turn was answered: greeting about 2.4–2.5 s after answer, human replies a median 1.4 s after the caller stopped (p90 4.4 s), barge-in stop 1.4–1.6 s. A further 4-call regression on the final code was clean on both routes: replies 0.02–2.4 s, barge-in stop 1.9–2.8 s.
- **Earlier live rounds:** the greeting failed on 1 of 15 calls with the greeting in the start instructions and 5 of 11 with `instructions.append`; these are kept in the record.

**Browser.** Headless Chromium 153 used a WAV as its microphone. Eight runs passed: both pipelines, WebSocket and WebRTC, the synthetic Tokyo question and a recorded human utterance. Over WebSocket, "completed" requires the page's own render acknowledgments. Cascaded end-of-speech to first dispatch was 4.4–6.8 s.

**Not executed.** No PSTN, carrier or real Twilio call and no handset were used. There was no concurrency test of GPT-Live sessions, and no WER or MOS against verified references.
