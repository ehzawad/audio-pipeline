# Research and source ledger — 7 October 2026

These are primary sources, not promotional rankings. Paper observations inform evaluation requirements; implementation decisions below remain this repository's engineering choices. No listed benchmark was reproduced, no reported model score is a repository score, and no source certifies fleet capacity.

## Research that changes the evaluation target

**R1. Full-Duplex-Bench-v2 — ACL 2026 Short Papers, pp. 27–36.** Lin et al. evaluate multi-turn full-duplex systems with an automated examiner, fast/slow pacing, and Daily, Correction, Entity Tracking and Safety tasks. Their evaluation separates conversational timing from maintaining context and fulfilling goals. Methods and limitations were read from the paper, not only its abstract. This motivates preserving bounded confirmed conversation state and testing corrected entities after interruption. The shipped deterministic tests validate state boundaries; a real-model semantic evaluation remains required. Examiner and transcription/judging behavior are themselves measurement assumptions.

https://aclanthology.org/2026.acl-short.4/
https://aclanthology.org/2026.acl-short.4.pdf

**R2. FD-Bench — Interspeech 2025, pp. 176–180.** Peng et al. assess interruptions, response delay and robustness under difficult conversational conditions. The full paper distinguishes different interruption intents and reports event-oriented measures. Our engineering consequence is to test cancellation, stale audio/receipts, completion ordering and controlled provider failure separately. Those tests are not a substitute for measuring successful recognition of an affirmative backchannel versus a denial or topic change. The current VAD-based interruption rule cannot make that semantic distinction.

https://www.isca-archive.org/interspeech_2025/peng25b_interspeech.html
https://www.isca-archive.org/interspeech_2025/peng25b_interspeech.pdf

**R3. Analysing the role of lexical and temporal information in turn-taking through predictability — EACL 2026 Long Papers.** The paper studies how lexical and temporal information contributes to turn-taking predictability; its analysis does not make silence or punctuation an infallible completion signal. Our consequence is explicit labeling: the implemented endpoint is a VAD/silence policy, not a learned semantic end-of-turn model. Changing the ASR streaming interface alone does not implement prosodic prediction. Methods/conclusions were read; no trained turn model was imported.

https://aclanthology.org/2026.eacl-long.283/
https://aclanthology.org/2026.eacl-long.283.pdf

## Engineering sources and the exact boundary they justify

**E1. Redis Lua evaluation.** Redis executes scripts atomically relative to other commands and provides a script cache. This informs `control/backend.py`: short, explicit-key scripts; Redis TIME; bounded session metadata; EVALSHA via registered scripts; reload after NOSCRIPT. Atomic execution is not a claim that asynchronous failover cannot lose acknowledged state. No every-frame audio or unbounded history belongs in Lua.
https://redis.io/docs/latest/develop/programmability/eval-intro/

**E2. Redis Cluster specification.** Hash tags colocate related keys. The implementation hashes cell and tenant into the tag so session, quota and checkpoint keys share a slot. It uses a cell-scoped Redis endpoint, not an automatic RedisCluster client. Asynchronous replication/failover and state rollback remain operational hazards; do not describe the lease as a consensus service.
https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/

**E3. LiveKit server lifecycle/options.** Official lifecycle documentation distinguishes admission/capacity, job ownership and graceful draining. This supports the design separation in `control/runtime.py`; it does not mean this gateway uses LiveKit's dispatch or worker process implementation. Redis control loss fails this runtime closed rather than silently migrating a live audio stream.
https://docs.livekit.io/agents/server/lifecycle/
https://docs.livekit.io/agents/server/options/

**E4. Twilio Media Streams.** The wire contract is base64 raw mu-law at 8 kHz, with marks and clear. A mark returned after clearing buffered media is not necessarily evidence the audio played. Epoch invalidation and mark handling live in `transports/twilio.py` and `playback.py`. Bidirectional streams and DTMF have documented limits; no generic outbound DTMF, human transfer or carrier feature is invented here.
https://www.twilio.com/docs/voice/media-streams/websocket-messages
https://www.twilio.com/docs/voice/media-streams
https://www.twilio.com/docs/api/errors/31931

**E5. Asterisk interfaces.** AudioSocket remains the implemented private TCP interface. Asterisk's newer WebSocket channel driver documents a richer media/control path, but that is an evaluated extension opportunity, not an implemented adapter or a reason to call AudioSocket Internet-safe. SIP and carrier trust still terminate at a PBX/SBC.
https://docs.asterisk.org/Configuration/Channel-Drivers/AudioSocket/
https://docs.asterisk.org/Configuration/Channel-Drivers/WebSocket/

**E6. Identity and mobile transport APIs.** PyJWT's API supports explicit algorithms, required claims and audience checking; the implementation pins HS256 and a cell-specific audience. Apple's URLSession/WebSocket APIs provide native transport primitives. Neither source supplies microphone lifecycle, echo control, audio-session interruptions or handset playback evidence automatically.
https://pyjwt.readthedocs.io/en/stable/api.html
https://developer.apple.com/documentation/foundation/urlsession
https://developer.apple.com/documentation/foundation/urlsessionwebsockettask

## Model provenance retained from v2

The local path uses an English streaming recognizer rather than utterance-only Whisper. That is an interface/overlap decision, not proof of lower word error rate. Model IDs are configuration/provenance, not novelty claims.

- Streaming ASR: `csukuangfj/sherpa-onnx-streaming-zipformer-en-2023-06-26`; sherpa-onnx online transducer documentation:
  https://k2-fsa.github.io/sherpa/onnx/pretrained_models/online-transducer/zipformer-transducer-models.html
  https://huggingface.co/csukuangfj/sherpa-onnx-streaming-zipformer-en-2023-06-26
- VAD: Silero ONNX, separate recurrent state per call:
  https://github.com/snakers4/silero-vad
- Phrase TTS: `hexgrad/Kokoro-82M`, not causal token-to-waveform streaming:
  https://huggingface.co/hexgrad/Kokoro-82M
- Example text LLM launcher: `Qwen/Qwen3-4B-Instruct-2507`; independently served, operator replaceable:
  https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507
- Hosted ASR/TTS contracts: Deepgram streaming utterance close and ElevenLabs streaming synthesis:
  https://developers.deepgram.com/docs/close-stream
  https://elevenlabs.io/docs/api-reference/text-to-speech/stream

For deployment, inspect exact model licenses and resolved revisions in your manifest. Hosted APIs are not open weights merely because another stage uses Hugging Face. The emergency prompt is locally generated synthetic eSpeak speech; see NOTICE.md.

## Evidence hierarchy

A paper can motivate a failure class. A protocol document can define a wire obligation. A source file can show a mechanism. A deterministic test can exercise a particular schedule. Only a workload-specific live measurement can establish speech quality, real handset latency, deployment safety or fleet capacity. This release deliberately keeps those levels separate.
