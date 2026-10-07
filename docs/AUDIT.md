# V2 to v3: implementation delta

The predecessor ZIP is the comparison baseline. The remote `main` inspected at the start contained only `.gitignore`, so this release publishes the previously delivered voice code together with the v3 changes; it does not pretend there was already a populated v2 Git history.

| Boundary | Previous baseline | Current mechanism / code |
|---|---|---|
| Cross-worker grants | Process-local ticket ownership | Cell/tenant/subject-bound HMAC grant + shared CAS consumption (`control/service.py`) |
| Active owner | Local call/task identity | Store lease, monotonic fence, local monotonic write guard (`control/runtime.py`) |
| Retry and recovery | No shared reconnect/checkpoint contract | Idempotent reservation, explicit expiry/reconnect, incarnation-safe credentials |
| Conversation state | Bounded in-process history | Optional bounded checkpoints, owner/revision fencing; default no transcript persistence |
| Store implementation | In-process controls | SQLite single-host reference + Redis atomic scripts, same-slot key layout, script cache reload |
| Input time | Frame sizes and finite queues | Audio-seconds rate budget and separate control-message rate budget |
| Output time | Receipts and epoch invalidation | Finite unacknowledged playback credit, timeout and interruption wakeups |
| Dependencies | Adapter timeouts | Independent fail-fast bulkheads and generation-aware circuit breakers |
| Inference placement | Combined speech worker | ASR-only, TTS-only or combined role; separate gateway URLs |
| Rollout | Basic task shutdown | Admission drain, backend health, tracked/fenced finalization before socket disposal |
| Observability | Local JSON summaries | Prometheus histograms/counters with bounded labels, event-loop lag |
| Mobile integration | Browser source only | JS socket/control client; Swift control/protocol package; Kotlin protocol core; shared fixtures |

The v2 streaming recognizer, phrase TTS, transport-specific clear, codecs, per-call VAD, native compute ownership, and original mock tests were retained. Architecture is not accuracy: no paired ASR WER, TTS MOS, live PSTN reliability or GPU latency improvement is claimed. Redis/PyAV/model-dependent tests are individually labeled in the verification record.

This release intentionally does not implement semantic endpoint prediction, conversational backchannel classification, native end-to-end audio models, human-agent transfer workflows, global cross-cell quotas, consensus failover, a distributed outbound effect ledger or finished mobile audio/UI applications. Those omissions are not hidden behind additional empty microservice directories.
