# The Conversation Clock, Volume II
## Ownership, state and scale — the Audio Pipeline 3.0 guide

The first version explained a relay: listen, transcribe, think, speak, interrupt. This volume explains what happens when the relay is distributed across processes, devices and a network that does not share one clock. Read the [protocol](PROTOCOLS.md) beside the code and the [source ledger](SOURCES.md) beside the research claims.

### 1. A call is not an HTTP request

A request is often finished after its response body leaves a server. A voice session can own a microphone, decoder state, generated text, unplayed sound and dialogue memory simultaneously. Ending one of those objects does not automatically end the others. A caller can hear the tail of a response long after the LLM stopped generating. A canceled coroutine can leave native inference running. A TCP disconnect can leave a shared record claiming the session is active until its lease expires.

The design question is therefore not just “which service handles this request?” It is “which owner may mutate each piece of state, and what event ends that permission?”

`main.py` handles public wire contracts. `control/service.py` defines legal state changes. `control/backend.py` closes races atomically. `control/runtime.py` owns the live task tree. `session.py` owns turn and response semantics. Transport adapters own codec and playback capabilities. Splitting these responsibilities makes a change reviewable: a new telephone adapter must not quietly invent a new tenant identity rule.

### 2. Four kinds of state

**Identity state** says who is calling and in which tenant/cell. It comes from the trusted application issuer or a configured telephone edge, not a model prompt or arbitrary header.

**Control state** says a session is reserved, active, stopping or closed; who owns it; when its permission expires; and which checkpoint revision is current. This is shared across gateways. Small CAS transactions protect it.

**Media state** includes resampler memory, VAD state, partial ASR decoder state, PCM queues, response epoch and packet sequence. It lives with one current gateway owner. Writing this on every frame to Redis would introduce a remote dependency into the most timing-sensitive path.

**Conversation state** contains a bounded sequence of user messages and assistant phrases supported by the transport's playback evidence. It can be checkpointed at meaningful boundaries, with explicit privacy/retention rules. It is not a database of business truth or proof a user understood a statement.

The first practical invariant follows: shared control should be small and infrequent; live media should be local and bounded.

### 3. Reserving is different from consuming

Imagine two load-balanced requests. The phone application asks gateway A to create a session, but its WebSocket lands on B. A process-local ticket store fails here: B has never seen A's ticket.

The v3 reservation lives in the control backend. It counts against the tenant limit before media connects. The signed grant identifies that reservation, but possession alone is insufficient: the store must atomically change reserved to active. Two simultaneous claims cannot both succeed at the same version.

```text
identity + idempotency key
             |
          reserved ---- expires ----> effective expired
             |
       one atomic claim
             v
           active ---- stop request ----> stopping ---- finalizer ----> closed
             |
         lease expires
             v
      effective disconnected ---- authenticated reconnect ----> reserved
```

“Effective disconnected” is derived from the store clock, not an immediate background rewrite. This avoids requiring a global sweeper to decide whether every record is alive. Reservation and lease expiry also free quota when the next atomic admission operation removes stale slot entries. No abandoned WebSocket has to call a perfect cleanup hook for quota to recover.

### 4. A lease is a borrowed permission

Suppose ownership is valid for nine seconds and renewal runs every three seconds. A successful renewal says the current owner may keep working for a bounded interval. If the store stops answering, the gateway does not assume success. Its local monotonic guard eventually refuses output.

Why a local monotonic clock? Wall clocks can be adjusted. More subtly, the server's lease response can arrive late. Extending local permission from response-arrival time would accidentally add network delay to the lease. `LeaseGuard` therefore starts its budget at the beginning of the control request and subtracts a margin. This is conservative: a slow response may shorten useful permission. It does not lengthen permission beyond the acknowledged budget.

The guard is checked near transport writes, including queued WebRTC track frames. A check only at the beginning of a long response would be insufficient: its lease might expire while TTS is still producing audio.

This contract cannot revoke packets already in an OS/carrier buffer. It controls new writes. Nor does it survive arbitrary rollback of the authoritative datastore. Those are distinct boundaries, not defects solved by renaming a lock a “lease.”

### 5. Fences distinguish an old owner from a new one

An old process can wake up after a pause and attempt a checkpoint. An owner identifier alone is useful but does not express succession. Each successful ownership claim also increments a fence.

Owner A has fence 7. Its lease expires. A reconnect gives owner B fence 8. A late “finished” write from A must be rejected, even if its text looks valid and it used the correct session ID. The store checks both owner and fence against current state before changing history or releasing capacity.

A second version number handles ordinary concurrent mutations: heartbeat, stop and checkpoint may all read the same document. CAS lets one write win; the others reread and apply their operation to the new state. The fence answers “which generation of owner?” The document version answers “which exact revision of shared state?” They are not interchangeable.

The media **epoch** is a third counter. It changes within one live owner whenever a response is invalidated. It does not replace the ownership fence. Keep the three meanings separate: owner succession, shared-state revision, response succession.

### 6. Why credentials need an incarnation

An idempotency key deterministically selects a session ID for a tenant and subject. After retention expires, that ID can be created again. Without an incarnation, a very old signed grant could match a newly created record whose grant version restarted at one.

The new record carries an unpredictable incarnation. Every media grant binds it. A recreated session can share a deterministic ID while being a different lifetime. Old signed bytes no longer match. Cell binding adds a separate boundary: even a correctly signed grant from another cell is rejected. The trusted backend must still enforce home-cell assignment; a namespace is not a global routing service.

### 7. An interruption is a cut across several queues

The caller says “Actually, make that Thursday” while the bot is saying “Your booking is Tuesday...” The old response may simultaneously exist as LLM tokens, a queued phrase, native TTS work, dispatched packets and a partially rendered sound.

Stopping the LLM alone does not stop the queued sound. Clearing the sound alone does not stop a late TTS result from refilling the queue. Updating history first can falsely record words that were never rendered.

The runtime invalidates the response epoch, cancels producers, clears transport-supported buffers, and finalizes only the playback-supported prefix. Late writes and receipts from the old epoch cannot advance the new response. The task remains alive while output is still outstanding. Barge-in after generation but before rendering must still clear the tail.

Twilio is particularly instructive: clear can return pending marks for audio discarded from its queue. The epoch invalidation prevents treating those marks as proof that the caller heard that audio. This follows the documented wire semantics, not a timer guessed from a demo.

### 8. Playback evidence has a strength

For WebSocket PCM, a cooperating renderer reports completed packets. For Twilio, marks acknowledge progress under that platform's semantics, with clear handled separately. For Asterisk and the optional WebRTC adapter, completion is estimated from paced delivery rather than known from a handset speaker.

Even the strongest of these is not physical hearing or comprehension. A muted device can report rendering. A user can be distracted. The ledger's job is more limited: do not claim a complete assistant phrase was delivered when available media evidence contradicts that claim.

The implementation stores complete phrases whose final packet is acknowledged or estimated complete under the adapter contract. It does not proportionally map audio duration to words. That conservative policy can omit a phrase the caller partially heard. It avoids inventing a precise word boundary without alignment information.

### 9. Backpressure is also a time policy

A finite queue stops memory from growing indefinitely. But real-time systems also need to stop time from growing indefinitely. A client can fill a queue with an hour of old microphone data even when individual packets are valid.

The ingress rate budget is measured in audio seconds. At steady state the client can deliver slightly more than one audio second per real second, with a small burst allowance for jitter. Exceeding it ends an invalid/stale stream rather than moving old speech toward a current answer. The allowance is a trade-off: a very late TCP burst may be rejected rather than played much later.

On output, playback credit limits outstanding unacknowledged packets. At the default 25 ordinary 20-ms packets, the application has roughly half a second of outstanding audio. This does not prove the whole Internet/device path is bounded by half a second. It bounds the part the application controls and can observe.

Phrase, ASR and network queues have separate bounds because they store different units. Do not use a queue length of 100 to mean 100 tokens, 100 samples, 100 phrases and 100 seconds interchangeably.

### 10. Native compute does not obey coroutine cancellation

A Python waiter can be canceled while an ONNX or synthesis call continues in a native thread. Releasing its compute slot immediately would admit new work while the old work still consumes the resource. A stream of interruptions could silently defeat the concurrency limit.

`ModelRunner` retains that slot until the actual native job completes. The canceled caller stops waiting and discards the result, but capacity accounting stays tied to compute reality. This does not make the native model preemptible. It makes a non-preemptible component honest.

Similarly, each dependency's circuit breaker is scoped separately. A failing TTS provider must not be represented as an ASR failure. Cancellation is neutral rather than counted as a provider outage. A half-open circuit admits one probe. A late success from a request belonging to an older circuit generation cannot close a newly opened breaker. No partial spoken response is replayed as an automatic retry.

### 11. Checkpoints are snapshots, not event sourcing

A checkpoint queue contains the latest bounded history snapshot. If a newer snapshot arrives before an older one is written, it replaces the pending snapshot. That is correct because the newer snapshot subsumes the older one. It would be wrong for an append-only business event queue.

A checkpoint write requires current owner/fence and a newer history revision. Finalization cannot replace newer restored history with an empty snapshot at the same revision. Shutdown owns one finalizer, which joins heartbeat/checkpoint tasks, attempts guarded completion, then closes the transport and releases local capacity. A second route exception must not race that finalizer and prematurely release its slot.

`PERSIST_HISTORY` defaults false. With it true, a crashed process can leave a gap after the latest completed checkpoint. Recovery starts fresh media with that last bounded context. This design does not promise exactly-once speech or seamless continuity; it gives an explicit, inspectable recovery point.

### 12. Microservices should separate resource ownership

A separate folder is not a microservice. In this repository, separate speech roles are actual independently launchable workers. An ASR worker loads a streaming recognizer; a TTS worker loads a phrase synthesizer. The gateway can target different service URLs. The LLM is an independent server from the outset.

This permits different scaling and hardware choices. It also introduces network failures and authentication. The speech token is therefore private, response formats are explicit, and each stage has its own timeout/bulkhead. A provider's global quota does not grow just because more gateway replicas exist. Admission at the worker and the configured per-gateway limits must be sized together.

The combined speech role remains useful on one development host. More deployments are not inherently better. They are justified when they isolate independently scarce resources or failure domains.

### 13. A million is not a workload specification

Start with an ordinary example. If twelve calls arrive per second and each lasts three minutes, roughly 2,160 calls are active on average. Arrival rate multiplied by duration gives occupancy. Now make the assumptions explicit rather than saying “million requests.”

One million sessions per day divided by 86,400 seconds is about 11.574 new sessions per second. With a 180-second mean duration, average occupancy is about 2,083. A threefold peak arrival multiplier gives 6,250 active sessions under this simplified model. A million simultaneous sessions is about 160 times that peak occupancy, not another spelling of the same requirement.

Each active duplex 20-ms packet path can process about 100 packet events per second across both directions. At 6,250 calls, that is about 625,000 events per second before any additional control messages. A three-second heartbeat produces roughly 2,083 lease renewals per second; the current backend uses a read script and a CAS script per renewal, plus occasional contention retries. Frames must not become Redis transactions.

`scripts.capacity` performs this arithmetic and labels every assumed input. A supplied 100-call gateway capacity with 60% target utilization implies 105 gateway replicas before explicit failure reserve. That is not a measured capacity claim. The model's queues, ASR/TTS real-time factor, carrier quotas, bandwidth, burst distribution, regional skew and cost still require measurement.

### 14. A cell limits blast radius; it is not magic global consistency

A cell contains a gateway group, a control store, inference capacity and an admission budget. The application issuer chooses a home cell. Credentials bind that choice. One failing cell should not force every other cell's media to synchronize through its store.

There is no global scheduler or cross-cell quota ledger in this release. A tenant allowed into several cells could consume a quota in each unless the issuer applies a global policy. A Redis failover that rolls back state is a separate hazard: the system must quiesce and reconcile the affected cell instead of allowing old and new owners to proceed independently.

Call-local media state remains on one gateway for the life of the connection. Scaling adds admission capacity for new calls; it does not automatically migrate active calls. Scale-down must drain.

### 15. Web, mobile and telco are different edges

The browser reference owns AudioWorklet capture/rendering. The JavaScript SDK supplies a transport core with a renderer completion callback. Swift supplies typed control and protocol primitives; Kotlin supplies the packet and playback cursor core. Device audio sessions, interruptions from another app, Bluetooth routes, audio focus and speaker echo behavior remain platform work. A successful Linux Swift test proves protocol/control compilation, not an iPhone audio session.

Twilio owns a hosted phone-media leg. Asterisk owns a PBX leg. The gateway does not implement a carrier core, IMS, SS7, emergency routing, enterprise call distribution or recording compliance. Saying “telco integration” here means interoperating with a properly operated PBX/provider boundary using the implemented protocol. That is useful without pretending it is a telephone network.

### 16. Research should change the failure experiment

ACL 2026 Full-Duplex-Bench-v2 makes multi-turn corrections and entity tracking visible alongside timing. Interspeech 2025 FD-Bench makes interruption behavior and noisy conditions explicit. EACL 2026 turn-taking analysis warns against assuming one information source explains turn boundaries. See the source ledger for paper-specific scope and limits.

For this implementation, the corresponding verification ladder is concrete: prove stale owners cannot write; prove stale response audio cannot play as a new epoch; prove history never includes an unconfirmed phrase; prove a corrected entity survives a reconnect at the checkpoint boundary; then evaluate real recognition, semantic correction, backchannels and audible timing on labeled calls. The current deterministic tests cover the software contracts, not all those live semantic outcomes.

A useful release record must contain model IDs/revisions, runtime versions, channel/codec, concurrency, interruption schedules, recording/consent policy, timing definitions and failure counts. A fake-model p95 is a transport result. A real corpus WER is recognition fidelity. Neither alone is task success.

### 17. The smallest useful operational loop

Measure at a known workload. Find the first violated boundary. Reproduce it with a deterministic schedule when possible. Add a regression test. Change the smallest owning component. Re-run both the software suite and the relevant real-channel evaluation. Roll out to a canary cell with a drain plan.

That loop is the point of the architecture. More state management is not more dictionaries and flags. It is a smaller set of named facts, each with one owner, one clock, one legal transition and an observation that could show the design is wrong.
