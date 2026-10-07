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
