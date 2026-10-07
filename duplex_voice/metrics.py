"""No audio, caller IDs, phone numbers or transcript text in default logs/metrics."""
import json
import logging
import time
from collections import Counter, defaultdict, deque
from prometheus_client import CollectorRegistry, Counter as PromCounter, Histogram, Gauge, generate_latest

log = logging.getLogger("duplex.metrics")


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.events = PromCounter('audio_pipeline_events_total', 'Bounded event categories', ['event'], registry=self.registry)
        self.durations = Histogram('audio_pipeline_stage_seconds', 'Stage latency, not device playout latency',
            ['stage'], buckets=(.005,.01,.025,.05,.1,.25,.5,1,2,5,10,30), registry=self.registry)
        self.active = Gauge('audio_pipeline_active_sessions', 'Local admitted transports', registry=self.registry)
        self.draining = Gauge('audio_pipeline_draining', 'Gateway rejects new work', registry=self.registry)
        self.counts = Counter()
        self.samples = defaultdict(lambda: deque(maxlen=2048))

    def count(self, key):
        self.counts[key] += 1
        self.events.labels(event=key).inc()

    def observe(self, key, seconds):
        self.samples[key].append(float(seconds))
        self.durations.labels(stage=key).observe(seconds)

    def prometheus(self, active, draining):
        self.active.set(active)
        self.draining.set(int(draining))
        return generate_latest(self.registry)

    def snapshot(self):
        def stats(values):
            s = sorted(values)
            return {"n": len(s), "p50": s[int((len(s)-1)*.5)], "p95": s[int((len(s)-1)*.95)]}
        return {"counts": dict(self.counts), "seconds": {k: stats(v) for k, v in self.samples.items()}}


class Trace:
    def __init__(self, endpoint_s=0):
        self.start = time.perf_counter()
        self.endpoint_s = endpoint_s
        self.times = {}

    def mark(self, name):
        self.times.setdefault(name, time.perf_counter() - self.start)

    def report(self, metrics, transport, status):
        data = {"type": "timing", "transport": transport, "status": status,
                "endpoint_s": self.endpoint_s, **self.times}
        if "first_media_dispatched_s" in self.times:
            v = self.endpoint_s + self.times["first_media_dispatched_s"]
            data["end_speech_to_dispatch_s"] = v
            metrics.observe(f"{transport}.end_speech_to_dispatch", v)
        metrics.count(f"response.{status}")
        log.info(json.dumps(data, separators=(",", ":")))
        return data
