"""Run the configured ASR adapter over a recorded corpus; report word agreement and latency.

Input is scripts/human_corpus.py's corpus.json. Each WAV is resampled to the gateway's internal
16 kHz and streamed in 32 ms frames (real time unless --fast), exactly as a turn would feed it.
"Word error" is edit distance against the corpus reference after lowercasing and stripping
punctuation. MInDS-14 references were not re-verified, so treat it as approximate agreement.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import statistics
import time
import wave
from pathlib import Path

import numpy as np

from duplex_voice.audio import Resampler, pcm16_to_float
from duplex_voice.config import Settings
from duplex_voice.providers.clients import OpenAIRealtimeASR


def words(text):
    return re.sub(r"[^a-z0-9' ]+", " ", text.lower().replace("’", "'")).split()


def edits(ref, hyp):
    row = list(range(len(hyp) + 1))
    for i, r in enumerate(ref, 1):
        prev, row[0] = row[0], i
        for j, h in enumerate(hyp, 1):
            prev, row[j] = row[j], min(row[j] + 1, row[j - 1] + 1, prev + (r != h))
    return row[-1]


async def recognize(asr, path, fast):
    with wave.open(path, "rb") as w:
        rate, x = w.getframerate(), pcm16_to_float(w.readframes(w.getnframes()))
    rs = Resampler(rate, 16000)
    x = np.concatenate([rs.process(x), rs.flush()])
    eof = None

    async def frames():
        nonlocal eof
        for i in range(0, len(x), 512):
            yield x[i:i + 512]
            if not fast:
                await asyncio.sleep(512 / 16000)
        eof = time.perf_counter()

    text = await asr.recognize(frames(), lambda _: None)
    return text, time.perf_counter() - eof


async def run(args):
    s = Settings.from_env()
    if args.model:  # an overridden model does not inherit the profile's model-specific delay
        s = s.model_copy(update={"openai_asr_model": args.model, "openai_asr_delay": args.delay})
    asr = OpenAIRealtimeASR(s)
    corpus = json.loads(args.corpus.read_text())
    rows, total_edits, total_words = [], 0, 0
    for u in corpus["utterances"][:args.limit]:
        try:
            text, residual = await asyncio.wait_for(recognize(asr, u["wav"], args.fast), 90)
        except Exception as exc:
            rows.append({"id": u["id"], "error": type(exc).__name__})
            continue
        ref, hyp = words(u["reference"]), words(text)
        e = edits(ref, hyp)
        total_edits, total_words = total_edits + e, total_words + len(ref)
        rows.append({"id": u["id"], "seconds": u["seconds"], "final_after_audio_s": round(residual, 3),
                     "word_error": round(e / max(1, len(ref)), 3), "reference": u["reference"], "hypothesis": text})
    ok = [r for r in rows if "error" not in r]
    report = {"scope": "ASR adapter on recorded telephone speech; references are the corpus's own, not verified",
              "model": s.openai_asr_model, "delay": s.openai_asr_delay or "default", "realtime_feed": not args.fast,
              "source": corpus["source"], "utterances": len(rows), "failures": len(rows) - len(ok),
              "corpus_word_error": round(total_edits / max(1, total_words), 3),
              "final_after_audio_s": {"median": statistics.median(r["final_after_audio_s"] for r in ok),
                                      "max": max(r["final_after_audio_s"] for r in ok)} if ok else None,
              "rows": rows}
    if args.output:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "rows"}, indent=2))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--corpus", type=Path, default=Path("data/human/corpus.json"))
    p.add_argument("--model", help="override OPENAI_ASR_MODEL")
    p.add_argument("--delay", default="", help="with --model: delay setting (gpt-live-transcribe only)")
    p.add_argument("--limit", type=int)
    p.add_argument("--fast", action="store_true", help="send audio faster than real time")
    p.add_argument("--output", type=Path)
    asyncio.run(run(p.parse_args()))
