"""Fetch real human telephone speech for the lab callers: MInDS-14 en-US (PolyAI, CC BY 4.0).

The recordings are customers phoning a bank's voice assistant, stored as G.711 mu-law 8 kHz.
A deterministic selection per intent is decoded to PCM16 WAV under data/human/ (ignored by
Git) with corpus.json (references, intents, source revision and hash) and caller scenarios.

The dataset's transcriptions are its own and were not re-verified here; some contain obvious
recognition errors. Treat word agreement against them as approximate, not as WER ground truth.
Attribution: Gerz et al., "Multilingual and Cross-Lingual Intent Detection from Spoken Data"
(MInDS-14), https://huggingface.co/datasets/PolyAI/minds14, CC BY 4.0.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path

import numpy as np

from duplex_voice.audio import mulaw_decode
from scripts.lab_caller import wav_bytes

REPO, FILE = "PolyAI/minds14", "en-US/train-00000-of-00001.parquet"


def chunks(riff: bytes):
    i = 12
    while i + 8 <= len(riff):
        cid, n = riff[i:i + 4], struct.unpack("<I", riff[i + 4:i + 8])[0]
        yield cid, riff[i + 8:i + 8 + n]
        i += 8 + n + (n & 1)


def decode(riff: bytes) -> np.ndarray:
    c = dict(chunks(riff))
    tag, channels, rate, _, _, bits = struct.unpack("<HHIIHH", c[b"fmt "][:16])
    if (tag, channels, rate, bits) != (7, 1, 8000, 8):
        raise ValueError("expected mono 8 kHz G.711 mu-law")
    return mulaw_decode(c[b"data"])


def main():
    import pyarrow.parquet as pq
    from huggingface_hub import HfApi, hf_hub_download
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=Path("data/human"))
    p.add_argument("--revision", help="dataset commit; default resolves the current one")
    p.add_argument("--per-intent", type=int, default=2)
    p.add_argument("--min-seconds", type=float, default=2)
    p.add_argument("--max-seconds", type=float, default=12)
    p.add_argument("--turns", type=int, default=3, help="human utterances per caller scenario")
    args = p.parse_args()
    revision = args.revision or HfApi().dataset_info(REPO).sha
    path = Path(hf_hub_download(REPO, FILE, repo_type="dataset", revision=revision,
                                local_dir="data/datasets/minds14"))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    rows = sorted(pq.read_table(path).to_pylist(), key=lambda r: r["path"])
    taken, corpus = {}, []
    args.out.mkdir(parents=True, exist_ok=True)
    for r in rows:
        intent = r["path"].split("~", 1)[1].split("/", 1)[0]
        samples = decode(r["audio"]["bytes"])
        seconds = len(samples) / 8000
        if taken.get(intent, 0) >= args.per_intent or not args.min_seconds <= seconds <= args.max_seconds:
            continue
        taken[intent] = taken.get(intent, 0) + 1
        uid = f"{intent.lower()}-{taken[intent]}"
        wav = args.out / f"{uid}.wav"
        wav.write_bytes(wav_bytes(samples))
        corpus.append({"id": uid, "intent": intent, "reference": r["transcription"].strip(),
                       "seconds": round(seconds, 2), "wav": str(wav), "source": r["path"]})
    # Interleave intents so each three-turn call hears different requests.
    order = [c for i in range(args.per_intent) for c in corpus if c["id"].endswith(f"-{i + 1}")]
    scenarios = []
    for n in range(0, len(order) - args.turns + 1, args.turns):
        turns = [{"audio": c["wav"], "say": c["reference"], "id": c["id"]} for c in order[n:n + args.turns]]
        scenario = args.out / f"scenario-{n // args.turns + 1:02d}.json"
        scenario.write_text(json.dumps(turns, indent=2) + "\n")
        scenarios.append(str(scenario))
    (args.out / "corpus.json").write_text(json.dumps({
        "source": {"repo": REPO, "file": FILE, "revision": revision, "sha256": digest, "license": "CC BY 4.0"},
        "selection": vars(args) | {"out": str(args.out)}, "utterances": corpus, "scenarios": scenarios},
        indent=2, default=str) + "\n")
    print(f"{len(corpus)} utterances from {len(taken)} intents, {len(scenarios)} scenarios; "
          f"{REPO}@{revision[:12]} sha256 {digest[:16]}")


if __name__ == "__main__":
    main()
