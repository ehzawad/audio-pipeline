"""A bundled generic synthetic failure prompt; no cloud or ML provider is needed."""
from functools import lru_cache
from pathlib import Path
import lzma
from ..audio import mulaw_decode


@lru_cache(maxsize=1)
def samples():
    path = Path(__file__).parents[1] / 'assets' / 'service-unavailable.ulaw.xz'
    # Fixed, repository-owned narrowband prompt; no runtime synthesizer dependency.
    packed = path.read_bytes()
    if len(packed) > 65536:
        raise ValueError('invalid bundled prompt size')
    raw = lzma.decompress(packed, memlimit=16*1024*1024)
    if not 8000 <= len(raw) <= 8000*10:
        raise ValueError('invalid bundled prompt duration')
    return mulaw_decode(raw)


class ServiceUnavailable:
    sample_rate = 8000
    text = 'Sorry, the voice service is unavailable. Please try again later.'

    async def synthesize(self, text):
        audio = samples()
        for start in range(0, len(audio), 160):
            yield audio[start:start+160]
