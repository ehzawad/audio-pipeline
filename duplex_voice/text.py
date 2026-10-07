"""Text that is meant to be heard: the voice system prompt, sentence chunking, TTS cleanup."""
from __future__ import annotations

import re

VOICE_SYSTEM_PROMPT = """You are a friendly, efficient English-speaking voice assistant on a live call.
Everything you write is converted to speech, so write for the ear:
- Answer in one to three short sentences unless the caller asks for more detail.
- Plain spoken English only. No markdown, lists, emojis, URLs, or code.
- Say numbers, dates, money, and units the way a person says them aloud.
- If you did not catch something, ask one short clarifying question.
- The caller can interrupt you. A previous reply may have been interrupted. History includes only completed phrases,
  followed by [interrupted]. Delivery evidence can be approximate; confirm important facts.
"""

_HARD = re.compile(r"[.!?…]+[\"'”’)\]]*(?=\s)|\n+")
_SOFT = re.compile(r"[,;:—–](?=\s)")
_ABBREV = {"mr", "mrs", "ms", "dr", "prof", "sr", "jr", "st", "vs", "etc", "e.g", "i.e",
           "a.m", "p.m", "u.s", "u.k", "no", "approx", "dept", "inc", "ltd", "co"}


class SentenceChunker:
    """Turns an LLM token stream into speakable chunks as early as is safe.

    The first chunk matters most (it sets time-to-first-audio), so it may also be cut
    at a comma once it is long enough. Later chunks wait for sentence ends, which
    gives the TTS more context and better prosody.
    """

    def __init__(self, min_chars: int = 12, first_soft_chars: int = 48, max_chars: int = 240):
        self.min_chars, self.first_soft_chars, self.max_chars = min_chars, first_soft_chars, max_chars
        self.buf = ""
        self.emitted = 0

    def push(self, text: str) -> list[str]:
        self.buf += text
        out: list[str] = []
        while (cut := self._find_cut()) is not None:
            piece, self.buf = self.buf[:cut].strip(), self.buf[cut:]
            if piece:
                out.append(piece)
                self.emitted += 1
        return out

    def flush(self) -> list[str]:
        piece, self.buf = self.buf.strip(), ""
        if piece:
            self.emitted += 1
            return [piece]
        return []

    def _find_cut(self) -> int | None:
        b = self.buf
        for m in _HARD.finditer(b):
            end = m.end()
            if m.group().startswith("\n"):
                if b[:m.start()].strip():
                    return end
                continue
            if len(b[:end].strip()) < self.min_chars:
                continue
            if m.group().startswith(".") and _is_abbrev(b, m.start()):
                continue
            return end
        if self.emitted == 0 and len(b) >= self.first_soft_chars:
            for m in _SOFT.finditer(b):
                if m.end() >= self.first_soft_chars // 2:
                    return m.end()
        if len(b) >= self.max_chars:
            sp = b.rfind(" ", 0, self.max_chars)
            return sp if sp > 0 else self.max_chars
        return None


def _is_abbrev(b: str, dot_pos: int) -> bool:
    m = re.search(r"(\S+)$", b[:dot_pos])
    if not m:
        return False
    word = m.group(1).lower().rstrip(".")
    return word in _ABBREV or (len(word) == 1 and word.isalpha() and word != "i")


def clean_for_tts(s: str) -> str:
    """Strip what a TTS would read aloud literally (markdown, bullets, link syntax)."""
    s = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", s)        # [text](url) -> text
    s = re.sub(r"https?://\S+", "", s)
    s = re.sub(r"^\s*(?:[-*•]|\d+[.)])\s+", "", s, flags=re.M)
    s = re.sub(r"[*_`#>|]+", "", s)
    return re.sub(r"\s+", " ", s).strip()
