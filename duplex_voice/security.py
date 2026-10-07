"""Small explicit security primitives. Production identity belongs at a trusted gateway."""
from __future__ import annotations
import base64
import hashlib
import hmac
import ipaddress
import secrets
import time


def check_bearer(value: str, expected: str) -> bool:
    return bool(expected) and hmac.compare_digest(value, "Bearer " + expected)


def twilio_signature(url: str, pairs: list[tuple[str, str]], token: str) -> str:
    # Twilio's documented form signing, including sorted distinct multivalue parameters.
    fields = {}
    for key, value in pairs:
        fields.setdefault(key, set()).add(value)
    data = url + "".join(k + v for k in sorted(fields) for v in sorted(fields[k]))
    return base64.b64encode(hmac.new(token.encode(), data.encode(), hashlib.sha1).digest()).decode()


def valid_twilio(url, pairs, token, signature):
    return bool(token and signature) and hmac.compare_digest(twilio_signature(url, pairs, token), signature)


class Tickets:
    """Opaque, single-use, purpose-bound, expiring tickets. One gateway process only.

    Replace with an atomic shared-store GET+DELETE when routing to multiple replicas.
    No API token is embedded in a media URL. The signing key salts stored hashes.
    """
    def __init__(self, key: str = "", limit=2048, clock=time.monotonic):
        self.key = (key or secrets.token_urlsafe(32)).encode()
        self.data = {}
        self.limit, self.clock = limit, clock

    def digest(self, token):
        return hmac.new(self.key, token.encode(), hashlib.sha256).digest()

    def mint(self, purpose: str, subject: str = "", ttl: float = 60) -> str:
        now = self.clock()
        self.data = {k: v for k, v in self.data.items() if v[0] > now}
        if len(self.data) >= self.limit:
            raise ValueError("ticket capacity exhausted")
        token = secrets.token_urlsafe(32)
        self.data[self.digest(token)] = (now + ttl, purpose, subject)
        return token

    def consume(self, token: str, purpose: str, subject: str = "") -> bool:
        if not 20 <= len(token) <= 128:
            return False
        entry = self.data.get(self.digest(token))
        if entry is None:
            return False
        expiry, p, s = entry
        if expiry <= self.clock() or p != purpose or s != subject:
            return False
        self.data.pop(self.digest(token))
        return True


def allowed_peer(host: str, cidrs: str) -> bool:
    ip = ipaddress.ip_address(host)
    return any(ip in ipaddress.ip_network(c.strip(), strict=False) for c in cidrs.split(",") if c.strip())
