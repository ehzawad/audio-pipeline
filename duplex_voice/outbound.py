"""Opt-in outbound dialing: allowlisted destinations, consent attestation, durable reservation.

A timeout after dispatch is UNKNOWN, never automatically retried. This prevents our
endpoint from placing a duplicate call merely because the first HTTP response was lost.
"""
from __future__ import annotations
import hashlib
import json
import sqlite3
from pathlib import Path


class DialStore:
    def __init__(self, filename):
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        self.filename = filename
        with self.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS dial (key TEXT PRIMARY KEY, digest TEXT NOT NULL, state TEXT NOT NULL, call_sid TEXT)")

    def connect(self):
        return sqlite3.connect(self.filename, timeout=5)

    def reserve(self, key, to):
        digest = hashlib.sha256(to.encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT digest,state,call_sid FROM dial WHERE key=?", (key,)).fetchone()
            if row:
                if row[0] != digest:
                    raise ValueError("idempotency key reused for different destination")
                return {"state": row[1], "call_sid": row[2]}, False
            db.execute("INSERT INTO dial VALUES (?,?,?,NULL)", (key, digest, "unknown"))
            return {"state": "unknown", "call_sid": None}, True

    def complete(self, key, state, sid=None):
        with self.connect() as db:
            db.execute("UPDATE dial SET state=?, call_sid=? WHERE key=?", (state, sid, key))


async def dial(s, store, client, key, to):
    import asyncio
    previous, fresh = await asyncio.to_thread(store.reserve, key, to)
    if not fresh:
        return previous
    # reserve() commits UNKNOWN before crossing the external side-effect boundary.
    response = await client.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{s.twilio_account_sid}/Calls.json",
        auth=(s.twilio_account_sid, s.twilio_auth_token),
        data={"To": to, "From": s.twilio_from_number,
              "Url": s.public_base_url.rstrip("/") + "/twilio/voice", "Method": "POST"})
    if 400 <= response.status_code < 500:
        await asyncio.to_thread(store.complete, key, "rejected")
    response.raise_for_status()
    sid = response.json()["sid"]
    await asyncio.to_thread(store.complete, key, "accepted", sid)
    return {"state": "accepted", "call_sid": sid}
