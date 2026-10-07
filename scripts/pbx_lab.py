"""Place a synthetic call through the loopback Asterisk lab and time it from the caller's ear.

The caller is itself an AudioSocket endpoint: AMI originates PJSIP/7001@voicebot-loop, so the
call crosses real SIP signalling and G.711 RTP inside Asterisk before reaching the gateway's
AudioSocket listener. Scripted turns are synthesized with OpenAI TTS (see scripts/lab_caller.py).

Evidence boundary: "heard" means audio delivered to the synthetic caller's AudioSocket after
the RTP loopback, not a handset loudspeaker. One run is a smoke measurement, not a latency SLO.
Recordings stay under data/ (ignored by Git); only timing and transcripts go in the report.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import dotenv_values

from duplex_voice.audio import float_to_pcm16, pcm16_to_float
from scripts import lab_caller
from scripts.lab_caller import FRAME


class AudioSocketLine:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer

    async def recv(self):
        while True:
            try:
                h = await self.reader.readexactly(3)
                body = await self.reader.readexactly(int.from_bytes(h[1:], "big"))
            except (asyncio.IncompleteReadError, ConnectionError):
                return None
            if h[0] == 0x10:
                return pcm16_to_float(body)
            if h[0] in (0x00, 0xFF):
                return None

    async def send(self, frame):
        self.writer.write(b"\x10" + (FRAME * 2).to_bytes(2, "big") + float_to_pcm16(frame))
        await self.writer.drain()

    async def hangup(self):
        self.writer.write(b"\x00\x00\x00")
        await self.writer.drain()


async def ami_originate(secret, channel, call_id, port, timeout):
    reader, writer = await asyncio.open_connection("127.0.0.1", 5038)

    async def send(**fields):
        writer.write("".join(f"{k}: {v}\r\n" for k, v in fields.items()).encode() + b"\r\n")
        await writer.drain()

    async def message():
        fields = {}
        while line := (await reader.readline()).decode().rstrip("\r\n"):
            k, _, v = line.partition(": ")
            fields[k] = v
        return fields

    try:
        await reader.readline()  # banner
        await send(Action="Login", Username="voicebot-lab", Secret=secret, Events="call")
        if (await message()).get("Response") != "Success":
            raise RuntimeError("AMI login failed; run deploy/asterisk/lab/install.sh")
        await send(Action="Originate", ActionID=call_id, Channel=channel, Application="AudioSocket",
                   Data=f"{call_id},127.0.0.1:{port}", Async="true", Timeout=int(timeout * 1000))
        async with asyncio.timeout(timeout + 5):
            while True:
                m = await message()
                if m.get("ActionID") != call_id:
                    continue
                if m.get("Response") == "Error":
                    raise RuntimeError("AMI refused the originate: " + m.get("Message", ""))
                if m.get("Event") == "OriginateResponse":
                    if m.get("Response") != "Success":
                        raise RuntimeError("call was not answered: " + m.get("Reason", "?"))
                    return
    finally:
        writer.close()


async def gateway_metrics(client, url):
    if not url:
        return None
    try:
        r = await client.get(url.rstrip("/") + "/metrics", timeout=3)
        return r.json() if r.status_code == 200 else None
    except httpx.HTTPError:
        return None


def delta(before, after):
    if not before or not after:
        return None
    counts = {k: v - before["counts"].get(k, 0) for k, v in after["counts"].items()
              if v != before["counts"].get(k, 0)}
    return {"counts": counts, "active_sessions": after.get("active_sessions")}


async def run(args):
    env = {**dotenv_values(".env"), **os.environ}
    env.setdefault("OPENAI_BASE_URL", "https://api.openai.com/v1")
    env.setdefault("CALLER_VOICE", "marin")
    for name in ("OPENAI_API_KEY", "ASTERISK_AMI_SECRET"):
        if not env.get(name):
            raise SystemExit(f"{name} is required in the environment or .env")
    scenario = lab_caller.load_scenario(args)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = args.recordings or Path("data/pbx-lab")
    (root / "caller-cache").mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(timeout=60) as client:
        utterances = [await lab_caller.synthesize(client, env, t["say"], root / "caller-cache") for t in scenario]
        result = asyncio.get_running_loop().create_future()

        async def accept(reader, writer):
            if result.done():
                writer.close()
                return
            try:
                header = await reader.readexactly(3)
                if header[0] != 0x01:
                    raise RuntimeError("expected the AudioSocket UUID packet first")
                await reader.readexactly(int.from_bytes(header[1:], "big"))
                result.set_result(await lab_caller.converse(AudioSocketLine(reader, writer), scenario, utterances, args))
            except BaseException as exc:
                if not result.done():
                    result.set_exception(exc)
            finally:
                writer.close()

        server = await asyncio.start_server(accept, "127.0.0.1", args.port)
        call_id = str(uuid.uuid4())
        before = await gateway_metrics(client, args.gateway)
        try:
            await ami_originate(env["ASTERISK_AMI_SECRET"], args.channel, call_id, args.port, 15)
            greeting, turns, ear, said, duration = await asyncio.wait_for(result, args.call_timeout)
        finally:
            server.close()
            await server.wait_closed()
        await asyncio.sleep(1)
        after = await gateway_metrics(client, args.gateway)
        failures = await lab_caller.analyze(client, env, scenario, turns, ear, duration)
    report = {
        "scope": "synthetic caller over the loopback Asterisk lab; one call, not a latency SLO",
        "evidence": "audio received by the caller's AudioSocket after Asterisk PJSIP/RTP loopback",
        "run_id": run_id, "channel": args.channel, "call_seconds": round(duration, 3),
        "caller_voice": env["CALLER_VOICE"], "greeting": greeting, "turns": turns,
        "gateway_metrics_delta": delta(before, after), "recordings": str(root / run_id),
        "failures": failures,
    }
    lab_caller.save(root / run_id, report, ear, said, duration)
    print(lab_caller.json.dumps(report, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    lab_caller.add_arguments(p)
    p.add_argument("--channel", default="PJSIP/7001@voicebot-loop",
                   help="Asterisk dial string; Local/7001@voicebot-test skips SIP/RTP")
    p.add_argument("--port", type=int, default=9093, help="loopback AudioSocket port for the caller")
    p.add_argument("--gateway", default="http://127.0.0.1:8000", help="gateway for /metrics deltas")
    raise SystemExit(asyncio.run(run(p.parse_args())))
