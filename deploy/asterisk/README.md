# Private Asterisk bring-up

Use an Asterisk build with `app_audiosocket`, its channel support, and `func_uuid` available. Inspect `module show like audiosocket`, `core show application AudioSocket`, and `core show function UUID`. Version/package module availability is a deployment gate; the application does not install Asterisk.

Merge `extensions.conf` into your dialplan, reload, and route an existing authenticated test endpoint into context `voicebot-test`. Dial 7001. Configure your gateway with `AUDIOSOCKET_ENABLED=true`; for same-host tests retain the loopback bind and peer allowlist. A separate gateway requires an explicit private bind, exact allowed Asterisk CIDR, and firewall/VPN isolation. Asterisk supplies `UUID` and the PCM AudioSocket frames; SIP and RTP remain Asterisk's responsibility.

Before attaching a paid carrier, confirm inbound route context, codecs, NAT/public addresses, authentication, TLS/SRTP capability, media ports, allowed carrier peers, dialing policy and service authorization with that provider. Those values cannot be guessed generically. Do not expose AudioSocket TCP 9092 to the public internet. Asterisk pacing is not an end-user playback acknowledgment; already sent bytes cannot be remotely cleared by this protocol.

## Loopback lab

`lab/` turns a **disposable** Asterisk 22 install into a loopback-only test PBX. It replaces `modules.conf`, `pjsip.conf`, `extensions.conf` and `rtp.conf`, and keeps each stock file once as `<name>.dist`. It also stops loading the IAX2, Lua and AEL sample dialplans and adds an originate-only AMI user bound to 127.0.0.1. SIP listens on 127.0.0.1:5060 only, and RTP on 127.0.0.1:10000–10200.

```sh
sudo apt install asterisk asterisk-modules        # Ubuntu 26.04 universe: 22.5.2
python3 -c 'import secrets; print("ASTERISK_AMI_SECRET=" + secrets.token_urlsafe(32))' >> .env
sudo sh deploy/asterisk/lab/install.sh
AUDIOSOCKET_ENABLED=true uv run uvicorn duplex_voice.main:app --port 8000   # any real model profile
uv run python -m scripts.pbx_lab
```

`scripts.pbx_lab` originates `PJSIP/7001@voicebot-loop`, so Asterisk calls itself over real SIP and G.711 RTP. Its far end is the `voicebot-test` route into the gateway. The caller side is a second AudioSocket connection owned by the script: it waits for the greeting, speaks scenario turns synthesized with OpenAI TTS in a different voice, barges in, and times what it hears. It writes WAVs and `report.json` under `data/pbx-lab/`. `--channel Local/7001@voicebot-test` skips SIP/RTP. Expect the loopback path to hide network jitter, loss and NAT; it does not replace a trunk test.

### The Twilio route without Twilio

Extension 7002 hands the call to `scripts.twilio_emulator` on 127.0.0.1:9094. The emulator then plays Twilio's part against the gateway: a signed webhook, TwiML, a signed Media Streams WebSocket with the ticket parameter, mu-law media, and marks echoed when its playout reaches them. It also handles `clear`, DTMF and `stop`. Give the gateway and emulator the same **lab-only** `TWILIO_ACCOUNT_SID` (`AC` + 32 hex) and `TWILIO_AUTH_TOKEN`; never use a real account's token here.

```sh
AUDIOSOCKET_ENABLED=true TWILIO_ENABLED=true PUBLIC_BASE_URL=http://127.0.0.1:8000 \
  uv run uvicorn duplex_voice.main:app --port 8000
uv run python -m scripts.twilio_emulator
uv run python -m scripts.pbx_lab --channel PJSIP/7002@voicebot-loop
```

### Calling the lab yourself

Set `LAB_SOFTPHONE_SECRET` (24+ random characters) and `LAB_SOFTPHONE_PERMIT` (your LAN, for example `192.168.1.0/255.255.255.0`) in `.env`, then rerun `install.sh`. That adds one digest-authenticated account, `softphone`, on UDP 5062, plus an ACL that accepts SIP only from loopback and that subnet. It can dial only 7001 and 7002, and the lab has no trunk, so it cannot place a real call. Configure any SIP client with these settings:

- user `softphone`;
- the password from `.env`;
- domain `<this host's LAN IP>:5062`;
- transport UDP;
- codec PCMU/PCMA.

Linphone (open source; Android, iOS, desktop) and baresip work. Use headphones; the gateway has no acoustic echo canceller. Remove the account by deleting `LAB_SOFTPHONE_SECRET` and rerunning `install.sh`.
