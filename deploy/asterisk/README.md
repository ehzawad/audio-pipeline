# Private Asterisk bring-up

Use an Asterisk build with `app_audiosocket`, its channel support, and `func_uuid` available. Inspect `module show like audiosocket`, `core show application AudioSocket`, and `core show function UUID`. Version/package module availability is a deployment gate; the application does not install Asterisk.

Merge `extensions.conf` into your dialplan, reload, and route an existing authenticated test endpoint into context `voicebot-test`. Dial 7001. Configure your gateway with `AUDIOSOCKET_ENABLED=true`; for same-host tests retain the loopback bind and peer allowlist. A separate gateway requires an explicit private bind, exact allowed Asterisk CIDR, and firewall/VPN isolation. Asterisk supplies `UUID` and the PCM AudioSocket frames; SIP and RTP remain Asterisk's responsibility.

Before attaching a paid carrier, confirm inbound route context, codecs, NAT/public addresses, authentication, TLS/SRTP capability, media ports, allowed carrier peers, dialing policy and service authorization with that provider. Those values cannot be guessed generically. Do not expose AudioSocket TCP 9092 to the public internet. Asterisk pacing is not an end-user playback acknowledgment; already sent bytes cannot be remotely cleared by this protocol.
