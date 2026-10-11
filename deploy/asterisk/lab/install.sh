#!/bin/sh
# Install the loopback-only Asterisk lab. Run from the repository root:
#   sudo sh deploy/asterisk/lab/install.sh
# Each replaced stock file is kept once as <name>.dist. ASTERISK_AMI_SECRET is read from
# the environment or ./.env (never from a tracked file) and granted originate-only AMI rights.
set -eu
[ "$(id -u)" = 0 ] || { echo "run with sudo" >&2; exit 1; }
src=$(dirname "$0")
dst=/etc/asterisk
secret=${ASTERISK_AMI_SECRET:-$(sed -n 's/^ASTERISK_AMI_SECRET=//p' .env 2>/dev/null | tail -n 1)}
[ ${#secret} -ge 24 ] || { echo "set ASTERISK_AMI_SECRET (24+ chars) in .env first" >&2; exit 1; }
for f in modules.conf pjsip.conf extensions.conf rtp.conf; do
  if [ -e "$dst/$f" ] && [ ! -e "$dst/$f.dist" ]; then cp -p "$dst/$f" "$dst/$f.dist"; fi
  install -m 0640 -o root -g asterisk "$src/$f" "$dst/$f"
done
umask 077
cat > "$dst/manager.d/voicebot-lab.conf" <<CONF
[voicebot-lab]
secret = $secret
deny = 0.0.0.0/0.0.0.0
permit = 127.0.0.1/255.255.255.255
read = call
write = originate
CONF
chown root:asterisk "$dst/manager.d/voicebot-lab.conf"
chmod 0640 "$dst/manager.d/voicebot-lab.conf"
# Optional human test path: one digest-authenticated softphone, reachable only from
# LAB_SOFTPHONE_PERMIT, able to dial only the voicebot extensions (the lab has no trunk).
phone=$(sed -n 's/^LAB_SOFTPHONE_SECRET=//p' .env 2>/dev/null | tail -n 1)
lan=$(sed -n 's/^LAB_SOFTPHONE_PERMIT=//p' .env 2>/dev/null | tail -n 1)
if [ -n "$phone" ]; then
  [ ${#phone} -ge 24 ] && [ -n "$lan" ] || { echo "softphone needs LAB_SOFTPHONE_SECRET (24+) and LAB_SOFTPHONE_PERMIT" >&2; exit 1; }
  cat > "$dst/pjsip_softphone.conf" <<CONF
[lab-acl]
type = acl
deny = 0.0.0.0/0.0.0.0
permit = 127.0.0.1/255.255.255.255
permit = $lan

[transport-lan]
type = transport
protocol = udp
bind = 0.0.0.0:5062

[softphone]
type = endpoint
transport = transport-lan
context = voicebot-test
disallow = all
allow = ulaw,alaw
auth = softphone
aors = softphone
direct_media = no
rtp_symmetric = yes
force_rport = yes
rewrite_contact = yes

[softphone]
type = auth
auth_type = userpass
username = softphone
password = $phone

[softphone]
type = aor
max_contacts = 2
remove_existing = yes
CONF
  chown root:asterisk "$dst/pjsip_softphone.conf"
  chmod 0640 "$dst/pjsip_softphone.conf"
else
  rm -f "$dst/pjsip_softphone.conf"
fi
systemctl restart asterisk
echo "Asterisk lab installed; SIP on 127.0.0.1:5060, AMI on 127.0.0.1:5038, user voicebot-lab."
[ -n "$phone" ] && echo "Softphone user 'softphone' on UDP 5062, permitted from $lan."
exit 0
