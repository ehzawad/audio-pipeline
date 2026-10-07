/** Transport helpers, not a microphone driver. App backends issue short-lived JWTs.
 * Audio callbacks acknowledge completed rendering, never packet arrival. */
export class VoiceError extends Error {
  constructor(message, status = 0, retryAfter = 0) {
    super(message); this.name = 'VoiceError'; this.status = status; this.retryAfter = retryAfter;
  }
}

export class ControlClient {
  constructor(origin, identity, fetcher = globalThis.fetch) {
    const u = new URL(origin);
    if (u.username || u.password || u.search || u.hash || !['', '/'].includes(u.pathname) ||
        !(u.protocol === 'https:' || (u.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(u.hostname)))) {
      throw new VoiceError('An HTTPS origin is required outside loopback development');
    }
    this.origin = u.origin; this.identity = identity; this.fetcher = fetcher;
  }
  async request(path, method, body, key) {
    const token = await this.identity();
    if (!token || token.length > 4096) throw new VoiceError('Missing backend-issued identity');
    const headers = {Authorization: `Bearer ${token}`};
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    if (key) headers['Idempotency-Key'] = key;
    const response = await this.fetcher(this.origin + path, {
      method, headers, body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(10000), redirect: 'error', credentials: 'omit',
    });
    if (!response.ok) throw new VoiceError(`Voice control rejected (${response.status})`, response.status,
      Math.min(30, Math.max(0, Number(response.headers.get('Retry-After')) || 0)));
    return response.json();
  }
  create(key, transport = 'websocket') {
    if (typeof key !== 'string' || key.length < 1 || key.length > 128) throw new VoiceError('Persist a bounded idempotency key before creating a session');
    if (!['websocket', 'webrtc'].includes(transport)) throw new VoiceError('Unsupported transport');
    return this.request('/v1/sessions', 'POST', {transport}, key);
  }
  path(id) { if (!/^[0-9a-f]{32}$/.test(id)) throw new VoiceError('Invalid session identifier'); return '/v1/sessions/' + id; }
  status(id) { return this.request(this.path(id), 'GET'); }
  stop(id) { return this.request(this.path(id), 'DELETE'); }
  reconnect(id) { return this.request(this.path(id) + '/reconnect', 'POST'); }
}

export function decodePacket(value) {
  const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
  if (bytes.byteLength < 10 || bytes.byteLength > 968 || (bytes.byteLength - 8) % 2) throw new VoiceError('Invalid 20 ms PCM envelope');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const epoch = view.getUint32(0, true), seq = view.getUint32(4, true);
  if (!epoch || !seq) throw new VoiceError('Epoch and sequence must be positive');
  const pcm = new Int16Array((bytes.length - 8) / 2);
  for (let i = 0; i < pcm.length; i++) pcm[i] = view.getInt16(8 + 2 * i, true);
  return {epoch, seq, pcm, sampleRate: 24000};
}

export class PlaybackCursor {
  constructor() { this.epoch = 0; this.received = 0; this.played = 0; this.finished = new Set(); }
  clear(epoch) {
    if (!Number.isSafeInteger(epoch) || epoch < 1 || epoch > 0xffffffff) throw new VoiceError('Invalid clear epoch');
    if (epoch <= this.epoch) return false;
    this.epoch = epoch; this.received = this.played = 0; this.finished.clear(); return true;
  }
  receive(packet) {
    if (packet.epoch < this.epoch) return false;
    this.clear(packet.epoch);
    if (packet.seq !== this.received + 1 || this.received - this.played >= 100) throw new VoiceError('Sequence gap or playback backlog; do not replay stale media');
    this.received = packet.seq; return true;
  }
  didPlay(packet) {
    if (packet.epoch !== this.epoch || packet.seq <= this.played || packet.seq > this.received) return null;
    this.finished.add(packet.seq);
    const before = this.played;
    while (this.finished.delete(this.played + 1)) this.played++;
    return this.played === before ? null : {type: 'played', epoch: this.epoch, seq: this.played};
  }
}

/** An application must supply play(packet, completion) and clear() audio operations.
 * completion means finished scheduled rendering. Cancellation must NOT call it.
 * A disconnected socket is terminal: reconnect through ControlClient, with a NEW cursor.
 */
export function connectPCM(origin, grant, sampleRate, {play, clear, event, ended}, Socket = globalThis.WebSocket) {
  if (![16000, 24000, 44100, 48000].includes(sampleRate)) throw new VoiceError('Unsupported microphone sample rate');
  if (grant.state !== 'reserved') throw new VoiceError('This session has no fresh media grant');
  const u = new URL(new ControlClient(origin, async () => '').origin); u.protocol = u.protocol === 'https:' ? 'wss:' : 'ws:'; u.pathname = '/ws';
  u.search = ''; u.hash = '';
  const socket = new Socket(u.toString(), ['duplex', grant.ticket]);
  const cursor = new PlaybackCursor(); let closed = false;
  socket.binaryType = 'arraybuffer';
  const stop = () => { if (closed) return; closed = true; clear(); if (socket.readyState === 1) socket.send(JSON.stringify({type: 'hangup'})); socket.close(); };
  socket.onopen = () => socket.send(JSON.stringify({type: 'hello', sample_rate: sampleRate}));
  socket.onmessage = message => {
    if (closed) return;
    try {
      if (typeof message.data === 'string') {
        const data = JSON.parse(message.data);
        if (data.type === 'clear' && cursor.clear(data.epoch)) clear();
        event(data); return;
      }
      const packet = decodePacket(message.data), oldEpoch = cursor.epoch;
      if (!cursor.receive(packet)) return;
      if (cursor.epoch !== oldEpoch) clear();
      play(packet, () => {
        if (closed) return;
        const receipt = cursor.didPlay(packet);
        if (receipt && socket.readyState === 1) socket.send(JSON.stringify(receipt));
      });
    } catch (error) { event({type: 'error', message: error.message}); stop(); }
  };
  socket.onclose = () => { if (!closed) {closed = true; clear();} ended(); };
  socket.onerror = () => { event({type: 'error', message: 'Media connection failed'}); stop(); };
  return {
    stop,
    sendPCM(bytes) {
      if (closed || socket.readyState !== 1) return false;
      if (!(bytes instanceof ArrayBuffer) || !bytes.byteLength || bytes.byteLength % 2 || bytes.byteLength > sampleRate * .2) throw new VoiceError('Send mono signed PCM16 in frames of at most 100 ms');
      if (socket.bufferedAmount > sampleRate) { event({type: 'error', message: 'Uplink exceeds 500 ms; stopping instead of buffering old speech'}); stop(); return false; }
      socket.send(bytes); return true;
    },
  };
}
