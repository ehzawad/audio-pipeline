import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {ControlClient, PlaybackCursor, decodePacket} from './voice.mjs';
const fixture = JSON.parse(readFileSync(new URL('../fixtures/protocol.json', import.meta.url)));

test('wire fixture is little-endian PCM16; no float or native-endian assumptions', () => {
  for (const example of fixture.packets) {
    const packet = decodePacket(Uint8Array.from(Buffer.from(example.hex, 'hex')));
    assert.equal(packet.epoch, example.epoch); assert.equal(packet.seq, example.seq);
    assert.deepEqual([...packet.pcm], example.samples);
  }
  for (const hex of fixture.invalid) assert.throws(() => decodePacket(Uint8Array.from(Buffer.from(hex, 'hex'))));
});
test('late and out-of-order render completions never acknowledge unheard gaps', () => {
  const c = new PlaybackCursor(), a = {epoch: 1, seq: 1}, b = {epoch: 1, seq: 2};
  c.receive(a); c.receive(b); assert.equal(c.didPlay(b), null);
  assert.deepEqual(c.didPlay(a), {type: 'played', epoch: 1, seq: 2});
  c.clear(2); assert.equal(c.didPlay(b), null); assert.equal(c.receive(a), false);
  assert.throws(() => c.receive({epoch: 2, seq: 2}));
});
test('HTTP identity is supplied by the app backend and never put in media URLs', async () => {
  const seen=[]; const client=new ControlClient('https://voice.example', async ()=>'temporary-jwt', async (url, request)=>{
    seen.push({url,request}); return {ok:true,json:async()=>({state:'reserved'}),headers:new Headers()};
  });
  await client.create('persisted-request-key');
  assert.equal(seen[0].request.headers.Authorization, 'Bearer temporary-jwt');
  assert.equal(seen[0].request.headers['Idempotency-Key'], 'persisted-request-key');
  assert.ok(!seen[0].url.includes('temporary-jwt')); assert.equal(seen[0].request.redirect, 'error');
  assert.throws(()=>new ControlClient('http://public.example', ()=>''));
});
