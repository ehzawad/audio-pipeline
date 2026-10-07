import asyncio
import base64
import hashlib
import hmac
import threading
import time

import numpy as np
import pytest
import soxr
from pydantic import ValidationError
from duplex_voice.audio import (float_to_pcm16, pcm16_to_float, mulaw_encode,
    mulaw_decode, PCMDecoder, Packetizer, Framer, Resampler)
from duplex_voice.concurrency import Admission, ModelRunner, Overloaded
from duplex_voice.config import Settings
from duplex_voice.playback import PlaybackLedger
from duplex_voice.security import Tickets, valid_twilio, twilio_signature, check_bearer, allowed_peer
from duplex_voice.turns import TurnDetector, Start, End, UtteranceLimit
from duplex_voice.text import SentenceChunker


def test_pcm_exhaustive_roundtrip():
    integers = np.arange(-32768, 32768, dtype=np.int32).astype('<i2')
    assert float_to_pcm16(pcm16_to_float(integers.tobytes())) == integers.tobytes()


@pytest.mark.parametrize('bad', [np.array([np.nan]), np.array([np.inf]), np.zeros((2,2))])
def test_nonfinite_or_stereo_is_rejected(bad):
    with pytest.raises(ValueError): float_to_pcm16(bad)


def test_pcm_odd_byte_rejected():
    with pytest.raises(ValueError): pcm16_to_float(b'\x00')


def test_pcm_http_fragmentation():
    original = np.arange(-100, 100, dtype='<i2').tobytes()
    dec = PCMDecoder(); out = []
    for i in range(0, len(original), 7): out.append(dec.push(original[i:i+7]))
    dec.finish()
    assert float_to_pcm16(np.concatenate(out)) == original


def test_truncated_http_pcm():
    d = PCMDecoder(); d.push(b'\x01')
    with pytest.raises(ValueError): d.finish()


def reference_ulaw(sample):
    # Independent scalar transcription of Sun/ITU G.711 linear2ulaw, 14-bit input.
    pcm = int(sample) >> 2
    mask = 0x7f if pcm < 0 else 0xff
    pcm = min(abs(pcm), 8159) + 33
    ends = (0x3f,0x7f,0xff,0x1ff,0x3ff,0x7ff,0xfff,0x1fff)
    seg = next((i for i, end in enumerate(ends) if pcm <= end), 8)
    return (0x7f if seg == 8 else (seg << 4) | ((pcm >> (seg + 1)) & 15)) ^ mask


def test_mulaw_all_int16_against_scalar_reference():
    x = np.arange(-32768,32768,dtype=np.int32)
    expected = bytes(reference_ulaw(n) for n in x)
    assert mulaw_encode(x.astype(np.float32)/32768) == expected


def test_mulaw_decode_all_codewords():
    reference=[]
    for b in range(256):
        u=(~b)&255; magnitude=(((u&15)*8+132) << ((u>>4)&7))-132
        reference.append((-magnitude if u&128 else magnitude)/32768)
    np.testing.assert_array_equal(mulaw_decode(bytes(range(256))), reference)


@pytest.mark.parametrize('rate', [8000,16000,24000,48000])
def test_packetizer_pads_only_at_end(rate):
    p=Packetizer(rate); n=rate//50*3+7; x=np.arange(n,dtype=np.float32)
    frames=[]
    for i in range(0,n,37): frames+=p.push(x[i:i+37])
    frames+=p.finish(); y=np.concatenate(frames)
    np.testing.assert_array_equal(y[:n],x)
    assert len(frames)==4 and not y[n:].any() and p.finish()==[]


def test_framer_rejects_zero_size():
    with pytest.raises(ValueError): Framer(0)


def test_stream_resampling_matches_continuous_signal():
    x=np.sin(np.arange(44100,dtype=np.float32)*.01).astype(np.float32)
    r=Resampler(44100,16000)
    chunks=[r.process(x[i:i+882]) for i in range(0,len(x),882)]+[r.flush()]
    y=np.concatenate(chunks)
    np.testing.assert_allclose(y,soxr.resample(x,44100,16000,quality='HQ'),atol=2e-6)


def test_turn_preroll_and_quantized_endpoint():
    d=TurnDetector(start_ms=64,end_ms=100,barge_ms=128,preroll_ms=128)
    a=np.ones(512,np.float32)
    for _ in range(4): assert d.push(a*.1,0,False) is None
    assert d.push(a,1,False) is None
    start=d.push(a,1,False)
    assert isinstance(start,Start) and len(start.preroll)==6
    for _ in range(3): assert d.push(a*0,0,False) is None
    assert d.push(a*0,0,False)==End(128)


def test_barge_in_confirmation_is_longer():
    d=TurnDetector(start_ms=32,barge_ms=96)
    x=np.zeros(512,np.float32)
    assert d.push(x,1,True) is None and d.push(x,1,True) is None
    assert isinstance(d.push(x,1,True),Start)


def test_hysteresis_does_not_end_on_soft_speech():
    d=TurnDetector(start_ms=32,end_ms=64)
    x=np.zeros(512,np.float32); d.push(x,1,False)
    for _ in range(20): assert d.push(x,.4,False) is None


def test_continuous_utterance_limit_does_not_invent_endpoint():
    d=TurnDetector(start_ms=32,max_seconds=.064)
    x=np.zeros(512,np.float32); d.push(x,1,False); d.push(x,1,False)
    with pytest.raises(UtteranceLimit): d.push(x,1,False)


@pytest.mark.parametrize('prob', [-1,2,float('nan')])
def test_invalid_vad_probability(prob):
    with pytest.raises(ValueError): TurnDetector().push(np.zeros(512),prob,False)


def test_epoch_rejects_cleared_and_future_marks():
    p=PlaybackLedger(); e=p.begin(); p.issue(e); p.phrase_sent(e,'Never claim this.',1)
    e2=p.begin(); p.issue(e2)
    assert not p.acknowledge(e,1) and not p.acknowledge(e2,1000)
    assert p.completed_text()==''


def test_whole_phrase_receipts_not_proportional_words():
    p=PlaybackLedger(); e=p.begin()
    for _ in range(10): p.issue(e)
    p.phrase_sent(e,'First phrase.',4); p.phrase_sent(e,'Never assume remaining words.',10)
    assert p.acknowledge(e,7)
    assert p.completed_text()=='First phrase.'
    assert not p.acknowledge(e,5) and not p.acknowledge(e,7)


async def test_playback_timeout():
    p=PlaybackLedger();e=p.begin();p.issue(e)
    with pytest.raises(TimeoutError): await p.drain(e,.01)


async def test_drain_awakes_on_receipt():
    p=PlaybackLedger();e=p.begin();p.issue(e)
    task=asyncio.create_task(p.drain(e,1));await asyncio.sleep(0)
    p.acknowledge(e,1);await task


async def test_stale_writer_canceled():
    p=PlaybackLedger();e=p.begin();p.begin()
    with pytest.raises(asyncio.CancelledError): p.issue(e)


async def test_admission_and_release_and_drain():
    a=Admission(1)
    async with a.slot():
        assert a.active==1
        with pytest.raises(Overloaded):
            async with a.slot(): pass
    assert a.active==0
    a.draining=True
    with pytest.raises(Overloaded):
        async with a.slot(): pass


async def test_canceling_native_await_does_not_free_compute_slot():
    r=ModelRunner(1); started=threading.Event(); release=threading.Event(); second=threading.Event()
    def first():
        started.set(); release.wait(2); return 1
    a=asyncio.create_task(r.run(first))
    await asyncio.to_thread(started.wait,1)
    a.cancel(); await asyncio.gather(a,return_exceptions=True)
    b=asyncio.create_task(r.run(lambda: second.set()))
    await asyncio.sleep(.04); assert not second.is_set()
    release.set(); await b; await r.close()
    assert second.is_set()


def test_tickets_purpose_subject_single_use():
    t=Tickets(); token=t.mint('twilio','CA1')
    assert not t.consume(token,'websocket','CA1')
    assert not t.consume(token,'twilio','CA2')
    assert t.consume(token,'twilio','CA1')
    assert not t.consume(token,'twilio','CA1')


def test_ticket_expiry_and_capacity():
    now=[0.]; t=Tickets(limit=1,clock=lambda:now[0]); a=t.mint('websocket',ttl=2)
    with pytest.raises(ValueError): t.mint('websocket')
    now[0]=3; assert not t.consume(a,'websocket')
    assert t.consume(t.mint('websocket'),'websocket')


def test_twilio_published_signature_vector():
    # Twilio security documentation's example, not a self-generated expected signature.
    url='https://example.com/myapp.php?foo=1&bar=2'
    params=[('CallSid','CA1234567890ABCDE'),('Caller','+14158675310'),('Digits','1234'),
            ('From','+14158675310'),('To','+18005551212')]
    assert twilio_signature(url,params,'12345')=='L/OH5YylLD5NRKLltdqwSvS0BnU='


def test_signature_covers_path_parameters_and_token():
    sig=twilio_signature('https://test/voice',[('A','x')],'key')
    assert valid_twilio('https://test/voice',[('A','x')],'key',sig)
    assert not valid_twilio('https://test/stream',[('A','x')],'key',sig)
    assert not valid_twilio('https://test/voice',[('A','y')],'key',sig)
    assert not valid_twilio('https://test/voice',[('A','x')],'other',sig)
    assert not valid_twilio('https://test/voice',[],'','')


def test_bearer_and_private_network():
    assert check_bearer('Bearer secret','secret') and not check_bearer('Bearer ','')
    assert allowed_peer('127.0.0.1','127.0.0.0/8')
    assert not allowed_peer('8.8.8.8','10.0.0.0/8')


@pytest.mark.parametrize('changes',[
    {'deployment':'production'}, {'voice_mode':'local','vad_backend':'energy'},
    {'twilio_enabled':True}, {'public_base_url':'https://a/x'},
    {'public_base_url':'https://user:pw@a'}, {'max_sessions':0},
    {'outbound_enabled':True},
])
def test_configuration_fails_closed(changes):
    with pytest.raises(ValidationError): Settings(**changes)


def test_sentence_chunker_preserves_abbreviation_decimal_and_tail():
    c=SentenceChunker(); result=[]
    text='Dr. Adams paid 3.50 dollars. Please wait here.'
    for char in text: result+=c.push(char)
    result+=c.flush()
    assert ' '.join(result)==text and all(x!='Dr.' for x in result)
