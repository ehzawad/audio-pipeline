import asyncio
import base64
import json
import numpy as np
import pytest
import websockets
from duplex_voice.audio import float_to_pcm16
from duplex_voice.config import Settings
from duplex_voice.live import LiveSession
from duplex_voice.metrics import Metrics
from .fakes import FakeTransport


class Phone(FakeTransport):
    """8 kHz both ways; the caller sends `seconds` of real-time silence, then hangs up."""
    in_rate=out_rate=8000
    def __init__(self,seconds=.8):
        super().__init__();self.seconds=seconds
    async def frames(self):
        for _ in range(int(self.seconds/.02)):
            yield np.zeros(160,np.float32);await asyncio.sleep(.02)


def live_server(log,start_delay=.15,transcripts=()):
    """Speaks the documented GPT-Live WebSocket protocol for one session."""
    async def handler(ws):
        start=json.loads(await ws.recv());log.append(('recv',start))
        await asyncio.sleep(start_delay)   # caller audio keeps arriving meanwhile
        log.append(('sent','session.started'))
        await ws.send(json.dumps({'type':'session.started','session':{'id':'live_test'}}))
        voice=base64.b64encode(float_to_pcm16(np.full(2400,.1,np.float32))).decode()  # 100 ms
        for _ in range(3):
            await ws.send(json.dumps({'type':'session.output_audio.delta','delta':voice}))
        for item in transcripts:
            if item=='pause':
                await asyncio.sleep(1.6);continue   # long enough for a turn to settle
            kind,text,start_ms=item
            await ws.send(json.dumps({'type':f'session.{kind}_transcript.delta','delta':text,'start_ms':start_ms,'end_ms':start_ms+200}))
        async for raw in ws:
            msg=json.loads(raw);log.append(('recv',msg))
            if msg['type']=='session.close':
                await ws.send(json.dumps({'type':'session.closed','reason':'close_requested','usage':{'seconds':1}}))
                return
    return handler


def settings(port,**kw):
    return Settings(voice_mode='live',openai_api_key='test',live_url=f'ws://127.0.0.1:{port}',
                    greeting='Hi there.',**kw)


async def test_live_session_protocol_pacing_and_graceful_close():
    log=[]
    # The caller's transcript lags: the assistant starts before the caller's last words arrive.
    transcripts=[('output','Hi there.',0),'pause',('input',' What time',3000),('output','Noon.',4200),
                 ('input',' is it?',3400),'pause']
    async with websockets.serve(live_server(log,transcripts=transcripts),'127.0.0.1',0) as server:
        port=server.sockets[0].getsockname()[1];t=Phone(seconds=4.2);metrics=Metrics()
        session=LiveSession(t,None,settings(port),metrics)
        await asyncio.wait_for(session.run(),8)
    start=log[0][1]
    assert start['type']=='session.start' and start['session']['model']=='gpt-live-1'
    assert start['session']['audio']=={'format':{'type':'audio/pcm','rate':24000},'output':{'voice':'marin'}}
    assert start['session']['delegation']=={'type':'responses','responses':{'model':'gpt-5.6-luna'}}
    order=[x[1] if x[0]=='sent' else x[1]['type'] for x in log]
    started=order.index('session.started')
    # Audio read during connection setup is dropped, never sent ahead of the session.
    assert 'session.input_audio.append' not in order[:started]
    greeting=next(x[1] for x in log if x[0]=='recv' and x[1]['type']=='session.commentary.append')
    assert greeting=={'type':'session.commentary.append','event_id':'greeting-0','delegation_id':None,'content':'Hi there.'}
    assert 'AI voice assistant' in start['session']['instructions']
    assert order.count('session.input_audio.append')>=10 and order[-1]=='session.close'
    # 300 ms of output became paced 20 ms frames at the transport's 8 kHz (the streaming
    # resampler holds its filter tail until more audio arrives, as it would mid-call).
    assert len(t.writes)>=11 and all(w[2]==160 for w in t.writes)
    gaps=[b[3]-a[3] for a,b in zip(t.writes,t.writes[1:])]
    assert min(gaps)>=.015   # never two frames at once, including the first
    assert session.history==[{'role':'assistant','content':'Hi there.'},{'role':'user','content':'What time is it?'},
                             {'role':'assistant','content':'Noon.'}]
    kinds=[e['type'] for e in t.events]
    assert {'ready','partial','user','assistant_generated','timing'}<=set(kinds)
    assert metrics.counts['live.closed.close_requested']==1 and t.is_closed


async def test_unreachable_live_service_plays_the_bundled_prompt():
    t=Phone(seconds=10)   # stays on the line for the whole prompt
    session=LiveSession(t,None,Settings(voice_mode='live',openai_api_key='test',live_url='ws://127.0.0.1:9',io_timeout=1),Metrics())
    with pytest.raises(OSError):
        await asyncio.wait_for(session.run(),15)
    assert len(t.writes)>50 and t.is_closed   # the recorded "service unavailable" prompt


def test_live_mode_requires_a_key_and_skips_local_vad():
    with pytest.raises(ValueError):Settings(voice_mode='live')
    assert Settings(voice_mode='live',openai_api_key='k').effective_vad=='none'


async def test_unanswered_greeting_is_requested_once_more_then_counted():
    log=[]
    async with websockets.serve(live_server(log),'127.0.0.1',0) as server:
        port=server.sockets[0].getsockname()[1];metrics=Metrics()
        session=LiveSession(Phone(seconds=2.6),None,settings(port,live_greeting_retry_seconds=1),metrics)
        await asyncio.wait_for(session.run(),6)
    greetings=[(x[1]['type'],x[1]['event_id']) for x in log if x[0]=='recv' and x[1]['event_id' if 'event_id' in x[1] else 'type'].startswith('greeting')]
    assert greetings==[('session.commentary.append','greeting-0'),('session.instructions.append','greeting-1')]
    assert metrics.counts['live.greeting_retry']==1 and metrics.counts['live.greeting_unanswered']==1


async def test_finished_assistant_turn_reaches_the_client_without_a_next_user_turn():
    log=[]
    async with websockets.serve(live_server(log,transcripts=[('output','Hi there.',0)]),'127.0.0.1',0) as server:
        port=server.sockets[0].getsockname()[1];t=Phone(seconds=2.4)
        await asyncio.wait_for(LiveSession(t,None,settings(port),Metrics()).run(),6)
    assert {'type':'assistant_generated','text':'Hi there.'} in t.events   # sent while the call was live
