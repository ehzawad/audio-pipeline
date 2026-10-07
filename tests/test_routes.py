import asyncio
import base64
import json
import struct
from xml.etree import ElementTree as ET

import numpy as np
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from duplex_voice.main import create_app
from duplex_voice.config import Settings
from duplex_voice.security import twilio_signature
from duplex_voice.audio import mulaw_encode, float_to_pcm16
from duplex_voice.session import VoiceSession
from duplex_voice.metrics import Metrics
from duplex_voice.transports.audiosocket import AudioSocketTransport
from .fakes import fake_providers

ORIGIN='http://localhost:8000'


def make_ticket(c, token=''):
    h={'Origin':ORIGIN}
    if token:h['Authorization']='Bearer '+token
    return c.post('/api/tickets',headers=h,json={'transport':'websocket'})


def test_health_and_security_headers():
    with TestClient(create_app(Settings())) as c:
        r=c.get('/');assert r.status_code==200
        assert "frame-ancestors 'none'" in r.headers['content-security-policy']
        assert c.get('/healthz').json()=={'alive':True}
        assert c.get('/readyz').status_code==200
        assert c.post('/twilio/outbound',json={'to':'+12025550123','consent_confirmed':True}).status_code==404


def test_browser_token_origin_and_single_use():
    s=Settings(app_token='private')
    with TestClient(create_app(s)) as c:
        assert make_ticket(c).status_code==401
        assert c.post('/api/tickets',headers={'Origin':'https://evil','Authorization':'Bearer private'},json={}).status_code==403
        t=make_ticket(c,'private').json()['ticket']
        with c.websocket_connect('/ws',subprotocols=['duplex',t],headers={'Origin':ORIGIN}) as ws:
            ws.send_json({'type':'hello','sample_rate':16000})
            assert ws.receive_json()['sample_rate']==24000
            assert ws.receive_json()['mode']=='demo'
            ws.send_json({'type':'hangup'})
        with pytest.raises(WebSocketDisconnect):
            with c.websocket_connect('/ws',subprotocols=['duplex',t],headers={'Origin':ORIGIN}):pass


def test_browser_real_route_bidirectional_pcm_ack_and_complete_turn():
    with TestClient(create_app(Settings(greeting='',vad_start_ms=32,vad_end_ms_web=64))) as c:
        t=make_ticket(c).json()['ticket']
        with c.websocket_connect('/ws',subprotocols=['duplex',t],headers={'Origin':ORIGIN}) as ws:
            ws.send_json({'type':'hello','sample_rate':16000})
            ws.receive_json(); ws.receive_json()
            # Each frame is below the 200-ms ingress size limit.
            for _ in range(3):ws.send_bytes(float_to_pcm16(np.ones(512,np.float32)*.2))
            for _ in range(3):ws.send_bytes(bytes(1024))
            packets=0;completed=False
            for _ in range(100):
                msg=ws.receive()
                if msg.get('bytes') is not None:
                    b=msg['bytes'];epoch,seq=struct.unpack('<II',b[:8]); assert len(b)==968
                    packets+=1;ws.send_json({'type':'played','epoch':epoch,'seq':seq})
                elif msg.get('text'):
                    event=json.loads(msg['text'])
                    if event.get('type')=='timing' and event['status']=='completed':completed=True;break
            assert packets==24 and completed
            ws.send_json({'type':'hangup'})
            assert ws.receive()['type']=='websocket.close'
        # Shared-state finalization precedes socket closure; resource release is asynchronous.
        for _ in range(100):
            if c.get('/metrics').json()['active_sessions']==0: break
            __import__('time').sleep(.01)
        assert c.get('/metrics').json()['active_sessions']==0


def test_browser_invalid_sample_rate_is_rejected():
    with TestClient(create_app(Settings(greeting=''))) as c:
        t=make_ticket(c).json()['ticket']
        with c.websocket_connect('/ws',subprotocols=['duplex',t],headers={'Origin':ORIGIN}) as ws:
            ws.send_json({'type':'hello','sample_rate':123})
            msg=ws.receive();assert msg['type']=='websocket.close' or 'error' in msg.get('text','')


def test_admission_does_not_admit_second_call():
    app=create_app(Settings(greeting='',max_sessions=1))
    with TestClient(app) as c:
        t=make_ticket(c).json()['ticket']
        with c.websocket_connect('/ws',subprotocols=['duplex',t],headers={'Origin':ORIGIN}) as ws:
            ws.send_json({'type':'hello','sample_rate':16000});ws.receive_json();ws.receive_json()
            assert make_ticket(c).status_code==503
            ws.send_json({'type':'hangup'})


def twilio_settings():
    return Settings(twilio_enabled=True,twilio_auth_token='secret',twilio_account_sid='AC1',
                    public_base_url='https://voice.example',greeting='Hello, test caller.')


def signed_webhook(c,s,call='CA1'):
    pairs=[('CallSid',call),('AccountSid','AC1')]
    sig=twilio_signature('https://voice.example/twilio/voice',pairs,s.twilio_auth_token)
    return c.post('/twilio/voice',data=dict(pairs),headers={'X-Twilio-Signature':sig})


def test_twilio_both_entrypoints_reject_unsigned_requests():
    with TestClient(create_app(twilio_settings())) as c:
        assert c.post('/twilio/voice',data={'CallSid':'CA1'}).status_code==403
        with pytest.raises(WebSocketDisconnect):
            with c.websocket_connect('/twilio/stream'):pass


def test_twilio_connect_start_pcm_mark_and_stop():
    s=twilio_settings()
    with TestClient(create_app(s)) as c:
        r=signed_webhook(c,s);assert r.status_code==200
        root=ET.fromstring(r.text);stream=root.find('Connect/Stream')
        assert stream.attrib['url']=='wss://voice.example/twilio/stream'
        token=stream.find('Parameter').attrib['value']
        sig=twilio_signature('https://voice.example/twilio/stream',[],s.twilio_auth_token)
        with c.websocket_connect('/twilio/stream',headers={'X-Twilio-Signature':sig}) as ws:
            ws.send_json({'event':'connected'})
            ws.send_json({'event':'start','start':{'streamSid':'MZ1','callSid':'CA1','accountSid':'AC1',
                'customParameters':{'ticket':token},'mediaFormat':{'encoding':'audio/x-mulaw','sampleRate':8000,'channels':1}}})
            media=0;marks=0
            for _ in range(30):
                msg=ws.receive_json()
                assert msg['streamSid']=='MZ1'
                if msg['event']=='media':
                    media+=1;assert len(base64.b64decode(msg['media']['payload']))==160
                elif msg['event']=='mark':
                    marks+=1;ws.send_json(msg)
                    if media==12:break
            assert media==12 and marks>=1
            ws.send_json({'event':'stop','streamSid':'MZ1'})


def test_twilio_cross_call_ticket_rejected_after_valid_signature():
    s=twilio_settings()
    with TestClient(create_app(s)) as c:
        token=ET.fromstring(signed_webhook(c,s).text).find('Connect/Stream/Parameter').attrib['value']
        sig=twilio_signature('https://voice.example/twilio/stream',[],s.twilio_auth_token)
        with c.websocket_connect('/twilio/stream',headers={'X-Twilio-Signature':sig}) as ws:
            ws.send_json({'event':'start','start':{'streamSid':'MZ1','callSid':'CA2','accountSid':'AC1',
                'customParameters':{'ticket':token},'mediaFormat':{'encoding':'audio/x-mulaw','sampleRate':8000,'channels':1}}})
            assert ws.receive()['type']=='websocket.close'


async def test_asterisk_real_tcp_paced_audio_and_clean_hangup():
    sessions=[]
    async def accept(r,w):
        t=AudioSocketTransport(r,w)
        s=VoiceSession(t,fake_providers(),Settings(greeting='A test greeting.'),Metrics())
        task=asyncio.create_task(s.run());sessions.append(task)
        await task
    server=await asyncio.start_server(accept,'127.0.0.1',0)
    reader,writer=await asyncio.open_connection('127.0.0.1',server.sockets[0].getsockname()[1])
    writer.write(b'\x01\x00\x10'+bytes(16));await writer.drain()
    times=[]
    for _ in range(8):
        hdr=await asyncio.wait_for(reader.readexactly(3),2)
        assert hdr[0]==0x10 and int.from_bytes(hdr[1:],'big')==320
        await reader.readexactly(320);times.append(asyncio.get_running_loop().time())
    assert times[-1]-times[0]>=.10
    writer.write(bytes(3));await writer.drain()
    writer.close();await writer.wait_closed();await asyncio.gather(*sessions)
    server.close();await server.wait_closed()


def test_turn_credentials_are_not_in_public_config():
    ice=[{'urls':['turn:relay.example:3478'],'username':'session-user','credential':'relay-secret'}]
    app=create_app(Settings(app_token='private',rtc_ice_servers_json=json.dumps(ice)))
    with TestClient(app) as c:
        app.state.rtc_available=True  # this test verifies ticket authorization, not aiortc
        public=c.get('/api/config')
        assert 'ice_servers' not in public.json() and 'relay-secret' not in public.text
        assert c.post('/api/tickets',headers={'Origin':ORIGIN},json={'transport':'webrtc'}).status_code==401
        granted=c.post('/api/tickets',headers={'Origin':ORIGIN,'Authorization':'Bearer private'},json={'transport':'webrtc'})
        assert granted.status_code==200 and granted.json()['ice_servers']==ice
