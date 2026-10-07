"""Failure paths added at the fleet/client boundary; no live neural inference."""
import asyncio
from unittest.mock import Mock
import json
import struct
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from duplex_voice.config import Settings
from duplex_voice.main import create_app
from duplex_voice.http_limits import BodyLimit
from duplex_voice.metrics import Metrics
from duplex_voice.providers.clients import RemoteTTS
from duplex_voice.speech_service import create_speech_app
from .test_providers import FakeNative


def test_prometheus_histograms_are_aggregatable_and_identity_is_not_a_label():
    m = Metrics();m.count('response.completed');m.observe('websocket.end_speech_to_dispatch', .12)
    output=m.prometheus(2,False).decode()
    assert 'audio_pipeline_stage_seconds_bucket' in output
    assert 'audio_pipeline_active_sessions 2.0' in output
    assert 'session_id' not in output and 'tenant' not in output


def test_metrics_route_requires_operator_not_application_identity():
    with TestClient(create_app(Settings(app_token='operator'))) as c:
        assert c.get('/metrics/prometheus').status_code == 401
        r=c.get('/metrics/prometheus',headers={'Authorization':'Bearer operator'})
        assert r.status_code==200 and 'audio_pipeline_active_sessions' in r.text


async def test_body_timeout_does_not_enter_application():
    entered=False;sent=[]
    async def app(*args):
        nonlocal entered;entered=True
    async def receive():await asyncio.sleep(60)
    async def send(message):sent.append(message)
    await BodyLimit(app,timeout=.01)({'type':'http'},receive,send)
    assert not entered and sent[0]['status']==408


@pytest.mark.parametrize('role,disabled', [('asr','tts'),('tts','asr')])
def test_speech_role_exposes_only_its_worker_capability(monkeypatch,role,disabled):
    monkeypatch.setenv('SPEECH_ROLE',role);monkeypatch.setenv('SPEECH_TOKEN','')
    with TestClient(create_speech_app(FakeNative)) as c:
        assert c.get('/readyz').json()[disabled] is None
        if disabled=='tts':assert c.post('/tts',json={'text':'Hello'}).status_code==404
        else:
            with pytest.raises(Exception):
                with c.websocket_connect('/asr'):pass
        c.app.state.cap.draining=True
        assert c.get('/readyz').status_code==503


async def test_tts_can_scale_independently_with_separate_url():
    seen=[]
    def handle(request):
        seen.append(str(request.url))
        return httpx.Response(200,headers={'x-sample-rate':'24000'},content=b'\0\0'*480)
    s=Settings(tts_base_url='https://tts.example',speech_base_url='https://combined.example')
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        chunks=[chunk async for chunk in RemoteTTS(s,client).synthesize('Hello')]
    assert seen==['https://tts.example/tts'] and sum(map(len,chunks))==480


def test_same_protocol_fixture_is_decodable_by_python():
    fixtures=json.loads((Path(__file__).parents[1]/'clients/fixtures/protocol.json').read_text())
    for example in fixtures['packets']:
        raw=bytes.fromhex(example['hex'])
        assert struct.unpack('<II',raw[:8])==(example['epoch'],example['seq'])
        assert list(struct.unpack('<'+'h'*len(example['samples']),raw[8:]))==example['samples']


def test_redis_configuration_refuses_process_random_signing_keys():
    with pytest.raises(ValueError):Settings(state_backend='redis')
    assert Settings(state_backend='redis',signing_key='x'*32).state_backend=='redis'


async def test_emergency_prompt_is_self_contained_and_bounded():
    import numpy as np
    from duplex_voice.providers.recorded import ServiceUnavailable
    fallback=ServiceUnavailable()
    chunks=[x async for x in fallback.synthesize(fallback.text)]
    audio=np.concatenate(chunks)
    assert 3 < len(audio)/fallback.sample_rate < 6
    assert np.isfinite(audio).all() and np.max(np.abs(audio)) > .01
    assert max(map(len,chunks))==160
