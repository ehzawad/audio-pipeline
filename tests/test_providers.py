import asyncio
import json
from types import SimpleNamespace
import numpy as np
import pytest
import httpx
import websockets
from fastapi.testclient import TestClient
from duplex_voice.audio import float_to_pcm16
from duplex_voice.config import Settings
from duplex_voice.providers.clients import CompatibleLLM, RemoteTTS, RemoteASR, DeepgramASR, sse_objects
from duplex_voice.speech_service import create_speech_app
from duplex_voice.outbound import DialStore, dial


class BytesStream(httpx.AsyncByteStream):
    def __init__(self,chunks):self.chunks=chunks;self.closed=False
    async def __aiter__(self):
        for c in self.chunks:yield c
    async def aclose(self):self.closed=True


async def test_sse_multiline_and_http_boundary_independence():
    stream=BytesStream([b':comment\ndata: {"a":',b'\ndata: 2}\n\ndata: [DONE]\n\n'])
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,stream=stream))) as client:
        async with client.stream('GET','https://test') as response:
            result=[x async for x in sse_objects(response)]
    assert result==[{'a':2}] and stream.closed


async def test_llm_adapter_payload_and_stream_close():
    captured=[];stream=BytesStream([b'data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n',b'data: [DONE]\n\n'])
    def handle(request):captured.append(json.loads(request.content));return httpx.Response(200,stream=stream)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        llm=CompatibleLLM(Settings(llm_token_parameter='max_completion_tokens',llm_send_temperature=False),client)
        assert ''.join([x async for x in llm.stream([{'role':'user','content':'Hi'}],'policy')])=='Hello'
    assert 'max_completion_tokens' in captured[0] and 'temperature' not in captured[0]
    assert captured[0]['messages'][0]['role']=='system' and stream.closed


async def test_llm_tool_call_is_not_silently_executed():
    data=b'data: {"choices":[{"delta":{"tool_calls":[{}]}}]}\n\n'
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,content=data))) as client:
        with pytest.raises(RuntimeError):
            async for _ in CompatibleLLM(Settings(),client).stream([],'policy'):pass


async def test_tts_odd_http_chunks_and_format_validation():
    b=float_to_pcm16(np.linspace(-.1,.1,53,dtype=np.float32))
    stream=BytesStream([b[i:i+7] for i in range(0,len(b),7)])
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,headers={'X-Sample-Rate':'24000'},stream=stream))) as client:
        audio=np.concatenate([x async for x in RemoteTTS(Settings(),client).synthesize('A phrase')])
        assert float_to_pcm16(audio)==b
    assert stream.closed


async def test_tts_wrong_sample_rate_rejected():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,headers={'X-Sample-Rate':'8000'},content=b'00'))) as client:
        with pytest.raises(ValueError):
            async for _ in RemoteTTS(Settings(),client).synthesize('Text'):pass


async def audio_source():
    yield np.zeros(512,np.float32)
    await asyncio.sleep(.005)
    yield np.zeros(512,np.float32)


async def test_remote_asr_against_real_local_websocket_server():
    async def handler(ws):
        frames=0
        async for data in ws:
            if isinstance(data,bytes):
                frames+=1;await ws.send(json.dumps({'type':'partial','text':'Book'}))
            else:
                assert json.loads(data)=={'type':'finish'} and frames==2
                await ws.send(json.dumps({'type':'final','text':'Book a table.'}));return
    async with websockets.serve(handler,'127.0.0.1',0) as server:
        port=server.sockets[0].getsockname()[1];partials=[]
        asr=RemoteASR(Settings(speech_base_url=f'http://127.0.0.1:{port}'))
        assert await asr.recognize(audio_source(),partials.append)=='Book a table.'
        assert partials==['Book','Book']


async def test_deepgram_close_stream_fence_via_local_protocol_server():
    paths=[]
    async def handler(ws):
        paths.append(ws.request.path)
        async for data in ws:
            if isinstance(data,bytes):
                await ws.send(json.dumps({'type':'Results','is_final':False,'channel':{'alternatives':[{'transcript':'Book'}]}}))
            else:
                assert json.loads(data)=={'type':'CloseStream'}
                await ws.send(json.dumps({'type':'Results','is_final':True,'channel':{'alternatives':[{'transcript':'Book a table.'}]}}))
                await ws.send(json.dumps({'type':'Metadata'}));return
    async with websockets.serve(handler,'127.0.0.1',0) as server:
        port=server.sockets[0].getsockname()[1]
        asr=DeepgramASR(Settings(deepgram_api_key='test',deepgram_url=f'ws://127.0.0.1:{port}/v1/listen'))
        assert await asr.recognize(audio_source(),lambda x:None)=='Book a table.'
        assert 'endpointing=false' in paths[0] and 'sample_rate=16000' in paths[0]


async def test_deepgram_missing_fence_fails_not_partial_success():
    async def handler(ws):
        async for data in ws:
            if not isinstance(data,bytes):return
    async with websockets.serve(handler,'127.0.0.1',0) as server:
        port=server.sockets[0].getsockname()[1]
        asr=DeepgramASR(Settings(deepgram_api_key='test',deepgram_url=f'ws://127.0.0.1:{port}'))
        with pytest.raises(ExceptionGroup):await asr.recognize(audio_source(),lambda x:None)


class FakeNative:
    def __init__(self): self.asr=SimpleNamespace(create_stream=lambda:object())
    def decode(self,stream,samples,finished=False):return 'Book a table.' if finished else 'Book'
    def synthesize(self,text):return np.zeros(960,np.float32)


def test_speech_worker_routes_with_injected_native_models(monkeypatch):
    monkeypatch.setenv('SPEECH_TOKEN','test-token')
    with TestClient(create_speech_app(FakeNative)) as c:
        assert c.post('/tts',json={'text':'hello'}).status_code==401
        h={'Authorization':'Bearer test-token'}
        r=c.post('/tts',json={'text':'hello'},headers=h)
        assert r.status_code==200 and r.headers['x-sample-rate']=='24000' and len(r.content)==1920
        assert c.get('/readyz').json()['active']==0
        with c.websocket_connect('/asr',headers=h) as ws:
            ws.send_bytes(bytes(1024));assert ws.receive_json()=={'type':'partial','text':'Book'}
            ws.send_json({'type':'finish'});assert ws.receive_json()=={'type':'final','text':'Book a table.'}
        assert c.post('/tts',json={'text':'x'*1000},headers=h).status_code==422


def test_outbound_reservation_is_durable_and_semantic(tmp_path):
    filename=str(tmp_path/'dial.db');store=DialStore(filename)
    assert store.reserve('key','+12025550123')[1]
    assert not DialStore(filename).reserve('key','+12025550123')[1]
    with pytest.raises(ValueError):store.reserve('key','+12025550124')


async def test_outbound_lost_response_is_not_redialed(tmp_path):
    calls=[]
    def request(req):calls.append(req);raise httpx.ReadTimeout('lost response')
    s=Settings();store=DialStore(str(tmp_path/'dial.db'))
    async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as client:
        with pytest.raises(httpx.ReadTimeout):await dial(s,store,client,'key','+12025550123')
        assert (await dial(s,store,client,'key','+12025550123'))['state']=='unknown'
    assert len(calls)==1
