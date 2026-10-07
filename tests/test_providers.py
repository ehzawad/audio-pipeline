import asyncio
import base64
import json
from types import SimpleNamespace
import numpy as np
import pytest
import httpx
import websockets
from fastapi.testclient import TestClient
from duplex_voice.audio import float_to_pcm16
from duplex_voice.config import Settings
from duplex_voice.providers.clients import (CompatibleLLM, ResponsesLLM, RemoteTTS, RemoteASR, DeepgramASR, OpenAIRealtimeASR,
                                           OpenAITTS, sse_objects)
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


async def test_llm_reasoning_effort_is_opt_in():
    captured=[]
    def handle(request):captured.append(json.loads(request.content));return httpx.Response(200,content=b'data: [DONE]\n\n')
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        for effort in ('','none'):
            async for _ in CompatibleLLM(Settings(llm_reasoning_effort=effort),client).stream([],'policy'):pass
    assert 'reasoning_effort' not in captured[0] and captured[1]['reasoning_effort']=='none'


def openai_server(events_after_commit,seen):
    """Speaks the realtime transcription protocol: deltas before commit, final after it."""
    async def handler(ws):
        seen.append(ws.request.path);seen.append(ws.request.headers.get('Authorization'))
        update=json.loads(await ws.recv());seen.append(update)
        async for raw in ws:
            msg=json.loads(raw)
            if msg['type']=='input_audio_buffer.append':
                seen.append(len(base64.b64decode(msg['audio'])))
                await ws.send(json.dumps({'type':'conversation.item.input_audio_transcription.delta','item_id':'i1','delta':' Book'}))
            elif msg['type']=='input_audio_buffer.commit':
                await ws.send(json.dumps({'type':'input_audio_buffer.committed','item_id':'i1'}))
                for event in events_after_commit:await ws.send(json.dumps(event))
                await asyncio.sleep(.05);return
    return handler


async def test_openai_asr_commits_once_and_takes_only_the_committed_items_final():
    seen=[];partials=[]
    events=[{'type':'conversation.item.input_audio_transcription.completed','item_id':'other','transcript':'Wrong turn.'},
            {'type':'conversation.item.input_audio_transcription.completed','item_id':'i1','transcript':' Book a table. '}]
    async with websockets.serve(openai_server(events,seen),'127.0.0.1',0) as server:
        port=server.sockets[0].getsockname()[1]
        asr=OpenAIRealtimeASR(Settings(openai_api_key='test',openai_realtime_url=f'ws://127.0.0.1:{port}/v1/realtime'))
        assert await asr.recognize(audio_source(),partials.append)=='Book a table.'
    assert seen[0]=='/v1/realtime?intent=transcription' and seen[1]=='Bearer test'
    session=seen[2]['session']['audio']['input']
    assert session['turn_detection'] is None and session['format']=={'type':'audio/pcm','rate':24000}
    assert session['transcription']=={'model':'gpt-live-transcribe','languages':['en']}
    # 2 x 512 samples at 16 kHz become 1536 samples at 24 kHz (some held until flush).
    assert sum(seen[3:])==1536*2 and partials[-1].startswith('Book')


def test_openai_asr_session_fields_follow_the_model_family():
    live=OpenAIRealtimeASR(Settings(openai_api_key='t',openai_asr_delay='minimal')).transcription()
    assert live=={'model':'gpt-live-transcribe','languages':['en'],'delay':'minimal'}
    legacy=OpenAIRealtimeASR(Settings(openai_api_key='t',openai_asr_model='gpt-4o-transcribe',openai_asr_delay='low')).transcription()
    assert legacy=={'model':'gpt-4o-transcribe','language':'en'}   # rejects `languages`


def sse(*events):
    return BytesStream([b''.join(b'event: x\ndata: '+json.dumps(e).encode()+b'\n\n' for e in events)])


async def responses_run(events,**kw):
    captured=[];stream=sse(*events)
    def handle(request):captured.append((str(request.url),json.loads(request.content)));return httpx.Response(200,stream=stream)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        llm=ResponsesLLM(Settings(llm_api='responses',llm_base_url='https://api.openai.com/v1',**kw),client)
        out=[x async for x in llm.stream([{'role':'user','content':'Hi'},{'role':'assistant','content':'Hello.'}],'policy')]
    return out,captured,stream


async def test_responses_adapter_payload_and_text_deltas():
    out,captured,stream=await responses_run([
        {'type':'response.created'},{'type':'response.output_item.added','item':{'type':'message'}},
        {'type':'response.output_text.delta','delta':'Tokyo'},{'type':'response.output_text.delta','delta':' it is.'},
        {'type':'response.completed'}],llm_send_temperature=False,llm_reasoning_effort='none',llm_max_tokens=200)
    assert ''.join(out)=='Tokyo it is.' and stream.closed
    url,body=captured[0]
    assert url=='https://api.openai.com/v1/responses' and body['store'] is False and body['instructions']=='policy'
    assert body['input'][1]=={'role':'assistant','content':'Hello.'} and body['reasoning']=={'effort':'none'}
    assert body['max_output_tokens']==200 and 'temperature' not in body


@pytest.mark.parametrize('event',[{'type':'error'},{'type':'response.failed'},
                                  {'type':'response.output_item.added','item':{'type':'function_call'}}])
async def test_responses_adapter_fails_explicitly(event):
    with pytest.raises(RuntimeError):
        await responses_run([{'type':'response.output_text.delta','delta':'Partial'},event])


async def test_openai_asr_provider_error_fails_the_utterance():
    events=[{'type':'conversation.item.input_audio_transcription.failed','item_id':'i1'}]
    async with websockets.serve(openai_server(events,[]),'127.0.0.1',0) as server:
        port=server.sockets[0].getsockname()[1]
        asr=OpenAIRealtimeASR(Settings(openai_api_key='test',openai_realtime_url=f'ws://127.0.0.1:{port}'))
        with pytest.raises(ExceptionGroup):await asr.recognize(audio_source(),lambda x:None)


async def test_openai_asr_close_without_final_is_not_partial_success():
    async with websockets.serve(openai_server([],[]),'127.0.0.1',0) as server:
        port=server.sockets[0].getsockname()[1]
        asr=OpenAIRealtimeASR(Settings(openai_api_key='test',openai_realtime_url=f'ws://127.0.0.1:{port}'))
        with pytest.raises(ExceptionGroup):await asr.recognize(audio_source(),lambda x:None)


async def test_openai_tts_payload_odd_chunks_and_media_type():
    b=float_to_pcm16(np.linspace(-.1,.1,53,dtype=np.float32));captured=[]
    stream=BytesStream([b[i:i+7] for i in range(0,len(b),7)])
    def handle(request):
        captured.append((str(request.url),request.headers['authorization'],json.loads(request.content)))
        return httpx.Response(200,headers={'Content-Type':'audio/pcm'},stream=stream)
    s=Settings(openai_api_key='test',openai_tts_instructions='Calm.')
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        audio=np.concatenate([x async for x in OpenAITTS(s,client).synthesize('A phrase')])
    assert float_to_pcm16(audio)==b and stream.closed
    url,auth,body=captured[0]
    assert url=='https://api.openai.com/v1/audio/speech' and auth=='Bearer test'
    assert body=={'model':'gpt-4o-mini-tts','voice':'cedar','input':'A phrase','response_format':'pcm','instructions':'Calm.'}


async def test_openai_tts_rejects_compressed_audio():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,headers={'Content-Type':'audio/mpeg'},content=b'ID3'))) as client:
        with pytest.raises(ValueError):
            async for _ in OpenAITTS(Settings(openai_api_key='test'),client).synthesize('Text'):pass


def test_openai_providers_require_a_key():
    with pytest.raises(ValueError):OpenAIRealtimeASR(Settings())
    with pytest.raises(ValueError):OpenAITTS(Settings(),None)


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
