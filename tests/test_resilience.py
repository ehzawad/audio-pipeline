import asyncio
import numpy as np
import pytest
from duplex_voice.resilience import Circuit, GuardedLLM, HedgedTTS
from duplex_voice.concurrency import Overloaded
from duplex_voice.control.service import Grants
from duplex_voice.control.identity import issue_identity, authenticate
from duplex_voice.config import Settings


async def fail(gate):
    with pytest.raises(RuntimeError):
        async with gate.request():raise RuntimeError('provider failed')


async def test_bulkhead_rejects_without_a_waiting_queue():
    c=Circuit(capacity=1)
    async with c.request():
        with pytest.raises(Overloaded):
            async with c.request():pass
    assert c.active==0


async def test_open_circuit_admits_one_half_open_probe():
    clock=[0];c=Circuit(threshold=2,reset_seconds=1,clock=lambda:clock[0])
    await fail(c);await fail(c);assert c.state=='open'
    with pytest.raises(Overloaded):
        async with c.request():pass
    clock[0]=2
    async with c.request():
        assert c.state=='half_open'
        with pytest.raises(Overloaded):
            async with c.request():pass
    assert c.state=='closed' and c.active==0


async def test_old_success_cannot_close_newer_open_circuit():
    c=Circuit(capacity=2,threshold=1)
    older=c.request();await older.__aenter__()
    await fail(c);assert c.state=='open'
    await older.__aexit__(None,None,None)
    assert c.state=='open' and c.active==0


async def test_human_interruption_is_not_a_provider_failure():
    c=Circuit(threshold=1)
    with pytest.raises(asyncio.CancelledError):
        async with c.request():raise asyncio.CancelledError
    assert c.state=='closed' and c.active==0 and c.failures==0


async def test_failed_half_open_probe_reopens():
    clock=[0];c=Circuit(threshold=1,reset_seconds=1,clock=lambda:clock[0])
    await fail(c);clock[0]=2;await fail(c)
    assert c.state=='open' and c.until==3


async def test_abandoned_generator_releases_gate_and_closes_provider():
    closed=[]
    class LLM:
        async def stream(self,messages,system):
            try:
                yield 'first'
                await asyncio.sleep(60)
            finally:closed.append(True)
    c=Circuit();stream=GuardedLLM(LLM(),c).stream([],'')
    assert await anext(stream)=='first' and c.active==1
    await stream.aclose()
    assert c.active==0 and c.failures==0 and closed


def test_application_identity_is_home_cell_scoped():
    secret='i'*32;token=issue_identity(secret,'tenant','person',cell='east')
    assert authenticate(Settings(identity_secret=secret,cell_id='east'),'Bearer '+token).tenant=='tenant'
    with pytest.raises(ValueError):authenticate(Settings(identity_secret=secret,cell_id='west'),'Bearer '+token)


def test_audio_seconds_cannot_be_flooded_faster_than_real_time():
    from duplex_voice.resilience import RateBudget
    clock=[0];budget=RateBudget(1.1,.5,clock=lambda:clock[0])
    budget.consume(.4)
    with pytest.raises(Overloaded):budget.consume(.2)
    clock[0]=.2;budget.consume(.2)
    clock[0]=100;budget.consume(.5)
    with pytest.raises(Overloaded):budget.consume(.01)


class ScriptedTTS:
    """Attempt n follows plan[n]: 'fast', 'stall' (head chunk, then hangs) or 'error'."""
    sample_rate=1000
    def __init__(self,*plan): self.plan=list(plan);self.calls=0;self.closed=[]
    async def synthesize(self,text):
        n=self.calls;self.calls+=1;kind=self.plan[n]
        try:
            if kind=='error':
                await asyncio.sleep(.01);raise RuntimeError('provider error')
            yield np.full(200,n+1,np.float32)   # 0.2 s head chunk
            await asyncio.sleep(30 if kind=='stall' else .05)
            for _ in range(3): yield np.full(200,n+1,np.float32)
        finally: self.closed.append(n)


async def collect(tts):
    loop=asyncio.get_running_loop();start=loop.time();out=[];first=None
    async for x in tts.synthesize('phrase'):
        first=first if first is not None else loop.time()-start;out.append(x)
    return np.concatenate(out),first


async def test_prebuffer_holds_a_phrase_until_enough_audio_arrived():
    inner=ScriptedTTS('fast')
    audio,first=await collect(HedgedTTS(inner,prebuffer_s=.4))
    assert len(audio)==800 and first>=.04   # the 0.2 s head alone was not released
    assert inner.calls==1


async def test_hedge_replaces_a_stalled_attempt_and_yields_one_attempt_only():
    inner=ScriptedTTS('stall','fast')
    audio,_=await asyncio.wait_for(collect(HedgedTTS(inner,prebuffer_s=.4,hedge_after_s=.05)),2)
    assert inner.calls==2 and set(audio.tolist())=={2.0} and len(audio)==800
    assert sorted(inner.closed)==[0,1]   # the unheard loser was closed, not abandoned


async def test_no_hedge_when_the_first_attempt_is_on_time():
    inner=ScriptedTTS('fast','fast')
    await collect(HedgedTTS(inner,prebuffer_s=.2,hedge_after_s=1))
    assert inner.calls==1


async def test_provider_error_is_not_retried_without_a_running_hedge():
    inner=ScriptedTTS('error','fast')
    with pytest.raises(RuntimeError):await collect(HedgedTTS(inner,prebuffer_s=.4,hedge_after_s=1))
    assert inner.calls==1


async def test_running_hedge_survives_the_first_attempt_failing():
    class Slow(ScriptedTTS):
        async def synthesize(self,text):
            if self.calls==0:
                self.calls+=1;await asyncio.sleep(.1);raise RuntimeError('late provider error')
                yield
            async for x in super().synthesize(text):yield x
    inner=Slow(None,'fast')
    audio,_=await asyncio.wait_for(collect(HedgedTTS(inner,prebuffer_s=.4,hedge_after_s=.02)),2)
    assert set(audio.tolist())=={2.0}


async def test_closing_the_consumer_closes_every_attempt():
    inner=ScriptedTTS('stall','stall')
    stream=HedgedTTS(inner,prebuffer_s=.4,hedge_after_s=.02).synthesize('phrase')
    task=asyncio.create_task(anext(stream))
    await asyncio.sleep(.1);task.cancel()
    await asyncio.gather(task,return_exceptions=True);await stream.aclose()
    assert inner.calls==2 and sorted(inner.closed)==[0,1]
