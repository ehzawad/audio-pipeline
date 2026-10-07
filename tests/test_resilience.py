import asyncio
import pytest
from duplex_voice.resilience import Circuit, GuardedLLM
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
