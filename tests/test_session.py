import asyncio
from types import SimpleNamespace
import numpy as np
import pytest
from duplex_voice.config import Settings
from duplex_voice.session import VoiceSession, Utterance
from duplex_voice.metrics import Metrics, Trace
from duplex_voice.concurrency import Overloaded
from .fakes import FakeTransport, fake_providers, wait_until


async def text_stream(text):
    yield text


def make_session(t=None,p=None,**kwargs):
    return VoiceSession(t or FakeTransport(),p or fake_providers(),Settings(greeting='',**kwargs),Metrics())


async def test_speak_paced_and_commits_only_completed_phrase():
    s=make_session(); s.history=[{'role':'user','content':'Hello'}]
    await s.speak(text_stream('A complete test phrase.'),None,Trace())
    assert s.history[-1]['content']=='A complete test phrase.'
    assert len(s.t.writes)==12
    assert s.t.writes[-1][3]-s.t.writes[0][3] >= .18


async def test_cancel_mid_tts_closes_generator_and_does_not_invent_words():
    done=asyncio.Event()
    class SlowTTS:
        sample_rate=24000
        async def synthesize(self,text):
            try:
                yield np.ones(480,np.float32)*.1
                await asyncio.sleep(30)
            finally: done.set()
    s=make_session(p=fake_providers(tts=SlowTTS()))
    s.history=[{'role':'user','content':'Question'}]
    s.response=s.spawn(s.speak(text_stream('These words are not all spoken.'),None,Trace()))
    old=s.response; await s.t.first_write.wait(); await s.interrupt()
    await asyncio.gather(old,return_exceptions=True)
    assert done.is_set() and s.t.clears
    assert s.history[-1]['content']=='[interrupted; no complete phrase confirmed]'
    assert not any('These words' in m['content'] for m in s.history)


async def test_response_remains_interruptible_after_generation_before_receipt():
    t=FakeTransport(ack=False);s=make_session(t=t)
    s.history=[{'role':'user','content':'Question'}]
    s.response=s.spawn(s.speak(text_stream('Do not claim I heard this.'),None,Trace()))
    old=s.response
    await wait_until(lambda:len(t.writes)==12)
    assert not old.done()
    await s.interrupt();await asyncio.gather(old,return_exceptions=True)
    assert len(t.clears)==1 and s.history[-1]['content'].startswith('[interrupted')


async def test_turn_resumption_preserves_pending_futures_and_merges():
    s=make_session();loop=asyncio.get_running_loop()
    u=Utterance(asyncio.Queue(8),loop.create_future());s.pending=[u]
    s.user_revision=1
    s.response=s.spawn(s.respond(1,[u],.1));old=s.response
    await asyncio.sleep(.01);s.user_revision=2;await s.interrupt()
    await asyncio.gather(old,return_exceptions=True)
    assert not u.final.cancelled()
    v=Utterance(asyncio.Queue(8),loop.create_future());s.pending.append(v)
    u.final.set_result('Move the appointment');v.final.set_result('to next Tuesday.')
    await s.respond(2,[u,v],.1)
    assert s.history[0]=={'role':'user','content':'Move the appointment to next Tuesday.'}
    assert s.pending==[]


async def test_audio_turn_ingress_asr_and_response():
    s=make_session(vad_start_ms=32,vad_end_ms_web=64,barge_in_start_ms=32)
    await s.on_frame(np.ones(512,np.float32)*.2)
    await s.on_frame(np.ones(512,np.float32)*.2)
    await s.on_frame(np.zeros(512,np.float32));await s.on_frame(np.zeros(512,np.float32))
    await s.response
    assert s.history[0]['role']=='user' and len(s.t.writes)>0
    for task in list(s.tasks): task.cancel()
    await asyncio.gather(*s.tasks,return_exceptions=True)


async def test_asr_queue_overload_fails_explicitly():
    s=make_session();u=Utterance(asyncio.Queue(1),asyncio.get_running_loop().create_future())
    s.feed_asr(u,np.zeros(512))
    with pytest.raises(Overloaded):s.feed_asr(u,np.zeros(512))


async def test_producer_failure_cancels_tts_child():
    closed=asyncio.Event()
    class Slow:
        sample_rate=24000
        async def synthesize(self,text):
            try:
                yield np.ones(480,np.float32)*.1
                await asyncio.sleep(20)
            finally: closed.set()
    async def broken():
        yield 'This begins normally. '
        await asyncio.sleep(.04)
        raise RuntimeError('provider failure')
    s=make_session(p=fake_providers(tts=Slow()))
    with pytest.raises(ExceptionGroup): await s.speak(broken(),None,Trace())
    assert closed.is_set()


async def test_token_budget_cancels_bounded_pipeline():
    s=make_session(max_reply_chars=32)
    with pytest.raises(ExceptionGroup): await s.speak(text_stream('x'*10000),None,Trace())
    assert len(s.t.writes)==0


async def test_call_hangup_joins_response_and_releases_transport():
    t=FakeTransport();s=make_session(t=t)
    task=asyncio.create_task(s.run())
    await t.input.put(None);await asyncio.wait_for(task,1)
    assert t.is_closed and not s.tasks


async def test_history_bound_is_real_not_only_prompt_slice():
    s=make_session(max_history_chars=128,max_history_messages=4)
    s.history=[{'role':'user' if i%2==0 else 'assistant','content':'x'*40} for i in range(20)]
    s.trim_history()
    assert sum(len(m['content']) for m in s.history)<=128
    assert s.history[0]['role']=='user'


async def test_one_assistant_commit_per_epoch():
    s=make_session();s.history=[{'role':'user','content':'Question'}]
    await s.speak(text_stream('Keep this phrase once.'),None,Trace())
    s.commit_assistant();s.commit_assistant(interrupted=True)
    assert len(s.history)==2


class PhraseTTS:
    """Each phrase: `delay` before the first chunk, then 10 x 20 ms; records start/close."""
    sample_rate=24000
    def __init__(self,delay=0.,fail=(),stall=()):
        self.delay,self.fail,self.stall=delay,fail,stall;self.started=[];self.closed=[]
    async def synthesize(self,text):
        self.started.append((text,asyncio.get_running_loop().time()))
        try:
            await asyncio.sleep(self.delay)
            if text in self.fail: raise RuntimeError('provider failed this phrase')
            for i in range(10):
                yield np.ones(480,np.float32)*.1
                if text in self.stall and i==2: await asyncio.sleep(30)
        finally: self.closed.append(text)


def largest_write_gap(t):
    times=[w[3] for w in t.writes]
    return max(b-a for a,b in zip(times,times[1:]))


@pytest.mark.parametrize('lookahead',[0,1])
async def test_tts_lookahead_hides_next_phrase_latency(lookahead):
    tts=PhraseTTS(delay=.15)
    s=make_session(p=fake_providers(tts=tts),output_queue_frames=2,tts_lookahead=lookahead)
    await s.speak(text_stream('First phrase is here. Second phrase is here.'),None,Trace())
    assert [x[0] for x in tts.started]==['First phrase is here.','Second phrase is here.']
    assert len(s.t.writes)==20
    if lookahead:
        assert largest_write_gap(s.t)<.07   # second phrase arrived during the first
    else:
        assert largest_write_gap(s.t)>.09   # sequential request: its delay is audible


async def test_prefetched_phrase_failure_waits_for_earlier_audio():
    tts=PhraseTTS(fail={'Second phrase is here.'})
    s=make_session(p=fake_providers(tts=tts),output_queue_frames=2)
    s.history=[{'role':'user','content':'Question'}]
    with pytest.raises(ExceptionGroup): await s.speak(text_stream('First phrase is here. Second phrase is here.'),None,Trace())
    assert len(s.t.writes)==10   # every frame of the complete first phrase was dispatched
    assert s.history[-1]['content']=='First phrase is here. [interrupted]'


async def test_interrupt_closes_prefetched_tts_stream():
    tts=PhraseTTS(stall={'First phrase is here.'})
    s=make_session(p=fake_providers(tts=tts))
    s.history=[{'role':'user','content':'Question'}]
    s.response=s.spawn(s.speak(text_stream('First phrase is here. Second phrase is here.'),None,Trace()))
    old=s.response
    await wait_until(lambda:len(tts.started)==2 and len(s.t.writes)>=3)
    await s.interrupt();await asyncio.gather(old,return_exceptions=True)
    assert sorted(tts.closed)==['First phrase is here.','Second phrase is here.']
