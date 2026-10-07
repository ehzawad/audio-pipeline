import asyncio
from types import SimpleNamespace
import numpy as np
from duplex_voice.transports.base import Transport
from duplex_voice.providers.clients import DemoASR, DemoLLM, DemoTTS
from duplex_voice.vad import EnergyVAD


class FakeTransport(Transport):
    name='test'; evidence='synthetic_ack'
    def __init__(self, ack=True):
        super().__init__(); self.ack=ack; self.writes=[]; self.events=[]; self.clears=[]
        self.input=asyncio.Queue(); self.first_write=asyncio.Event(); self.is_closed=False
    async def frames(self):
        while (item:=await self.input.get()) is not None: yield item
    async def write(self, samples, epoch, seq):
        if epoch!=self.ledger.epoch: raise asyncio.CancelledError
        self.writes.append((epoch,seq,len(samples),asyncio.get_running_loop().time()))
        self.first_write.set()
        if self.ack: self.ledger.acknowledge(epoch,seq)
    async def clear(self,epoch): self.clears.append(epoch)
    async def event(self,event): self.events.append(event)
    async def close(self):
        self.is_closed=True; await super().close()


def fake_providers(asr=None,llm=None,tts=None):
    return SimpleNamespace(asr=asr or DemoASR(),llm=llm or DemoLLM(),tts=tts or DemoTTS(),make_vad=EnergyVAD)


async def wait_until(predicate,timeout=2):
    async with asyncio.timeout(timeout):
        while not predicate(): await asyncio.sleep(.005)
