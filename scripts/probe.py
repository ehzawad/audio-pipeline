"""Run a consented WAV through real configured providers; write reply WAV + timing JSON.

This is a provider-path measurement, not browser/PSTN end-to-end latency.
"""
import argparse
import asyncio
import json
from pathlib import Path
import time
import wave
import numpy as np
from duplex_voice.audio import pcm16_to_float,float_to_pcm16,Resampler
from duplex_voice.config import Settings
from duplex_voice.providers import Providers
from duplex_voice.text import SentenceChunker,VOICE_SYSTEM_PROMPT,clean_for_tts


async def run(args):
    s=Settings.from_env()
    if s.voice_mode=='demo' and not args.allow_demo:raise ValueError('Select a real model profile, or explicitly pass --allow-demo')
    with wave.open(str(args.input),'rb') as w:
        if w.getsampwidth()!=2 or w.getnchannels()!=1:raise ValueError('Input must be mono 16-bit PCM WAV')
        rate=w.getframerate();x=pcm16_to_float(w.readframes(w.getnframes()))
    rs=Resampler(rate,16000);x=np.concatenate([rs.process(x),rs.flush()])
    if not 0<len(x)<=120*16000:raise ValueError('Use a nonempty recording shorter than 120 seconds')
    p=Providers(s);marks={};start=time.perf_counter();eof=None
    async def input_audio():
        nonlocal eof
        for i in range(0,len(x),512):
            yield x[i:i+512]
            if not args.fast:await asyncio.sleep(len(x[i:i+512])/16000)
        eof=time.perf_counter()
    try:
        async with asyncio.timeout(180):
            text=await p.asr.recognize(input_audio(),lambda _:None)
            marks['asr_residual_after_upload_s']=time.perf_counter()-eof
            if not text:raise ValueError('ASR produced an empty transcript')
            chunker=SentenceChunker();out=[];spoken=[]
            async def utter(text):
                spoken.append(text)
                async for samples in p.tts.synthesize(text):
                    marks.setdefault('first_audio_since_start_s',time.perf_counter()-start)
                    out.append(samples)
            async for token in p.llm.stream([{'role':'user','content':text}],VOICE_SYSTEM_PROMPT):
                marks.setdefault('first_token_since_start_s',time.perf_counter()-start)
                for phrase in chunker.push(token):await utter(clean_for_tts(phrase))
            for phrase in chunker.flush():await utter(clean_for_tts(phrase))
            if not out:raise ValueError('No synthesized audio')
        with wave.open(str(args.output),'wb') as w:
            w.setnchannels(1);w.setsampwidth(2);w.setframerate(p.tts.sample_rate)
            w.writeframes(float_to_pcm16(np.concatenate(out)))
        report={'scope':'provider probe; sequential phrase consumption, no turn detector or transport',
                'mode':s.voice_mode,'realtime_upload':not args.fast,'input_seconds':len(x)/16000,
                'output_seconds':sum(map(len,out))/p.tts.sample_rate,'timings':marks}
        if args.include_text:report.update(transcript=text,reply=' '.join(spoken))
        args.output.with_suffix('.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report,indent=2))
    finally:await p.close()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('input',type=Path)
    p.add_argument('--output',type=Path,default=Path('reply.wav'));p.add_argument('--fast',action='store_true')
    p.add_argument('--allow-demo',action='store_true');p.add_argument('--include-text',action='store_true')
    asyncio.run(run(p.parse_args()))
