"""Offline configuration checks, optionally probing already-running model services."""
import argparse
import asyncio
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import sys
import httpx
from duplex_voice.config import Settings
from scripts.download_models import sha256


async def run(args):
    s=Settings.from_env();errors=[]
    print(json.dumps({'python':sys.version.split()[0],'mode':s.voice_mode,'deployment':s.deployment,
                      'session_limit':s.max_sessions,'provider_secrets':'never printed'}))
    for module in ['fastapi','numpy','soxr','websockets']+(['onnxruntime'] if s.effective_vad=='silero' else []):
        present=importlib.util.find_spec(module) is not None
        print(f'{module}: {"present" if present else "MISSING"}')
        if not present:errors.append(module)
    if s.effective_vad=='silero' and not Path(s.vad_model_path).is_file():errors.append(s.vad_model_path)
    if args.manifest:
        path=Path(args.manifest);m=json.loads(path.read_text())
        for model in m['models']:
            for file in model['files']:
                p=path.parent/file['file']
                if not p.is_file() or sha256(p)!=file['sha256']:errors.append(f'bad hash: {p}')
        print('Model manifest verified' if not errors else 'Check errors below')
    if args.probe and s.voice_mode!='demo':
        async with httpx.AsyncClient(timeout=5) as c:
            urls=[]
            if s.asr_provider=='speech':urls.append(((s.asr_base_url or s.speech_base_url)+'/readyz',s.speech_token))
            if s.tts_provider=='speech':urls.append(((s.tts_base_url or s.speech_base_url)+'/readyz',s.speech_token))
            urls=list(dict.fromkeys(urls))
            urls.append((s.llm_base_url.rstrip('/')+'/models',s.llm_api_key))
            for url,key in urls:
                try:
                    r=await c.get(url,headers={'Authorization':'Bearer '+key} if key else {})
                    r.raise_for_status();print(f'{url}: HTTP {r.status_code}')
                    if url.endswith('/models'):
                        ids=[m['id'] for m in r.json().get('data',[])]
                        if s.llm_model not in ids:errors.append('LLM_MODEL not listed by model server')
                except Exception as e:errors.append(f'probe failed: {url}: {type(e).__name__}')
    for item in errors:print('ERROR:',item)
    print('This does not establish WER, TTS quality, realtime capacity, or phone connectivity.')
    return bool(errors)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--probe',action='store_true');p.add_argument('--manifest')
    raise SystemExit(asyncio.run(run(p.parse_args())))
