"""Download only selected HF artifacts, resolve revision IDs, and hash their bytes.

Run from the repository root. --reuse-manifest downloads the SAME revisions again.
This is provenance, not a security audit or a complete dependency lock.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone


def sha256(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        while b:=f.read(1024*1024):h.update(b)
    return h.hexdigest()


def main():
    from huggingface_hub import HfApi, hf_hub_download
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path('models'))
    parser.add_argument('--vad-only',action='store_true')
    parser.add_argument('--gguf',action='store_true',help='Also download the community Q4_K_M LLM artifact')
    parser.add_argument('--voice',default='af_heart')
    parser.add_argument('--reuse-manifest',type=Path)
    args=parser.parse_args()
    if not args.voice.replace('_','').isalnum():parser.error('invalid voice name')
    plans=[('mediainbox/silero-vad-onnx','',['silero_vad.onnx'])]
    suffix='epoch-99-avg-1-chunk-16-left-128.int8.onnx'
    if not args.vad_only:
        plans += [('csukuangfj/sherpa-onnx-streaming-zipformer-en-2023-06-26','zipformer',
                   [f'{k}-{suffix}' for k in ('encoder','decoder','joiner')]+['tokens.txt']),
                  ('hexgrad/Kokoro-82M','kokoro',['config.json','kokoro-v1_0.pth',f'voices/{args.voice}.pt'])]
    previous=json.loads(args.reuse_manifest.read_text()) if args.reuse_manifest else None
    api=HfApi(); rows=[]
    for repo,directory,files in plans:
        if previous:
            found=next((m for m in previous['models'] if m['repo']==repo),None)
            if not found:raise ValueError(f'{repo} absent from reuse manifest')
            revision=found['revision']
        else:revision=api.model_info(repo).sha
        rows.append(download(api,hf_hub_download,args.root,repo,revision,directory,files))
    if args.gguf:
        repo='unsloth/Qwen3-4B-Instruct-2507-GGUF'
        previous_row=next((m for m in previous['models'] if m['repo']==repo),None) if previous else None
        if previous and not previous_row:raise ValueError('GGUF absent from reuse manifest')
        info=api.model_info(repo,revision=previous_row['revision'] if previous_row else None)
        candidates=[f.rfilename for f in info.siblings if f.rfilename.endswith('Q4_K_M.gguf') and '/' not in f.rfilename]
        if len(candidates)!=1:raise ValueError('Expected exactly one unsplit Q4_K_M GGUF; inspect the repository before selecting')
        rows.append(download(api,hf_hub_download,args.root,repo,info.sha,'llm',candidates))
    args.root.mkdir(parents=True,exist_ok=True)
    target=args.root/'manifest.json'
    target.write_text(json.dumps({'downloaded_at':datetime.now(timezone.utc).isoformat(),
        'scope':'selected model artifacts only; pin Python/native packages separately','models':rows},indent=2)+'\n')
    print(f'Wrote {target}; retain it with your release. No model execution was performed.')


def download(api,fetch,root,repo,revision,directory,files):
    entries=[]
    for filename in files:
        path=Path(fetch(repo_id=repo,revision=revision,filename=filename,local_dir=root/directory))
        entries.append({'file':str(path.relative_to(root)),'sha256':sha256(path),'bytes':path.stat().st_size})
        print(f'{repo}@{revision[:12]} {filename}: {path.stat().st_size} bytes')
    return {'repo':repo,'revision':revision,'files':entries}


if __name__=='__main__':main()
