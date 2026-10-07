"""Synthetic microphone traffic over real HTTP/WebSocket routes. Not an ASR benchmark."""
import argparse
import asyncio
import json
import os
import struct
import time
import numpy as np
import httpx
import websockets
from duplex_voice.audio import float_to_pcm16


async def run(args):
    origin=args.url.rstrip('/'); media=(args.media_url or args.url).rstrip('/');headers={'Origin':origin}
    if os.environ.get('APP_TOKEN'):headers['Authorization']='Bearer '+os.environ['APP_TOKEN']
    async with httpx.AsyncClient(timeout=10) as http:
        config=(await http.get(origin+'/api/config')).json()
        if config['mode']!='demo' and not args.allow_real:raise ValueError('Refusing real-model load without --allow-real')
        async def call(index):
            r=await http.post(origin+'/api/tickets',json={'transport':'websocket'},headers=headers)
            if r.status_code!=200:return {'call':index,'status':'rejected','http':r.status_code}
            token=r.json()['ticket'];n=0;user_seen=False;start=time.perf_counter()
            async with websockets.connect(media.replace('http://','ws://').replace('https://','wss://')+'/ws',
                    origin=origin,subprotocols=['duplex',token],max_size=65536) as ws:
                await ws.send(json.dumps({'type':'hello','sample_rate':16000}))
                await ws.recv();await ws.recv()
                async def send():
                    await asyncio.sleep(.4)
                    for i in range(38):
                        x=np.ones(512,np.float32)*.2 if i<8 else np.zeros(512,np.float32)
                        await ws.send(float_to_pcm16(x));await asyncio.sleep(.032)
                tx=asyncio.create_task(send())
                try:
                    async with asyncio.timeout(20):
                        async for raw in ws:
                            if isinstance(raw,bytes):
                                n+=1;e,seq=struct.unpack('<II',raw[:8])
                                # Synthetic receipt: confirms test-client handling, not speaker playout.
                                await ws.send(json.dumps({'type':'played','epoch':e,'seq':seq}))
                            else:
                                event=json.loads(raw)
                                if event.get('type')=='user' and event.get('committed'):user_seen=True
                                if user_seen and event.get('type')=='timing' and event.get('status')=='completed':
                                    await tx;await ws.send(json.dumps({'type':'hangup'}))
                                    return {'call':index,'status':'completed','media_packets':n,
                                            'wall_seconds':time.perf_counter()-start,'trace':event}
                finally:
                    tx.cancel();await asyncio.gather(tx,return_exceptions=True)
            return {'call':index,'status':'closed_without_completed_turn'}
        results=await asyncio.gather(*(call(i) for i in range(args.calls)),return_exceptions=True)
        output={'scope':'synthetic transport traffic; NOT real model quality or PSTN latency','mode':config['mode'],
                'control_url':origin,'media_url':media,'calls':[{'status':'exception','type':type(r).__name__} if isinstance(r,BaseException) else r for r in results]}
        print(json.dumps(output,indent=2))
        return output


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--url',default='http://localhost:8000')
    p.add_argument('--media-url',help='Optional other gateway in the same cell');p.add_argument('--calls',type=int,default=4);p.add_argument('--allow-real',action='store_true')
    args=p.parse_args()
    if not 1<=args.calls<=100:p.error('--calls must be 1..100')
    asyncio.run(run(args))
