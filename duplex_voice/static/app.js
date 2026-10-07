'use strict';
const $ = id => document.getElementById(id);
let config, ctx, mic, ws, pc, dc, source, capture, silentGain;
let epoch=0, nextPlay=0, muted=false, active=false, sources=new Set();
const show = (text,error=false) => { $('notice').textContent=text; $('notice').className='notice'+(error?' error':''); };
function message(role,text) {
  const div=document.createElement('div'); div.className='message';
  const b=document.createElement('b'); b.textContent=role; div.append(b,document.createTextNode(text));
  const log=$('transcript'); log.append(div); while(log.children.length>40) log.firstChild.remove(); log.scrollTop=log.scrollHeight;
}
function clearAudio(newEpoch) {
  epoch=newEpoch;
  for(const node of sources) { node.onended=null; try { node.stop(); } catch {} node.disconnect(); }
  sources.clear(); nextPlay=ctx?.currentTime || 0;
}
function event(data) {
  if(data.type==='clear') { clearAudio(data.epoch); $('status').textContent='Listening / interrupted'; }
  if(data.type==='ready') { $('evidence').textContent=data.playback_evidence; $('status').textContent='Connected / '+data.transport; }
  if(data.type==='partial') $('partial').textContent=data.text;
  if(data.type==='user' && data.committed) message('YOU / ASR TRANSCRIPT',data.text);
  if(data.type==='assistant_generated') message('ASSISTANT / GENERATED, NOT NECESSARILY HEARD',data.text);
  if(data.type==='speech_start') $('partial').textContent='Listening…';
  if(data.type==='error') show(data.message,true);
  if(data.type==='timing' && data.end_speech_to_dispatch_s !== undefined) {
    $('timing').textContent=`End of speech → first dispatch: ${Math.round(data.end_speech_to_dispatch_s*1000)} ms. Endpoint: ${Math.round(data.endpoint_s*1000)} ms. Status: ${data.status}.`;
  }
}
function playback(buffer) {
  if(!active || !ctx || buffer.byteLength<8 || (buffer.byteLength-8)%2) return;
  const view=new DataView(buffer), e=view.getUint32(0,true), seq=view.getUint32(4,true);
  if(e<epoch) return; if(e>epoch) clearAudio(e);
  const n=(buffer.byteLength-8)/2, audio=ctx.createBuffer(1,n,24000), x=audio.getChannelData(0);
  for(let i=0;i<n;i++) x[i]=view.getInt16(8+2*i,true)/32768;
  if(nextPlay-ctx.currentTime>1.5) { show('Playback fell behind its buffer limit. Reconnect on a stable network.',true); stop(); return; }
  const node=ctx.createBufferSource(); node.buffer=audio; node.connect(ctx.destination);
  const start=Math.max(ctx.currentTime+.04,nextPlay); nextPlay=start+audio.duration;
  sources.add(node);
  node.onended=()=> { sources.delete(node); node.disconnect(); if(e===epoch && ws?.readyState===1) ws.send(JSON.stringify({type:'played',epoch:e,seq})); };
  node.start(start);
}
async function ticket(transport) {
  const headers={'Content-Type':'application/json'};
  if($('token').value) headers.Authorization='Bearer '+$('token').value;
  const r=await fetch('/api/tickets',{method:'POST',headers,body:JSON.stringify({transport})});
  if(!r.ok) throw new Error('Session authorization/capacity failed: HTTP '+r.status);
  const result=await r.json();
  if(transport==='webrtc') config.ice_servers=result.ice_servers || [];
  return result.ticket;
}
async function startWS(t) {
  ws=new WebSocket(location.origin.replace(/^http/,'ws')+'/ws',['duplex',t]); ws.binaryType='arraybuffer';
  ws.onmessage=m=> { if(typeof m.data==='string') event(JSON.parse(m.data)); else playback(m.data); };
  await new Promise((resolve,reject)=> { ws.onopen=resolve; ws.onerror=()=>reject(new Error('Media WebSocket failed')); });
  ws.onclose=()=> { if(active) { show('Connection ended. Start a new call to reconnect.'); stop(); } };
  ws.send(JSON.stringify({type:'hello',sample_rate:ctx.sampleRate}));
  await ctx.audioWorklet.addModule('/static/capture.js');
  source=ctx.createMediaStreamSource(mic); capture=new AudioWorkletNode(ctx,'duplex-capture');
  silentGain=ctx.createGain(); silentGain.gain.value=0;
  capture.port.onmessage=m=> {
    if(ws?.readyState===1) {
      if(ws.bufferedAmount>96000) { show('Uplink is more than about one second behind. Call stopped.',true); stop(); return; }
      ws.send(m.data);
    }
  };
  source.connect(capture); capture.connect(silentGain); silentGain.connect(ctx.destination);
}
async function waitIce(peer) {
  if(peer.iceGatheringState==='complete') return;
  await new Promise((resolve,reject)=> {
    const timer=setTimeout(()=> { peer.removeEventListener('icegatheringstatechange',change); reject(new Error('ICE gathering timed out; check STUN/TURN settings')); },15000);
    const change=()=> { if(peer.iceGatheringState==='complete') {clearTimeout(timer);peer.removeEventListener('icegatheringstatechange',change);resolve();} };
    peer.addEventListener('icegatheringstatechange',change);
  });
}
async function startRTC(t) {
  pc=new RTCPeerConnection({iceServers:config.ice_servers});
  dc=pc.createDataChannel('control'); dc.onmessage=m=>event(JSON.parse(m.data));
  pc.ontrack=e=> { $('remote').srcObject=new MediaStream([e.track]); $('remote').play().catch(()=>show('Press play on the audio control to enable browser playback.')); };
  pc.onconnectionstatechange=()=> { if(pc && ['failed','disconnected','closed'].includes(pc.connectionState) && active) {show('WebRTC disconnected. Check ICE/TURN or use the WebSocket transport.',true);stop();} };
  for(const track of mic.getTracks()) pc.addTrack(track,mic);
  await pc.setLocalDescription(await pc.createOffer()); await waitIce(pc);
  const r=await fetch('/api/rtc/offer',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({...pc.localDescription.toJSON(),ticket:t})});
  if(!r.ok) throw new Error('WebRTC offer rejected: HTTP '+r.status);
  await pc.setRemoteDescription(await r.json());
}
async function start() {
  if(active) return;
  $('start').disabled=true;
  try {
    ctx=new (window.AudioContext||window.webkitAudioContext)(); await ctx.resume();
    mic=await navigator.mediaDevices.getUserMedia({audio:{channelCount:1,echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false});
    const transport=$('transport').value, t=await ticket(transport); active=true; epoch=0; nextPlay=0;
    if(transport==='webrtc') await startRTC(t); else await startWS(t);
    $('stop').disabled=false; $('mute').disabled=false; $('status').textContent='Connecting / '+transport;
  } catch(err) { show(err.message,true); await stop(); }
}
async function stop() {
  active=false; clearAudio(epoch+1);
  if(ws) {if(ws.readyState===1) ws.send(JSON.stringify({type:'hangup'}));ws.close();ws=null;}
  if(dc?.readyState==='open') dc.send(JSON.stringify({type:'hangup'})); dc=null;
  if(pc) {const old=pc;pc=null;old.close();}
  if(mic) mic.getTracks().forEach(t=>t.stop()); mic=null;
  source?.disconnect(); capture?.disconnect(); silentGain?.disconnect(); source=capture=silentGain=null;
  if(ctx) {const old=ctx;ctx=null;await old.close();}
  $('remote').srcObject=null; muted=false; $('mute').textContent='Mute mic';
  $('start').disabled=false; $('stop').disabled=true; $('mute').disabled=true; $('status').textContent='Disconnected'; $('partial').textContent='Your microphone is off.';
}
$('start').addEventListener('click',start); $('stop').addEventListener('click',stop);
$('mute').addEventListener('click',()=> {muted=!muted;mic?.getAudioTracks().forEach(t=>t.enabled=!muted);$('mute').textContent=muted?'Unmute mic':'Mute mic';});
window.addEventListener('pagehide',()=>{stop();});
fetch('/api/config').then(r=>r.json()).then(c=> {config=c;$('transport').options[1].disabled=!c.webrtc;if(c.webrtc)$('transport').value='webrtc';show(c.mode==='demo'?'OFFLINE TRANSPORT DEMO — microphone audio is accepted, but speech recognition is scripted and the reply is a quiet tone. Select a real model profile for an actual voice assistant.':'Real model profile: '+c.mode+'. Microphone audio is processed by the configured local or hosted services.');}).catch(e=>show(e.message,true));
