"""These tests are intentionally skipped when their actual runtime is unavailable."""
import asyncio
import importlib.util
import os
from types import SimpleNamespace
import numpy as np
import pytest


@pytest.mark.rtc
async def test_real_aiortc_track_epoch_and_audio_frame():
    pytest.importorskip('aiortc',reason='Install the rtc extra; PyAV/aiortc are not faked.')
    from duplex_voice.transports.webrtc import OutgoingTrack
    from duplex_voice.transports.base import Transport
    from .fakes import FakeTransport
    owner=FakeTransport();owner.playout_delay=0
    track=OutgoingTrack(owner);epoch=owner.ledger.begin();seq=owner.ledger.issue(epoch)
    await track.queue.put((np.ones(960,np.float32)*.2,epoch,seq))
    f=await track.recv();assert f.samples==960 and f.sample_rate==48000
    await asyncio.sleep(0.001);assert owner.ledger.acked==seq
    stale=epoch;owner.ledger.begin()
    await track.queue.put((np.ones(960,np.float32),stale,seq))
    assert not (await track.recv()).to_ndarray().any()
    track.stop();await owner.close()


@pytest.mark.live
async def test_actual_downloaded_models_speech_roundtrip():
    if os.environ.get('DUPLEX_LIVE_MODELS') != '1':
        pytest.skip('Opt-in only: real sherpa/Kokoro weights and speech dependencies required.')
    from duplex_voice.speech_service import NativeModels
    from duplex_voice.audio import Resampler
    m=await asyncio.to_thread(NativeModels)
    audio=await asyncio.to_thread(m.synthesize,'Please book a table for tomorrow.')
    rs=Resampler(24000,16000)
    x=np.concatenate([rs.process(audio),rs.flush()]);stream=m.asr.create_stream()
    text=''
    for i in range(0,len(x),512):text=await asyncio.to_thread(m.decode,stream,x[i:i+512])
    text=await asyncio.to_thread(m.decode,stream,np.zeros(0,np.float32),True)
    assert len(text.split()) >= 3, f'Actual recognized output: {text!r}'
