import numpy as np
from scripts.lab_caller import gaps
from scripts.twilio_emulator import FRAME, Playout


def test_emulated_playout_echoes_a_mark_only_after_its_audio_was_played():
    p=Playout();p.media(np.ones(FRAME,np.float32));p.mark('1:1')
    p.media(np.ones(FRAME//2,np.float32));p.media(np.ones(FRAME//2,np.float32));p.mark('1:2')
    frame,marks=p.take();assert len(frame)==FRAME and marks==[]
    frame,marks=p.take();assert len(frame)==FRAME and marks==['1:1']   # two halves join
    frame,marks=p.take();assert frame is None and marks==['1:2']


def test_emulated_clear_drops_audio_and_returns_pending_marks_in_order():
    p=Playout();p.media(np.ones(FRAME*3,np.float32));p.mark('2:1');p.media(np.ones(FRAME,np.float32));p.mark('2:2')
    assert p.clear()==['2:1','2:2'] and p.take()==(None,[])


def test_short_final_frame_is_padded_to_twenty_milliseconds():
    p=Playout();p.media(np.ones(50,np.float32))
    frame,_=p.take();assert len(frame)==FRAME and frame[49]==1 and frame[50]==0


def test_reply_gaps_ignore_frame_cadence():
    assert gaps([0,.02,.04,.5,.52,1.2])==[.46,.68]
