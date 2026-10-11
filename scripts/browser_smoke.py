"""Exercise a running browser client with Chromium's synthetic microphone.

This is not a physical-microphone or speaker test. Install the browser extra and
run `python -m playwright install chromium` first. No transcript is saved unless
--include-text is given. With --audio-file, the WAV replaces the synthetic tone and
the run waits for a completed spoken reply; over WebSocket "completed" requires the
page's own render acknowledgments, not only server dispatch.
"""
from __future__ import annotations
import argparse
import json
import os
import time
from pathlib import Path
import httpx
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://localhost:8000')
    parser.add_argument('--transport', choices=['websocket', 'webrtc'], default='websocket')
    parser.add_argument('--allow-real', action='store_true', help='Explicitly permit real provider usage')
    parser.add_argument('--output', type=Path, default=Path('verification'))
    parser.add_argument('--audio-file', type=Path, help='16-bit PCM WAV used as the microphone, played once')
    parser.add_argument('--expect', help='text that must appear in the assistant reply (needs --audio-file)')
    parser.add_argument('--wait', type=float, default=45, help='seconds to wait for a completed reply')
    parser.add_argument('--include-text', action='store_true', help='store transcripts in the report')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    report = {'scope': 'Chromium with a synthetic microphone; not physical audio hardware',
              'status': 'failed', 'transcript_saved': False, 'validated': False}
    browser = None
    try:
        config = httpx.get(args.url.rstrip('/')+'/api/config', timeout=5).raise_for_status().json()
        if config['mode'] != 'demo' and not args.allow_real:
            raise RuntimeError('Refusing real model usage without --allow-real')
        with sync_playwright() as p:
            flags = ['--no-sandbox', '--use-fake-ui-for-media-stream',
                     '--use-fake-device-for-media-stream', '--autoplay-policy=no-user-gesture-required']
            if args.audio_file:
                flags.append(f'--use-file-for-fake-audio-capture={args.audio_file.resolve()}%noloop')
            browser = p.chromium.launch(executable_path=os.environ.get('CHROMIUM_PATH'),
                headless=True, args=flags)
            try:
                context = browser.new_context(permissions=['microphone'], viewport={'width':1280,'height':900})
                page = context.new_page()
                errors = []
                page.on('pageerror', lambda e: errors.append(str(e)))
                page.goto(args.url, wait_until='networkidle')
                page.select_option('#transport', args.transport)
                page.locator('#token').fill(os.environ.get('APP_TOKEN',''))
                page.click('#start')
                # Locator polling, not wait_for_function: the page's CSP forbids eval.
                page.locator('#stop:enabled').wait_for(timeout=20000)
                if args.audio_file:
                    reply = wait_for_reply(page, args.wait)
                    report.update(reply_completed=reply['completed'], timing=reply['timing'])
                    if args.expect:
                        report['expect_found'] = args.expect.lower() in ' '.join(reply['assistant']).lower()
                    if args.include_text:
                        report.update(user_text=reply['user'], assistant_text=reply['assistant'])
                else:
                    page.wait_for_timeout(900)
                    page.click('#mute')
                    page.wait_for_timeout(2500)
                report.update(browser=browser.version, mode=config['mode'], transport=args.transport,
                    transcript_present=bool(page.locator('#transcript').inner_text()),
                    evidence=page.locator('#evidence').inner_text(), errors=errors)
                # No screenshot: it might expose an entered token or real transcript.
                page.click('#stop')
                page.wait_for_timeout(200)
                report['stopped'] = page.locator('#status').inner_text() == 'Disconnected'
                report['validated'] = bool(report['transcript_present'] and report['stopped'] and not errors
                                           and report.get('reply_completed', True) and report.get('expect_found', True))
                report['status'] = 'passed' if report['validated'] else 'failed'
            finally:
                browser.close()
    except Exception as exc:
        report['reason'] = str(exc)[:2000]
        if 'ERR_BLOCKED_BY_ADMINISTRATOR' in str(exc):
            report['status'] = 'blocked'
    (args.output/'browser-smoke.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2))
    if not report['validated']:
        raise SystemExit(1)


def wait_for_reply(page, seconds):
    """Wait for the first user turn's reply: messages after it and a non-greeting timing line."""
    deadline = time.monotonic() + seconds
    user, assistant, timing = [], [], ''
    while time.monotonic() < deadline:
        lines = page.locator('#transcript .message').all_inner_texts()
        first = next((i for i, x in enumerate(lines) if x.startswith('YOU')), None)
        timing = page.locator('#timing').inner_text()
        if first is not None:
            user = [lines[first].split('TRANSCRIPT', 1)[1].strip()]
            assistant = [x.split('HEARD', 1)[1].strip() for x in lines[first + 1:] if x.startswith('ASSISTANT')]
            # Cascaded turns end with a completed timing line; a live model's turn ends when its
            # finished reply text arrives (the live session reports timing only at hangup).
            live = 'Real model profile: live' in page.locator('#notice').inner_text()
            if assistant and (live or ('Status: completed' in timing and 'Endpoint: 0 ms' not in timing)):
                return {'completed': True, 'user': user, 'assistant': assistant, 'timing': timing}
        page.wait_for_timeout(250)
    return {'completed': False, 'user': user, 'assistant': assistant, 'timing': timing}


if __name__ == '__main__':
    main()
