"""Exercise a running browser client with Chromium's synthetic microphone.

This is not a physical-microphone or speaker test. Install the browser extra and
run `python -m playwright install chromium` first. No transcript is saved.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import httpx
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://localhost:8000')
    parser.add_argument('--transport', choices=['websocket', 'webrtc'], default='websocket')
    parser.add_argument('--allow-real', action='store_true', help='Explicitly permit real provider usage')
    parser.add_argument('--output', type=Path, default=Path('verification'))
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
            browser = p.chromium.launch(executable_path=os.environ.get('CHROMIUM_PATH'),
                headless=True, args=['--no-sandbox', '--use-fake-ui-for-media-stream',
                '--use-fake-device-for-media-stream', '--autoplay-policy=no-user-gesture-required'])
            try:
                context = browser.new_context(permissions=['microphone'], viewport={'width':1280,'height':900})
                page = context.new_page()
                errors = []
                page.on('pageerror', lambda e: errors.append(str(e)))
                page.goto(args.url, wait_until='networkidle')
                page.select_option('#transport', args.transport)
                page.locator('#token').fill(os.environ.get('APP_TOKEN',''))
                page.click('#start')
                page.wait_for_function("document.getElementById('stop').disabled === false")
                page.wait_for_timeout(900)
                page.click('#mute')
                page.wait_for_timeout(2500)
                report.update(browser=browser.version, mode=config['mode'],
                    transcript_present=bool(page.locator('#transcript').inner_text()),
                    evidence=page.locator('#evidence').inner_text(), errors=errors)
                # No screenshot: it might expose an entered token or real transcript.
                page.click('#stop')
                page.wait_for_timeout(200)
                report['stopped'] = page.locator('#status').inner_text() == 'Disconnected'
                report['validated'] = bool(report['transcript_present'] and report['stopped'] and not errors)
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


if __name__ == '__main__':
    main()
