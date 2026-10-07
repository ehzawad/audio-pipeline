"""Public transport edges and composition; state and media ownership live elsewhere."""
from __future__ import annotations
import asyncio
import importlib.util
import json
import secrets
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, Header, HTTPException, Request, WebSocket
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import Settings
from .concurrency import Admission, Overloaded
from .metrics import Metrics
from .http_limits import BodyLimit
from .outbound import DialStore, dial
from .providers import Providers
from .security import check_bearer, valid_twilio, allowed_peer
from .control.backend import SQLiteBackend, RedisBackend, Capacity, Conflict, Expired
from .control.service import SessionControl, Principal, Grants, NotFound, InvalidState
from .control.identity import authenticate
from .control.runtime import GatewayRuntime
from .transports.audiosocket import AudioSocketTransport
from .transports.twilio import TwilioTransport, twiml
from .transports.websocket import WebSocketTransport

STATIC = Path(__file__).parent / 'static'


class TicketRequest(BaseModel):
    transport: Literal['websocket', 'webrtc'] = 'websocket'


class Offer(BaseModel):
    type: Literal['offer'] = 'offer'
    sdp: str = Field(min_length=1, max_length=65536)
    ticket: str = Field(min_length=20, max_length=1024)


class OutboundRequest(BaseModel):
    to: str = Field(pattern=r'^\+[1-9][0-9]{7,14}$')
    consent_confirmed: Literal[True]


def create_app(settings=None, provider_factory=Providers, backend_factory=None):
    s = settings or Settings.from_env()
    cap, metrics = Admission(s.max_sessions), Metrics()
    grants = Grants(s.signing_key or secrets.token_urlsafe(32), s.cell_id)
    rt = None
    control = None

    @asynccontextmanager
    async def lifespan(app):
        nonlocal rt, control
        backend = (backend_factory() if backend_factory else
                   RedisBackend(s.redis_url, s.cell_id) if s.state_backend == 'redis' else
                   SQLiteBackend(s.state_path))
        control = SessionControl(backend, tenant_limit=s.tenant_session_limit,
            lease_seconds=s.lease_seconds, call_seconds=s.max_call_seconds,
            retention_seconds=s.retention_seconds, history_chars=s.max_history_chars,
            history_messages=s.max_history_messages, persist_history=s.persist_history)
        providers = await asyncio.to_thread(provider_factory, s)
        rt = GatewayRuntime(control, s, metrics, cap, providers)
        app.state.providers, app.state.control, app.state.runtime = providers, control, rt
        app.state.metrics, app.state.cap = metrics, cap
        app.state.rtc_available = s.rtc_enabled and importlib.util.find_spec('aiortc') is not None
        app.state.dial_store = DialStore(s.outbound_db) if s.outbound_enabled else None
        tcp = None
        async def loop_lag():
            loop = asyncio.get_running_loop()
            while True:
                expected = loop.time() + .5
                await asyncio.sleep(.5)
                metrics.observe('gateway.event_loop_lag', max(0, loop.time()-expected))
        lag = asyncio.create_task(loop_lag())
        try:
            async with asyncio.timeout(s.control_timeout):
                await backend.healthy()
            if s.audiosocket_enabled:
                tcp = await asyncio.start_server(asterisk, s.audiosocket_host, s.audiosocket_port, limit=65536)
            yield
        finally:
            cap.draining = True
            lag.cancel()
            await asyncio.gather(lag, return_exceptions=True)
            if tcp:
                tcp.close()
                await tcp.wait_closed()
            await rt.drain()
            await providers.close()
            await backend.close()

    app = FastAPI(title='Audio Pipeline', version='3.0.0', lifespan=lifespan)
    app.state.settings = s
    app.add_middleware(BodyLimit)
    app.mount('/static', StaticFiles(directory=STATIC), name='static')

    def identity(header):
        try:
            return authenticate(s, header)
        except (ValueError, TypeError):
            raise HTTPException(401, 'invalid application identity') from None

    def browser_origin(value, required=False):
        if (required or value) and value not in s.origins:
            raise HTTPException(403, 'origin not allowed')

    def public(doc):
        return {k: doc[k] for k in ('sid', 'transport', 'state', 'created_at', 'deadline',
                'history_revision', 'reason', 'events')}

    async def checked(awaitable):
        try:
            async with asyncio.timeout(s.control_timeout):
                return await awaitable
        except NotFound:
            raise HTTPException(404, 'session not found') from None
        except (InvalidState, Expired):
            raise HTTPException(409, 'session state or grant no longer permits this operation') from None
        except (Capacity, Conflict, Overloaded):
            raise HTTPException(503, 'session capacity or state contention', headers={'Retry-After': '2'}) from None
        except HTTPException:
            raise
        except ValueError:
            raise HTTPException(400, 'invalid session parameters') from None
        except Exception:
            metrics.count('control.request_failed')
            raise HTTPException(503, 'session control unavailable') from None

    def grant_response(doc):
        result = {'ticket': grants.issue(doc), 'session_id': doc['sid'],
                  'grant_expires_at': doc['grant_until'], 'state': doc['state']}
        if doc['transport'] == 'webrtc':
            result['ice_servers'] = json.loads(s.rtc_ice_servers_json)
        return result

    def available(transport):
        if cap.draining or cap.active >= cap.limit:
            raise HTTPException(503, 'this gateway is draining or at capacity')
        if transport == 'webrtc' and not app.state.rtc_available:
            raise HTTPException(409, 'install the rtc extra to enable WebRTC')

    @app.middleware('http')
    async def safety_headers(request, call_next):
        response = await call_next(request)
        response.headers.update({'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer',
            'Cache-Control': 'no-store', 'Permissions-Policy': 'microphone=(self), camera=()',
            'Content-Security-Policy': "default-src 'self'; connect-src 'self'; media-src 'self' blob:; style-src 'self'; script-src 'self'; frame-ancestors 'none'"})
        return response

    @app.get('/')
    async def index():
        return FileResponse(STATIC/'index.html')

    @app.get('/healthz')
    async def health():
        return {'alive': True}

    @app.get('/readyz')
    async def ready():
        good = not cap.draining
        try:
            async with asyncio.timeout(s.control_timeout):
                good = good and await control.backend.healthy()
        except Exception:
            good = False
        return Response(json.dumps({'ready': good}), media_type='application/json', status_code=200 if good else 503)

    @app.get('/api/config')
    async def config():
        return {'mode': s.voice_mode, 'webrtc': app.state.rtc_available,
                'requires_token': bool(s.app_token or s.identity_secret)}

    @app.get('/metrics')
    async def get_metrics(authorization: str = Header(default='')):
        # Detailed metrics are operator-only when any production identity is configured.
        if (s.app_token or s.identity_secret) and not check_bearer(authorization, s.app_token):
            raise HTTPException(401)
        return {**metrics.snapshot(), 'active_sessions': cap.active, 'limit': cap.limit}

    @app.get('/metrics/prometheus')
    async def prometheus(authorization: str = Header(default='')):
        if (s.app_token or s.identity_secret) and not check_bearer(authorization, s.app_token):
            raise HTTPException(401)
        return Response(metrics.prometheus(cap.active, cap.draining), media_type='text/plain; version=0.0.4')

    @app.post('/admin/drain')
    async def drain(authorization: str = Header(default='')):
        if not check_bearer(authorization, s.app_token):
            raise HTTPException(401)
        await rt.drain()
        return {'draining': True, 'active_sessions': cap.active}

    @app.post('/api/tickets')
    async def ticket(body: TicketRequest, request: Request, authorization: str = Header(default='')):
        principal = identity(authorization)
        browser_origin(request.headers.get('origin', ''), required=True)
        available(body.transport)
        doc = await checked(control.create(principal, body.transport, secrets.token_hex(16)))
        return grant_response(doc)

    @app.post('/v1/sessions', status_code=201)
    async def create_session(body: TicketRequest, request: Request,
                             authorization: str = Header(default=''), idempotency_key: str = Header(default='')):
        principal = identity(authorization)
        browser_origin(request.headers.get('origin', ''))
        available(body.transport)
        if not idempotency_key:
            raise HTTPException(400, 'Idempotency-Key is required')
        doc = await checked(control.create(principal, body.transport, idempotency_key))
        return grant_response(doc)

    @app.get('/v1/sessions/{sid}')
    async def session_status(sid: str, authorization: str = Header(default='')):
        doc = await checked(control.get(identity(authorization), sid))
        return {**public(doc), 'effective_state': doc['effective_state']}

    @app.delete('/v1/sessions/{sid}', status_code=202)
    async def stop_session(sid: str, authorization: str = Header(default='')):
        return public(await checked(control.stop(identity(authorization), sid)))

    @app.post('/v1/sessions/{sid}/reconnect')
    async def reconnect(sid: str, request: Request, authorization: str = Header(default='')):
        principal = identity(authorization)
        browser_origin(request.headers.get('origin', ''))
        available('websocket')
        doc = await checked(control.reconnect(principal, sid))
        return grant_response(doc)

    @app.websocket('/ws')
    async def web(ws: WebSocket):
        owned = None
        accepted = False
        try:
            browser_origin(ws.headers.get('origin', ''))  # absent Origin supports native clients with signed grants
            offered = [v.strip() for v in ws.headers.get('sec-websocket-protocol', '').split(',')]
            if len(offered) != 2 or offered[0] != 'duplex':
                raise ValueError('expected duplex and a media grant')
            claims = grants.verify(offered[1], 'websocket')
            owned = await rt.acquire(claims)
            await ws.accept(subprotocol='duplex')
            accepted = True
            await owned.run(WebSocketTransport(ws))
        except BaseException as exc:
            metrics.count('ingress.rejected')
            if owned and not owned.closed:
                with suppress(Exception):
                    await owned.abort()
            if not owned or not owned.running:
                with suppress(Exception):
                    await ws.close(code=1013 if accepted else 1008)
            if not isinstance(exc, Exception):
                raise

    @app.post('/api/rtc/offer')
    async def offer(body: Offer, request: Request):
        browser_origin(request.headers.get('origin', ''))
        available('webrtc')
        try:
            claims = grants.verify(body.ticket, 'webrtc')
        except ValueError:
            raise HTTPException(401, 'invalid media grant') from None
        from aiortc import RTCPeerConnection, RTCSessionDescription, RTCConfiguration, RTCIceServer
        from .transports.webrtc import WebRTCTransport
        slot = cap.slot()
        try:
            await slot.__aenter__()
            slot._voice_entered = True
        except Overloaded:
            raise HTTPException(503, 'gateway at capacity') from None
        pc, owned, transferred = None, None, False
        try:
            ice = [RTCIceServer(**item) for item in json.loads(s.rtc_ice_servers_json)]
            pc = RTCPeerConnection(RTCConfiguration(iceServers=ice))
            transport = WebRTCTransport(pc, s)
            async with asyncio.timeout(20):
                await pc.setRemoteDescription(RTCSessionDescription(sdp=body.sdp, type=body.type))
                await pc.setLocalDescription(await pc.createAnswer())
            transferred = True  # acquire takes responsibility for releasing the slot on failure
            owned = await rt.acquire(claims, slot)
            task = asyncio.create_task(owned.run(transport))
            rt.active.add(task)
            return {'sdp': pc.localDescription.sdp, 'type': pc.localDescription.type}
        except BaseException:
            if pc:
                await pc.close()
            if owned:
                await owned.abort()
            elif not transferred:
                await slot.__aexit__(None, None, None)
            raise

    @app.post('/twilio/voice')
    async def incoming(request: Request):
        if not s.twilio_enabled:
            raise HTTPException(404)
        form = await request.form(max_fields=100, max_files=0)
        pairs = [(str(k), str(v)) for k, v in form.multi_items()]
        url = s.public_base_url.rstrip('/')+'/twilio/voice'
        if request.url.query:
            url += '?'+request.url.query
        if s.twilio_validate and not valid_twilio(url, pairs, s.twilio_auth_token, request.headers.get('x-twilio-signature', '')):
            raise HTTPException(403, 'invalid Twilio signature')
        call_sid = str(form.get('CallSid', ''))
        if not call_sid or len(call_sid) > 100:
            raise HTTPException(400, 'missing or invalid CallSid')
        if s.twilio_account_sid and form.get('AccountSid') != s.twilio_account_sid:
            raise HTTPException(403, 'wrong Twilio account')
        try:
            available('twilio')
            principal = Principal(s.default_tenant, 'twilio:'+call_sid)
            doc = await checked(control.create(principal, 'twilio', call_sid))
        except HTTPException:
            return Response('<Response><Say>The assistant is busy. Please try later.</Say></Response>', media_type='application/xml')
        wss = s.public_base_url.rstrip('/').replace('https://', 'wss://').replace('http://', 'ws://')
        return Response(twiml(wss+'/twilio/stream', grants.issue(doc)), media_type='application/xml')

    @app.websocket('/twilio/stream')
    async def twilio_stream(ws: WebSocket):
        url = s.public_base_url.rstrip('/')+'/twilio/stream'
        if ws.url.query:
            url += '?'+ws.url.query
        if not s.twilio_enabled or (s.twilio_validate and not valid_twilio(url, [], s.twilio_auth_token, ws.headers.get('x-twilio-signature', ''))):
            await ws.close(code=1008)
            return
        owned, transport, slot, transferred = None, None, None, False
        try:
            slot = cap.slot()
            await slot.__aenter__()
            slot._voice_entered = True
            claims = None
            def validate(start):
                nonlocal claims
                if s.twilio_account_sid and start.get('accountSid') != s.twilio_account_sid:
                    raise ValueError('wrong Twilio account')
                claims = grants.verify(start.get('customParameters', {}).get('ticket', ''), 'twilio')
                if claims['subject'] != 'twilio:'+start.get('callSid', ''):
                    raise ValueError('cross-call media grant')
            await ws.accept()
            transport = TwilioTransport(ws, validate)
            await asyncio.wait_for(transport.ready(), s.io_timeout)
            transferred = True
            owned = await rt.acquire(claims, slot)
            await owned.run(transport, prepared=True)
        except BaseException as exc:
            metrics.count('ingress.rejected')
            if owned and not owned.closed:
                with suppress(Exception):
                    await owned.abort()
            elif slot is not None and getattr(slot, '_voice_entered', False) and not transferred:
                await slot.__aexit__(None, None, None)
            if not owned or not owned.running:
                with suppress(Exception):
                    await ws.close(code=1008)
            if not isinstance(exc, Exception):
                raise

    async def asterisk(reader, writer):
        peer = writer.get_extra_info('peername')
        if not peer or not allowed_peer(peer[0], s.audiosocket_allowed_cidrs):
            writer.close()
            return
        slot, owned, transferred = cap.slot(), None, False
        transport = AudioSocketTransport(reader, writer)
        try:
            await slot.__aenter__()
            slot._voice_entered = True
            await asyncio.wait_for(transport.ready(), s.io_timeout)
            doc = await checked(control.create(Principal(s.default_tenant, 'pbx'), 'audiosocket', transport.call_uuid))
            transferred = True
            owned = await rt.acquire(grants.verify(grants.issue(doc), 'audiosocket'), slot)
            await owned.run(transport, prepared=True)
        except BaseException as exc:
            metrics.count('ingress.rejected')
            if owned and not owned.closed:
                with suppress(Exception):
                    await owned.abort()
            elif getattr(slot, '_voice_entered', False) and not transferred:
                await slot.__aexit__(None, None, None)
            if not owned or not owned.running:
                with suppress(Exception):
                    await transport.close()
            if not isinstance(exc, Exception):
                raise

    @app.post('/twilio/outbound')
    async def outbound(body: OutboundRequest, authorization: str = Header(default=''), idempotency_key: str = Header(default='')):
        if not s.outbound_enabled:
            raise HTTPException(404)
        if not check_bearer(authorization, s.app_token):
            raise HTTPException(401)
        if body.to not in {x.strip() for x in s.outbound_allowlist.split(',')}:
            raise HTTPException(403, 'destination is not allowlisted')
        if not 8 <= len(idempotency_key) <= 128:
            raise HTTPException(400, 'provide an 8-128 character Idempotency-Key')
        if not s.twilio_account_sid or not s.twilio_from_number:
            raise HTTPException(503, 'outbound account is not configured')
        try:
            return await dial(s, app.state.dial_store, app.state.providers.client, idempotency_key, body.to)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        except Exception:
            raise HTTPException(502, 'dispatch failed or is unknown; do not retry with a new key') from None
    return app


app = create_app()
