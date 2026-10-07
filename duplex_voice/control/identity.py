"""Short-lived application identities. Media grants are a separate token audience.

Your authenticated web/mobile backend issues HS256 JWTs. This is a narrow trusted
issuer integration, not a replacement for login, OAuth, or OIDC discovery/JWKS.
"""
from __future__ import annotations
import time
import jwt
from ..security import check_bearer
from .service import Principal


def authenticate(settings, authorization: str) -> Principal:
    if settings.app_token and check_bearer(authorization, settings.app_token):
        return Principal(settings.default_tenant, 'service')
    if settings.identity_secret and authorization.startswith('Bearer '):
        token = authorization[7:]
        if len(token) > 4096:
            raise ValueError('identity token too long')
        try:
            claims = jwt.decode(token, settings.identity_secret, algorithms=['HS256'],
                audience='audio-pipeline:'+settings.cell_id, issuer=settings.identity_issuer,
                options={'require': ['exp', 'iat', 'sub', 'tenant']}, leeway=0)
            if type(claims['exp']) not in (int, float) or type(claims['iat']) not in (int, float):
                raise ValueError('invalid token timestamps')
            if not 0 < claims['exp']-claims['iat'] <= 300:
                raise ValueError('identity token lifetime exceeds five minutes')
            return Principal(claims['tenant'], claims['sub'])
        except jwt.PyJWTError as exc:
            raise ValueError('invalid identity token') from exc
    if not settings.app_token and not settings.identity_secret and settings.deployment == 'development':
        return Principal(settings.default_tenant, 'development')
    raise ValueError('application authentication required')


def issue_identity(secret: str, tenant: str, subject: str, issuer='audio-pipeline-app', ttl=120, cell='default'):
    """Trusted application backend helper; never expose this as an anonymous route."""
    Principal(tenant, subject)
    if len(secret) < 32 or not 1 <= ttl <= 300:
        raise ValueError('strong issuer key and a 1..300 second lifetime required')
    now = int(time.time())
    return jwt.encode({'tenant': tenant, 'sub': subject, 'iss': issuer,
                       'aud': 'audio-pipeline:'+cell, 'iat': now, 'exp': now+ttl}, secret, algorithm='HS256')
