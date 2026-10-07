"""HTTP body memory/time budgets apply before JSON or multipart parsing."""
import asyncio
from starlette.responses import Response


class BodyLimit:
    def __init__(self, app, limit=100000, timeout=10):
        self.app, self.limit, self.timeout = app, limit, timeout

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        chunks, total, oversized = [], 0, False
        try:
            async with asyncio.timeout(self.timeout):
                while True:
                    message = await receive()
                    if message['type'] == 'http.disconnect':
                        return
                    data = message.get('body', b'')
                    total += len(data)
                    if total > self.limit:
                        oversized = True
                        break
                    chunks.append(data)
                    if not message.get('more_body', False):
                        break
        except TimeoutError:
            return await Response('request body timeout', status_code=408)(scope, receive, send)
        if oversized:
            return await Response('request body too large', status_code=413)(scope, receive, send)
        body, delivered = b''.join(chunks), False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {'type': 'http.request', 'body': body, 'more_body': False}
            return await receive()
        await self.app(scope, replay, send)
