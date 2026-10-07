"""Resource ownership survives cancellation of an awaiting coroutine."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import partial
from typing import Callable, TypeVar

T = TypeVar("T")


class Overloaded(RuntimeError):
    pass


class Admission:
    """A process-local cap. Mutations are atomic between awaits on one event loop."""
    def __init__(self, limit: int):
        self.limit, self.active, self.draining = limit, 0, False

    @asynccontextmanager
    async def slot(self):
        if self.draining or self.active >= self.limit:
            raise Overloaded("capacity unavailable")
        self.active += 1
        try:
            yield
        finally:
            self.active -= 1


class ModelRunner:
    """Bound outstanding native jobs; never release a slot just because a caller canceled.

    Native inference is not preempted. On cancellation its result is ignored, but the
    callback releases capacity only after the thread actually completes.
    """
    def __init__(self, workers: int = 1):
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="speech")
        self._slots = asyncio.Semaphore(workers)
        self._pending: set[asyncio.Future] = set()

    async def run(self, fn: Callable[..., T], *args, **kwargs) -> T:
        await self._slots.acquire()
        try:
            fut = asyncio.get_running_loop().run_in_executor(self._pool, partial(fn, *args, **kwargs))
        except BaseException:
            self._slots.release()
            raise
        self._pending.add(fut)

        def done(f):
            self._pending.discard(f)
            self._slots.release()
            if not f.cancelled():
                f.exception()  # retrieve native failure even when its caller went away
        fut.add_done_callback(done)
        return await asyncio.shield(fut)

    async def close(self):
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)
        self._pool.shutdown(wait=False, cancel_futures=True)


async def cancel_and_join(task: asyncio.Task | None):
    if task is None:
        return
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)
