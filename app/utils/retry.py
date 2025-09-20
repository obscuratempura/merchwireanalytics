"""Retry helpers without external dependencies."""

from __future__ import annotations

import asyncio
import functools
import random
from collections.abc import Awaitable, Callable

RETRY_EXCEPTIONS = (OSError, asyncio.TimeoutError)


def retry_async(func: Callable[..., Awaitable]):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        delay = 1.0
        for attempt in range(3):
            try:
                return await func(*args, **kwargs)
            except RETRY_EXCEPTIONS:
                if attempt == 2:
                    raise
                await asyncio.sleep(delay + random.random())
                delay *= 2
    return wrapper
