"""Per-host rate limiting utilities."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict


class RateLimiter:
    """Simple token bucket per host."""

    def __init__(self, *, rate: float = 1.5) -> None:
        self.rate = rate
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._last_request: dict[str, float] = defaultdict(lambda: 0.0)

    async def wait_for_host(self, host: str) -> None:
        lock = self._locks[host]
        async with lock:
            now = time.monotonic()
            elapsed = now - self._last_request[host]
            min_interval = 1.0 / self.rate
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            self._last_request[host] = time.monotonic()
