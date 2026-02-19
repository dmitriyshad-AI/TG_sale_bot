from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from threading import Lock
from typing import Deque, Dict


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    retry_after_seconds: int
    remaining: int


class InMemoryRateLimiter:
    """Simple sliding-window limiter for single-process deployments."""

    def __init__(self, *, window_seconds: int) -> None:
        self.window_seconds = max(1, int(window_seconds))
        self._events: Dict[str, Deque[float]] = {}
        self._lock = Lock()

    def check(self, key: str, *, limit: int, now_ts: float | None = None) -> RateLimitResult:
        normalized_key = (key or "").strip()
        if not normalized_key:
            raise ValueError("rate limit key must be non-empty")

        normalized_limit = max(1, int(limit))
        now = float(now_ts if now_ts is not None else time.time())
        cutoff = now - float(self.window_seconds)

        with self._lock:
            bucket = self._events.get(normalized_key)
            if bucket is None:
                bucket = deque()
                self._events[normalized_key] = bucket

            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            current = len(bucket)
            if current >= normalized_limit:
                earliest = bucket[0] if bucket else now
                retry_after = max(1, int(math.ceil((earliest + self.window_seconds) - now)))
                return RateLimitResult(
                    allowed=False,
                    retry_after_seconds=retry_after,
                    remaining=0,
                )

            bucket.append(now)
            remaining = max(0, normalized_limit - len(bucket))
            return RateLimitResult(
                allowed=True,
                retry_after_seconds=0,
                remaining=remaining,
            )

