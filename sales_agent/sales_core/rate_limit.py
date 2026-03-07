from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from threading import Lock
from typing import Deque, Dict, Protocol
from uuid import uuid4


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    retry_after_seconds: int
    remaining: int


class RateLimiter(Protocol):
    def check(self, key: str, *, limit: int, now_ts: float | None = None) -> RateLimitResult:
        ...


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


class RedisRateLimiter:
    """Sliding-window limiter backed by Redis sorted sets."""

    def __init__(self, *, redis_url: str, window_seconds: int, key_prefix: str = "rate_limit") -> None:
        normalized_url = (redis_url or "").strip()
        if not normalized_url:
            raise RuntimeError("redis_url is required for RedisRateLimiter")
        try:
            import redis  # type: ignore[import-not-found]
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on runtime package set
            raise RuntimeError("redis package is not installed") from exc

        self.window_seconds = max(1, int(window_seconds))
        self.key_prefix = (key_prefix or "rate_limit").strip() or "rate_limit"
        self._client = redis.Redis.from_url(normalized_url, decode_responses=True)

    def check(self, key: str, *, limit: int, now_ts: float | None = None) -> RateLimitResult:
        normalized_key = (key or "").strip()
        if not normalized_key:
            raise ValueError("rate limit key must be non-empty")

        normalized_limit = max(1, int(limit))
        now = float(now_ts if now_ts is not None else time.time())
        cutoff = now - float(self.window_seconds)
        redis_key = f"{self.key_prefix}:{normalized_key}"

        # Keep this sequence simple and robust. It is not a strict transaction,
        # but is good enough for limiter semantics and avoids Lua dependency.
        self._client.zremrangebyscore(redis_key, "-inf", cutoff)
        current = int(self._client.zcard(redis_key))
        if current >= normalized_limit:
            earliest = self._client.zrange(redis_key, 0, 0, withscores=True)
            earliest_ts = float(earliest[0][1]) if earliest else now
            retry_after = max(1, int(math.ceil((earliest_ts + self.window_seconds) - now)))
            return RateLimitResult(
                allowed=False,
                retry_after_seconds=retry_after,
                remaining=0,
            )

        member = f"{time.time_ns()}-{uuid4().hex}"
        self._client.zadd(redis_key, {member: now})
        self._client.expire(redis_key, self.window_seconds + 5)
        remaining = max(0, normalized_limit - (current + 1))
        return RateLimitResult(
            allowed=True,
            retry_after_seconds=0,
            remaining=remaining,
        )
