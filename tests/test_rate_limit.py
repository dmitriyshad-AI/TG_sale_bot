import unittest
from types import SimpleNamespace
from unittest.mock import patch

from sales_agent.sales_core.rate_limit import InMemoryRateLimiter, RedisRateLimiter


class RateLimitTests(unittest.TestCase):
    def test_allows_requests_within_limit(self) -> None:
        limiter = InMemoryRateLimiter(window_seconds=60)
        first = limiter.check("ip:1", limit=3, now_ts=100.0)
        second = limiter.check("ip:1", limit=3, now_ts=101.0)
        third = limiter.check("ip:1", limit=3, now_ts=102.0)

        self.assertTrue(first.allowed)
        self.assertTrue(second.allowed)
        self.assertTrue(third.allowed)
        self.assertEqual(third.remaining, 0)

    def test_blocks_request_over_limit(self) -> None:
        limiter = InMemoryRateLimiter(window_seconds=60)
        limiter.check("ip:1", limit=2, now_ts=100.0)
        limiter.check("ip:1", limit=2, now_ts=101.0)

        blocked = limiter.check("ip:1", limit=2, now_ts=102.0)
        self.assertFalse(blocked.allowed)
        self.assertGreaterEqual(blocked.retry_after_seconds, 1)
        self.assertEqual(blocked.remaining, 0)

    def test_allows_again_after_window_passes(self) -> None:
        limiter = InMemoryRateLimiter(window_seconds=10)
        limiter.check("ip:1", limit=1, now_ts=100.0)
        blocked = limiter.check("ip:1", limit=1, now_ts=101.0)
        allowed_again = limiter.check("ip:1", limit=1, now_ts=111.0)

        self.assertFalse(blocked.allowed)
        self.assertTrue(allowed_again.allowed)

    def test_separates_keys(self) -> None:
        limiter = InMemoryRateLimiter(window_seconds=60)
        limiter.check("ip:1", limit=1, now_ts=100.0)
        second_key = limiter.check("ip:2", limit=1, now_ts=100.0)

        self.assertTrue(second_key.allowed)

    def test_rejects_empty_key(self) -> None:
        limiter = InMemoryRateLimiter(window_seconds=60)
        with self.assertRaises(ValueError):
            limiter.check("", limit=1)

    def test_redis_rate_limiter_requires_url(self) -> None:
        with self.assertRaises(RuntimeError):
            RedisRateLimiter(redis_url="", window_seconds=60)

    def test_redis_rate_limiter_raises_when_package_missing(self) -> None:
        import builtins

        real_import = builtins.__import__

        def _import(name, globals=None, locals=None, fromlist=(), level=0):  # type: ignore[no-untyped-def]
            if name == "redis":
                raise ModuleNotFoundError("No module named 'redis'")
            return real_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=_import):
            with self.assertRaises(RuntimeError):
                RedisRateLimiter(redis_url="redis://localhost:6379/0", window_seconds=60)

    def test_redis_rate_limiter_allows_then_blocks_then_recovers(self) -> None:
        class FakeRedisClient:
            def __init__(self) -> None:
                self._data: dict[str, list[float]] = {}

            def zremrangebyscore(self, key: str, _min: str, cutoff: float) -> None:
                bucket = self._data.setdefault(key, [])
                self._data[key] = [score for score in bucket if score > float(cutoff)]

            def zcard(self, key: str) -> int:
                return len(self._data.setdefault(key, []))

            def zrange(self, key: str, start: int, end: int, withscores: bool = False):
                bucket = sorted(self._data.setdefault(key, []))
                if not bucket:
                    return []
                last = len(bucket) if end == -1 else end + 1
                window = bucket[start:last]
                if withscores:
                    return [(f"m-{idx}", score) for idx, score in enumerate(window)]
                return [f"m-{idx}" for idx, _score in enumerate(window)]

            def zadd(self, key: str, mapping: dict[str, float]) -> None:
                score = next(iter(mapping.values()))
                self._data.setdefault(key, []).append(float(score))

            def expire(self, key: str, ttl: int) -> None:
                _ = key, ttl

        fake_client = FakeRedisClient()
        fake_redis_module = SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda _url, decode_responses=True: fake_client)
        )

        with patch.dict("sys.modules", {"redis": fake_redis_module}):
            limiter = RedisRateLimiter(
                redis_url="redis://localhost:6379/0",
                window_seconds=10,
                key_prefix="test",
            )
            first = limiter.check("ip:1", limit=2, now_ts=100.0)
            second = limiter.check("ip:1", limit=2, now_ts=101.0)
            blocked = limiter.check("ip:1", limit=2, now_ts=102.0)
            allowed_again = limiter.check("ip:1", limit=2, now_ts=111.0)

        self.assertTrue(first.allowed)
        self.assertTrue(second.allowed)
        self.assertFalse(blocked.allowed)
        self.assertGreaterEqual(blocked.retry_after_seconds, 1)
        self.assertTrue(allowed_again.allowed)


if __name__ == "__main__":
    unittest.main()
