import unittest

from sales_agent.sales_core.rate_limit import InMemoryRateLimiter


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


if __name__ == "__main__":
    unittest.main()

