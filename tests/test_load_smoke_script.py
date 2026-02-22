import unittest
from io import StringIO
from unittest.mock import patch

from scripts import load_smoke


class _MockResponse:
    def __init__(self, status: int = 200, body: bytes = b"{}") -> None:
        self.status = status
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class LoadSmokeScriptTests(unittest.TestCase):
    def test_percentile_interpolates_values(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0]
        self.assertEqual(load_smoke._percentile(values, 0.0), 10.0)
        self.assertEqual(load_smoke._percentile(values, 1.0), 40.0)
        self.assertAlmostEqual(load_smoke._percentile(values, 0.5), 25.0)

    def test_main_reports_ok_when_all_responses_are_2xx(self) -> None:
        probe_result = load_smoke.ProbeResult(status_code=200, latency_ms=10.0, error="")
        with patch.object(load_smoke, "_probe_once", return_value=probe_result), patch(
            "sys.stdout", new_callable=StringIO
        ) as stdout:
            code = load_smoke.main(["--target", "health", "--requests", "4", "--concurrency", "2"])

        self.assertEqual(code, 0)
        self.assertIn("[OK] Load smoke finished", stdout.getvalue())

    def test_main_returns_fail_when_any_response_is_non_2xx(self) -> None:
        side_effect = [
            load_smoke.ProbeResult(status_code=200, latency_ms=10.0, error=""),
            load_smoke.ProbeResult(status_code=503, latency_ms=20.0, error=""),
            load_smoke.ProbeResult(status_code=200, latency_ms=15.0, error=""),
        ]
        with patch.object(load_smoke, "_probe_once", side_effect=side_effect), patch(
            "sys.stdout", new_callable=StringIO
        ) as stdout:
            code = load_smoke.main(["--target", "catalog", "--requests", "3", "--concurrency", "3"])

        self.assertEqual(code, 1)
        self.assertIn("[FAIL] Non-2xx responses detected", stdout.getvalue())

    def test_probe_once_health_success(self) -> None:
        with patch.object(load_smoke, "urlopen", return_value=_MockResponse(status=200, body=b'{"ok":true}')):
            result = load_smoke._probe_once(
                base_url="http://127.0.0.1:8000",
                target="health",
                timeout=2.0,
                assistant_token="",
            )
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.error, "")
        self.assertGreaterEqual(result.latency_ms, 0.0)

    def test_probe_once_handles_network_error(self) -> None:
        with patch.object(load_smoke, "urlopen", side_effect=RuntimeError("network down")):
            result = load_smoke._probe_once(
                base_url="http://127.0.0.1:8000",
                target="assistant",
                timeout=2.0,
                assistant_token="token",
            )
        self.assertEqual(result.status_code, 0)
        self.assertIn("network down", result.error.lower())


if __name__ == "__main__":
    unittest.main()
