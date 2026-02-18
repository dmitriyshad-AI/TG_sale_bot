import unittest
from io import StringIO
from unittest.mock import patch

from scripts import release_smoke


class ReleaseSmokeScriptTests(unittest.TestCase):
    def test_main_returns_ok_when_all_checks_pass(self) -> None:
        def fake_fetch_json(_base_url: str, path: str, _timeout: float) -> dict:
            if path == "/api/health":
                return {"status": "ok", "service": "sales-agent"}
            if path == "/api/runtime/diagnostics":
                return {
                    "status": "ok",
                    "runtime": {
                        "telegram_mode": "polling",
                        "telegram_webhook_secret_set": False,
                    },
                }
            if path == "/api/miniapp/meta":
                return {"ok": True, "advisor_name": "Гид"}
            if path == "/":
                return {"status": "ok", "user_miniapp": {"status": "ready"}}
            raise AssertionError(f"Unexpected path: {path}")

        with patch.object(release_smoke, "_fetch_json", side_effect=fake_fetch_json), patch.object(
            release_smoke, "_fetch_status", return_value=200
        ), patch("sys.stdout", new_callable=StringIO) as stdout:
            result = release_smoke.main(["--require-miniapp-ready"])

        self.assertEqual(result, 0)
        self.assertIn("Smoke result: OK", stdout.getvalue())

    def test_main_fails_when_runtime_is_warn_in_strict_mode(self) -> None:
        def fake_fetch_json(_base_url: str, path: str, _timeout: float) -> dict:
            if path == "/api/health":
                return {"status": "ok", "service": "sales-agent"}
            if path == "/api/runtime/diagnostics":
                return {
                    "status": "warn",
                    "runtime": {
                        "telegram_mode": "webhook",
                        "telegram_webhook_secret_set": False,
                    },
                }
            if path == "/api/miniapp/meta":
                return {"ok": True, "advisor_name": "Гид"}
            if path == "/":
                return {"status": "ok", "user_miniapp": {"status": "build-required"}}
            raise AssertionError(f"Unexpected path: {path}")

        with patch.object(release_smoke, "_fetch_json", side_effect=fake_fetch_json), patch.object(
            release_smoke, "_fetch_status", return_value=200
        ), patch("sys.stdout", new_callable=StringIO) as stdout:
            result = release_smoke.main(["--strict-runtime"])

        self.assertEqual(result, 1)
        text = stdout.getvalue()
        self.assertIn("[FAIL] runtime_diagnostics", text)
        self.assertIn("[OK] webhook_secret", text)
        self.assertIn("Smoke result: FAIL", text)


if __name__ == "__main__":
    unittest.main()
