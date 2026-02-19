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

    def test_main_fails_when_render_persistent_required_but_tmp_fallback(self) -> None:
        def fake_fetch_json(_base_url: str, path: str, _timeout: float) -> dict:
            if path == "/api/health":
                return {"status": "ok", "service": "sales-agent"}
            if path == "/api/runtime/diagnostics":
                return {
                    "status": "warn",
                    "runtime": {
                        "telegram_mode": "webhook",
                        "telegram_webhook_secret_set": True,
                        "running_on_render": True,
                        "persistent_data_root": "/tmp",
                        "database_on_persistent_storage": False,
                        "vector_meta_on_persistent_storage": False,
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
            result = release_smoke.main(["--require-render-persistent"])

        self.assertEqual(result, 1)
        self.assertIn("[FAIL] render_persistent_storage", stdout.getvalue())

    def test_main_passes_when_render_persistent_required_and_configured(self) -> None:
        def fake_fetch_json(_base_url: str, path: str, _timeout: float) -> dict:
            if path == "/api/health":
                return {"status": "ok", "service": "sales-agent"}
            if path == "/api/runtime/diagnostics":
                return {
                    "status": "ok",
                    "runtime": {
                        "telegram_mode": "webhook",
                        "telegram_webhook_secret_set": True,
                        "running_on_render": True,
                        "persistent_data_root": "/var/data",
                        "database_on_persistent_storage": True,
                        "vector_meta_on_persistent_storage": True,
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
            result = release_smoke.main(["--require-render-persistent"])

        self.assertEqual(result, 0)
        self.assertIn("[OK] render_persistent_storage", stdout.getvalue())

    def test_main_checks_telegram_webhook_info(self) -> None:
        def fake_fetch_json(_base_url: str, path: str, _timeout: float) -> dict:
            if path == "/api/health":
                return {"status": "ok", "service": "sales-agent"}
            if path == "/api/runtime/diagnostics":
                return {
                    "status": "ok",
                    "runtime": {
                        "telegram_mode": "webhook",
                        "telegram_webhook_path": "/telegram/webhook",
                        "telegram_webhook_secret_set": True,
                    },
                }
            if path == "/api/miniapp/meta":
                return {"ok": True, "advisor_name": "Гид"}
            if path == "/":
                return {"status": "ok", "user_miniapp": {"status": "ready"}}
            raise AssertionError(f"Unexpected path: {path}")

        with patch.object(release_smoke, "_fetch_json", side_effect=fake_fetch_json), patch.object(
            release_smoke, "_fetch_status", return_value=200
        ), patch.object(
            release_smoke,
            "_fetch_telegram_webhook_info",
            return_value={
                "ok": True,
                "result": {
                    "url": "https://example.com/telegram/webhook",
                    "pending_update_count": 0,
                    "last_error_message": "",
                },
            },
        ), patch.dict(
            "os.environ",
            {"TELEGRAM_BOT_TOKEN": "token"},
            clear=True,
        ), patch("sys.stdout", new_callable=StringIO) as stdout:
            result = release_smoke.main(
                [
                    "--base-url",
                    "https://example.com",
                    "--require-webhook-mode",
                    "--check-telegram-webhook",
                ]
            )

        self.assertEqual(result, 0)
        text = stdout.getvalue()
        self.assertIn("[OK] telegram_webhook_info", text)
        self.assertIn("[OK] telegram_webhook_expected_url", text)


if __name__ == "__main__":
    unittest.main()
