import unittest
from io import StringIO
from types import SimpleNamespace
from unittest.mock import patch

from scripts import start_api


class StartApiScriptTests(unittest.TestCase):
    def test_main_runs_uvicorn_after_preflight(self) -> None:
        settings = SimpleNamespace(startup_preflight_mode="fail")
        with patch.object(start_api, "get_settings", return_value=settings), patch.object(
            start_api, "enforce_startup_preflight", return_value={"status": "ok"}
        ) as mock_preflight, patch.object(start_api.uvicorn, "run") as mock_run, patch(
            "sys.stdout", new_callable=StringIO
        ) as stdout:
            result = start_api.main(["--host", "127.0.0.1", "--port", "8010", "--log-level", "warning"])

        self.assertEqual(result, 0)
        mock_preflight.assert_called_once_with(settings, mode=None)
        mock_run.assert_called_once()
        kwargs = mock_run.call_args.kwargs
        self.assertEqual(kwargs["host"], "127.0.0.1")
        self.assertEqual(kwargs["port"], 8010)
        self.assertEqual(kwargs["log_level"], "warning")
        self.assertIn("preflight=OK", stdout.getvalue())

    def test_main_propagates_preflight_failure(self) -> None:
        settings = SimpleNamespace(startup_preflight_mode="strict")
        with patch.object(start_api, "get_settings", return_value=settings), patch.object(
            start_api, "enforce_startup_preflight", side_effect=RuntimeError("preflight blocked")
        ), patch.object(start_api.uvicorn, "run") as mock_run:
            with self.assertRaises(RuntimeError):
                start_api.main([])

        mock_run.assert_not_called()


if __name__ == "__main__":
    unittest.main()

