import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from scripts import mango_offline_smoke

    HAS_DEPS = True
except ModuleNotFoundError:
    HAS_DEPS = False


@unittest.skipUnless(HAS_DEPS, "mango offline smoke dependencies are not installed")
class MangoOfflineSmokeScriptTests(unittest.TestCase):
    def test_load_payload_requires_json_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "payload.json"
            path.write_text("[1,2,3]", encoding="utf-8")
            with self.assertRaises(ValueError):
                mango_offline_smoke._load_payload(path)

    def test_run_offline_smoke_ok(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture_path = Path(tmpdir) / "fixture.json"
            fixture_path.write_text(
                json.dumps(
                    {
                        "event": "call_recording_ready",
                        "event_id": "fixture-event-script",
                        "data": {
                            "call_id": "fixture-call-script",
                            "phone": "+79990000011",
                            "recording_url": "https://cdn.example/fixture-script.mp3",
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            db_path = Path(tmpdir) / "smoke.db"
            ok, lines = mango_offline_smoke.run_offline_smoke(
                fixture_path=fixture_path,
                webhook_secret="mango-secret",
                db_path=db_path,
            )
        self.assertTrue(ok)
        text = "\n".join(lines)
        self.assertIn("[OK] webhook_ingest", text)
        self.assertIn("Offline Mango smoke: OK", text)

    def test_run_offline_smoke_can_report_failed_checks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture_path = Path(tmpdir) / "fixture_bad.json"
            fixture_path.write_text(
                json.dumps({"event": "contact_updated", "data": {"id": "x"}}, ensure_ascii=False),
                encoding="utf-8",
            )
            db_path = Path(tmpdir) / "smoke_bad.db"
            ok, lines = mango_offline_smoke.run_offline_smoke(
                fixture_path=fixture_path,
                webhook_secret="mango-secret",
                db_path=db_path,
            )
        self.assertFalse(ok)
        self.assertTrue(any("[FAIL]" in line for line in lines))

    def test_ensure_fixture_event_populates_defaults(self) -> None:
        payload = mango_offline_smoke._ensure_fixture_event({})
        self.assertEqual(payload["event"], "call_recording_ready")
        self.assertEqual(payload["event_id"], "fixture-event-1")
        self.assertEqual(payload["data"]["call_id"], "fixture-call-1")
        self.assertEqual(payload["data"]["phone"], "+79990000011")
        self.assertIn("recording_url", payload["data"])

    def test_main_fails_for_missing_fixture(self) -> None:
        with patch("sys.stdout.write"):
            code = mango_offline_smoke.main(["--fixture", "/tmp/does-not-exist.json"])
        self.assertEqual(code, 1)

    def test_main_runs_with_explicit_db_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture_path = Path(tmpdir) / "fixture.json"
            fixture_path.write_text(
                json.dumps(
                    {
                        "event": "call_recording_ready",
                        "event_id": "fixture-event-main-db",
                        "data": {
                            "call_id": "fixture-call-main-db",
                            "phone": "+79990000011",
                            "recording_url": "https://cdn.example/fixture-main-db.mp3",
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            db_path = Path(tmpdir) / "explicit.db"
            code = mango_offline_smoke.main(["--fixture", str(fixture_path), "--db-path", str(db_path)])
            self.assertTrue(db_path.parent.exists())
        self.assertEqual(code, 0)

    def test_main_runs_with_temporary_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture_path = Path(tmpdir) / "fixture.json"
            fixture_path.write_text(
                json.dumps(
                    {
                        "event": "call_recording_ready",
                        "event_id": "fixture-event-main-tmp",
                        "data": {
                            "call_id": "fixture-call-main-tmp",
                            "phone": "+79990000011",
                            "recording_url": "https://cdn.example/fixture-main-tmp.mp3",
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            code = mango_offline_smoke.main(["--fixture", str(fixture_path)])
        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
