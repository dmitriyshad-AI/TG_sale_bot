import hashlib
import hmac
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core import db
    from sales_agent.sales_core.config import Settings
    from sales_agent.sales_core.mango_client import MangoCallEvent, MangoClientError
    from tests.test_client_compat import build_test_client

    HAS_API_DEPS = True
except ModuleNotFoundError:
    HAS_API_DEPS = False


@unittest.skipUnless(HAS_API_DEPS, "fastapi dependencies are not installed")
class ApiMangoIngestTests(unittest.TestCase):
    def _settings(self, db_path: Path, *, webhook_secret: str = "mango-secret") -> Settings:
        return Settings(
            telegram_bot_token="",
            openai_api_key="sk-test",
            openai_model="gpt-4.1",
            tallanto_api_url="",
            tallanto_api_key="",
            brand_default="kmipt",
            database_path=db_path,
            catalog_path=Path("catalog/products.yaml"),
            knowledge_path=Path("knowledge"),
            vector_store_meta_path=Path("data/vector_store.json"),
            openai_vector_store_id="",
            admin_user="admin",
            admin_pass="secret",
            enable_call_copilot=True,
            enable_mango_auto_ingest=True,
            mango_api_base_url="https://mango.example/api",
            mango_api_token="mango-token",
            mango_webhook_secret=webhook_secret,
            mango_polling_enabled=False,
            mango_poll_interval_seconds=120,
            mango_call_recording_ttl_hours=1,
            mango_poll_limit_per_run=50,
            mango_poll_retry_attempts=3,
            mango_poll_retry_backoff_seconds=0,
            mango_retry_failed_limit_per_run=10,
        )

    def _settings_disabled(self, db_path: Path) -> Settings:
        settings = self._settings(db_path)
        settings.enable_mango_auto_ingest = False
        return settings

    def _settings_missing_mango_config(self, db_path: Path) -> Settings:
        settings = self._settings(db_path)
        settings.mango_api_base_url = ""
        settings.mango_api_token = ""
        return settings

    @staticmethod
    def _sign(secret: str, body: bytes) -> str:
        return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

    def test_mango_webhook_processes_event_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="mango-user-1",
                )
                db.create_lead_record(
                    conn=conn,
                    user_id=user_id,
                    status="created",
                    contact={"phone": "+79990000011"},
                    tallanto_entry_id=None,
                )
            finally:
                conn.close()

            client = build_test_client(app)
            payload = {
                "event": "call_recording_ready",
                "event_id": "evt-1",
                "data": {
                    "call_id": "call-1",
                    "phone": "+79990000011",
                    "recording_url": "https://cdn.example/rec-1.mp3",
                },
            }
            raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            headers = {
                "Content-Type": "application/json",
                "X-Mango-Signature": self._sign("mango-secret", raw),
            }

            first = client.post("/integrations/mango/webhook", content=raw, headers=headers)
            self.assertEqual(first.status_code, 200)
            first_payload = first.json()
            self.assertTrue(first_payload["ok"])
            self.assertFalse(first_payload["result"]["duplicate"])

            second = client.post("/integrations/mango/webhook", content=raw, headers=headers)
            self.assertEqual(second.status_code, 200)
            self.assertTrue(second.json()["result"]["duplicate"])

            auth = ("admin", "secret")
            calls = client.get("/admin/calls", auth=auth)
            self.assertEqual(calls.status_code, 200)
            items = calls.json()["items"]
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["source_type"], "mango")

            events = client.get("/admin/calls/mango/events", auth=auth)
            self.assertEqual(events.status_code, 200)
            event_items = events.json()["items"]
            self.assertEqual(len(event_items), 1)
            self.assertEqual(event_items[0]["status"], "done")
            self.assertEqual(event_items[0]["event_id"], "evt-1")

    def test_mango_webhook_rejects_invalid_signature(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            payload = {"event": "call_recording_ready", "data": {"call_id": "x", "recording_url": "https://x"}}
            raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            response = client.post(
                "/integrations/mango/webhook",
                content=raw,
                headers={"Content-Type": "application/json", "X-Mango-Signature": "bad"},
            )
            self.assertEqual(response.status_code, 403)

    def test_mango_webhook_ignored_for_non_call_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            payload = {"event": "contact_updated", "data": {"id": "x"}}
            raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            headers = {
                "Content-Type": "application/json",
                "X-Mango-Signature": self._sign("mango-secret", raw),
            }
            response = client.post("/integrations/mango/webhook", content=raw, headers=headers)
            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertTrue(body["ok"])
            self.assertTrue(body["ignored"])
            self.assertEqual(body["reason"], "not_call_event")

    def test_mango_webhook_rejects_invalid_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            response = client.post(
                "/integrations/mango/webhook",
                content=b"[1,2,3]",
                headers={
                    "Content-Type": "application/json",
                    "X-Mango-Signature": self._sign("mango-secret", b"[1,2,3]"),
                },
            )
            self.assertEqual(response.status_code, 400)

    def test_mango_webhook_returns_404_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings_disabled(db_path))
            client = build_test_client(app)
            response = client.post(
                "/integrations/mango/webhook",
                content=b"{}",
                headers={"Content-Type": "application/json"},
            )
            self.assertEqual(response.status_code, 404)

    def test_mango_webhook_returns_503_when_config_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings_missing_mango_config(db_path))
            client = build_test_client(app)
            response = client.post(
                "/integrations/mango/webhook",
                content=b"{}",
                headers={"Content-Type": "application/json"},
            )
            self.assertEqual(response.status_code, 503)

    @patch("sales_agent.sales_api.main.MangoClient.list_recent_calls")
    def test_mango_poll_endpoint(self, mock_list_recent_calls) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings(db_path))
            mock_list_recent_calls.return_value = [
                MangoCallEvent(
                    event_id="evt-poll-1",
                    call_id="poll-call-1",
                    phone="",
                    recording_url="https://cdn.example/a1.mp3",
                    transcript_hint="",
                    occurred_at="",
                    payload={"event": "call_recording_ready"},
                ),
                MangoCallEvent(
                    event_id="evt-poll-2",
                    call_id="poll-call-2",
                    phone="",
                    recording_url="https://cdn.example/a2.mp3",
                    transcript_hint="",
                    occurred_at="",
                    payload={"event": "call_recording_ready"},
                ),
            ]

            client = build_test_client(app)
            auth = ("admin", "secret")
            response = client.post("/admin/calls/mango/poll?limit=2", auth=auth)
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertTrue(data["enabled"])
            self.assertEqual(data["processed"], 2)
            self.assertEqual(data["created"], 2)
            self.assertEqual(data["duplicates"], 0)
            self.assertEqual(data["failed"], 0)
            self.assertEqual(data["attempts"], 1)

            calls = client.get("/admin/calls", auth=auth)
            self.assertEqual(calls.status_code, 200)
            self.assertEqual(len(calls.json()["items"]), 2)

    @patch("sales_agent.sales_api.main.MangoClient.list_recent_calls")
    def test_mango_poll_endpoint_retries_transient_api_error(self, mock_list_recent_calls) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            settings = self._settings(db_path)
            settings.mango_poll_retry_attempts = 3
            settings.mango_poll_retry_backoff_seconds = 0
            app = create_app(settings)
            mock_list_recent_calls.side_effect = [
                MangoClientError("temporary network"),
                MangoClientError("temporary network"),
                [
                    MangoCallEvent(
                        event_id="evt-retry-1",
                        call_id="call-retry-1",
                        phone="",
                        recording_url="https://cdn.example/retry.mp3",
                        transcript_hint="",
                        occurred_at="",
                        payload={"event": "call_recording_ready"},
                    )
                ],
            ]

            client = build_test_client(app)
            auth = ("admin", "secret")
            response = client.post("/admin/calls/mango/poll?limit=1", auth=auth)
            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertTrue(body["ok"])
            self.assertEqual(body["attempts"], 3)
            self.assertEqual(body["processed"], 1)
            self.assertEqual(body["created"], 1)

    @patch("sales_agent.sales_api.main.MangoClient.list_recent_calls")
    def test_mango_poll_endpoint_returns_error_after_retry_exhausted(self, mock_list_recent_calls) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            settings = self._settings(db_path)
            settings.mango_poll_retry_attempts = 2
            settings.mango_poll_retry_backoff_seconds = 0
            app = create_app(settings)
            mock_list_recent_calls.side_effect = MangoClientError("mango timeout")

            client = build_test_client(app)
            auth = ("admin", "secret")
            response = client.post("/admin/calls/mango/poll", auth=auth)
            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertFalse(body["ok"])
            self.assertEqual(body["attempts"], 2)
            self.assertEqual(body["processed"], 0)
            self.assertIn("mango timeout", body["error"].lower())

    def test_mango_retry_failed_endpoint_reprocesses_failed_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="mango-user-retry")
                db.create_lead_record(
                    conn=conn,
                    user_id=user_id,
                    status="created",
                    contact={"phone": "+79995550011"},
                    tallanto_entry_id=None,
                )
                state = db.create_or_get_mango_event(
                    conn,
                    event_id="evt-failed-1",
                    call_external_id="call-failed-1",
                    source="webhook",
                    payload={
                        "event": "call_recording_ready",
                        "event_id": "evt-failed-1",
                        "data": {
                            "call_id": "call-failed-1",
                            "phone": "+79995550011",
                            "recording_url": "https://cdn.example/retry-failed.mp3",
                        },
                    },
                )
                db.update_mango_event_status(
                    conn,
                    event_row_id=int(state["id"]),
                    status="failed",
                    error_text="temporary fail",
                )
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            retry_response = client.post("/admin/calls/mango/retry-failed?limit=5", auth=auth)
            self.assertEqual(retry_response.status_code, 200)
            payload = retry_response.json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["processed"], 1)
            self.assertEqual(payload["retried"], 1)
            self.assertEqual(payload["failed"], 0)

            events_response = client.get("/admin/calls/mango/events?status=done", auth=auth)
            self.assertEqual(events_response.status_code, 200)
            items = events_response.json()["items"]
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["event_id"], "evt-failed-1")

    def test_mango_poll_endpoint_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings_disabled(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")
            response = client.post("/admin/calls/mango/poll", auth=auth)
            self.assertEqual(response.status_code, 503)
            retry_response = client.post("/admin/calls/mango/retry-failed", auth=auth)
            self.assertEqual(retry_response.status_code, 503)

    def test_mango_poll_endpoint_reports_config_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings_missing_mango_config(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")
            response = client.post("/admin/calls/mango/poll", auth=auth)
            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertFalse(body["ok"])
            self.assertTrue(body["enabled"])
            self.assertIn("Mango API is not configured", body["error"])

    @patch("sales_agent.sales_api.main.MangoClient.list_recent_calls")
    def test_mango_poll_cleans_old_recordings(self, mock_list_recent_calls) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mango.db"
            app = create_app(self._settings(db_path))
            mock_list_recent_calls.return_value = []

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="cleanup-user")
                audio_file = Path(tmpdir) / "old_audio.raw"
                audio_file.write_bytes(b"abc")
                call_id = db.create_call_record(
                    conn,
                    user_id=user_id,
                    thread_id=f"tg:{user_id}",
                    source_type="upload",
                    source_ref="old-file",
                    file_path=str(audio_file),
                    status="done",
                    created_by="test",
                )
                conn.execute(
                    "UPDATE call_records SET created_at = datetime('now', '-5 hours') WHERE id = ?",
                    (call_id,),
                )
                conn.commit()
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            response = client.post("/admin/calls/mango/poll", auth=auth)
            self.assertEqual(response.status_code, 200)
            cleanup = response.json()["cleanup"]
            self.assertEqual(cleanup["cleaned"], 1)
            self.assertFalse(audio_file.exists())

            conn_verify = db.get_connection(db_path)
            try:
                item = db.get_call_record(conn_verify, call_id=call_id)
            finally:
                conn_verify.close()
            self.assertIsNotNone(item)
            assert item is not None
            self.assertIsNone(item["file_path"])


if __name__ == "__main__":
    unittest.main()
