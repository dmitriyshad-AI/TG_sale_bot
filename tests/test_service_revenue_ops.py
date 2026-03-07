import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from sales_agent.sales_api.services.revenue_ops import RevenueOpsService
from sales_agent.sales_core import db
from sales_agent.sales_core.config import Settings
from sales_agent.sales_core.mango_client import MangoCallEvent, MangoClientError


class _FakeUpload:
    def __init__(self, filename: str, payload: bytes) -> None:
        self.filename = filename
        self._payload = payload
        self.closed = False

    async def read(self) -> bytes:
        return self._payload

    async def close(self) -> None:
        self.closed = True


class RevenueOpsServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "revenue_ops.db"
        db.init_db(self.db_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _settings(self, **overrides) -> Settings:
        values = dict(
            telegram_bot_token="",
            openai_api_key="sk-test",
            openai_model="gpt-4.1",
            tallanto_api_url="",
            tallanto_api_key="",
            brand_default="kmipt",
            database_path=self.db_path,
            catalog_path=Path("catalog/products.yaml"),
            knowledge_path=Path("knowledge"),
            vector_store_meta_path=Path("data/vector_store.json"),
            openai_vector_store_id="",
            admin_user="admin",
            admin_pass="secret",
            enable_call_copilot=True,
            enable_mango_auto_ingest=True,
            enable_lead_radar=True,
            lead_radar_scheduler_enabled=False,
            lead_radar_no_reply_hours=1,
            lead_radar_call_no_next_step_hours=1,
            lead_radar_stale_warm_days=1,
            lead_radar_max_items_per_run=20,
            lead_radar_thread_cooldown_hours=0,
            lead_radar_daily_cap_per_thread=10,
            mango_api_base_url="https://mango.example/api",
            mango_api_token="mango-token",
            mango_webhook_secret="mango-secret",
            mango_poll_limit_per_run=50,
            mango_poll_retry_attempts=2,
            mango_poll_retry_backoff_seconds=0,
            mango_retry_failed_limit_per_run=20,
            mango_call_recording_ttl_hours=1,
        )
        values.update(overrides)
        return Settings(**values)

    @staticmethod
    def _require_user_exists(conn, user_id: int) -> None:
        row = conn.execute("SELECT id FROM users WHERE id = ? LIMIT 1", (int(user_id),)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="user not found")

    @staticmethod
    def _thread_id(user_id: int) -> str:
        return f"tg:{int(user_id)}"

    def _service(self, settings: Settings) -> RevenueOpsService:
        return RevenueOpsService(
            settings=settings,
            db_path=self.db_path,
            require_user_exists=self._require_user_exists,
            thread_id_from_user_id=self._thread_id,
            lead_radar_rule_no_reply="radar:no_reply",
            lead_radar_rule_call_no_next_step="radar:call_no_next_step",
            lead_radar_rule_stale_warm="radar:stale_warm",
            lead_radar_model_name="lead_radar_v1",
            call_copilot_model_name="call_copilot_v1",
            mango_cleanup_batch_size=50,
        )

    async def test_file_storage_and_priority_helpers(self) -> None:
        settings = self._settings(persistent_data_root=Path(self.tempdir.name) / "persist")
        service = self._service(settings)
        self.assertIn("calls_uploads", str(service._calls_storage_root()))
        self.assertEqual(service._priority_from_warmth("HOT"), "hot")
        self.assertEqual(service._priority_from_warmth("unknown"), "warm")
        self.assertEqual(service._safe_thread_id(123), "")
        self.assertEqual(service._safe_thread_id(" tg:1 "), "tg:1")
        conn = db.get_connection(self.db_path)
        try:
            self.assertEqual(service._count_recent_radar_followups(conn, thread_id="x", hours=0), 0)
        finally:
            conn.close()

        no_name_upload = _FakeUpload("", b"abc")
        self.assertIsNone(await service._store_call_upload_file(no_name_upload))

    async def test_resolve_call_thread_and_user_paths(self) -> None:
        service = self._service(self._settings())
        conn = db.get_connection(self.db_path)
        try:
            user_id = db.get_or_create_user(conn, channel="telegram", external_id="u1")
            resolved_user_id, thread_id = service._resolve_call_thread_and_user(
                conn, user_id_input=user_id, thread_id_input=None
            )
            self.assertEqual((resolved_user_id, thread_id), (user_id, f"tg:{user_id}"))

            resolved_user_id2, thread_id2 = service._resolve_call_thread_and_user(
                conn, user_id_input=None, thread_id_input=f"tg:{user_id}"
            )
            self.assertEqual((resolved_user_id2, thread_id2), (user_id, f"tg:{user_id}"))

            db.upsert_business_connection(
                conn,
                business_connection_id="bc-1",
                can_reply=True,
                is_enabled=True,
            )
            biz_thread = db.upsert_business_thread(
                conn,
                business_connection_id="bc-1",
                chat_id=555,
                user_id=user_id,
                direction="inbound",
            )
            resolved_user_id3, thread_id3 = service._resolve_call_thread_and_user(
                conn, user_id_input=None, thread_id_input=biz_thread
            )
            self.assertEqual((resolved_user_id3, thread_id3), (user_id, biz_thread))
        finally:
            conn.close()

    async def test_cleanup_old_call_files_counts_missing(self) -> None:
        service = self._service(self._settings())
        conn = db.get_connection(self.db_path)
        try:
            user_id = db.get_or_create_user(conn, channel="telegram", external_id="u-clean")
            call_id = db.create_call_record(
                conn,
                user_id=user_id,
                thread_id=f"tg:{user_id}",
                source_type="upload",
                source_ref="x",
                file_path=str(Path(self.tempdir.name) / "missing.wav"),
                status="done",
                created_by="test",
            )
            conn.execute(
                "UPDATE call_records SET created_at = datetime('now', '-2 hours'), updated_at = datetime('now', '-2 hours') WHERE id = ?",
                (call_id,),
            )
            conn.commit()
        finally:
            conn.close()

        result = service.cleanup_old_call_files()
        self.assertTrue(result["ok"])
        self.assertEqual(result["missing"], 1)

    async def test_collect_call_no_next_step_candidates_invalid_payload(self) -> None:
        service = self._service(self._settings())
        conn = db.get_connection(self.db_path)
        try:
            user_id = db.get_or_create_user(conn, channel="telegram", external_id="u-call-json")
            conn.execute(
                """
                INSERT INTO approval_actions (draft_id, user_id, thread_id, action, actor, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now', '-2 hours'))
                """,
                (None, user_id, f"tg:{user_id}", "manual_action", "test", "{call"),
            )
            conn.commit()
            items = service._collect_call_no_next_step_candidates(conn, call_no_next_step_hours=1, limit=10)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["payload"], {})
        finally:
            conn.close()

    async def test_run_lead_radar_disabled(self) -> None:
        service = self._service(self._settings(enable_lead_radar=False))
        result = await service.run_lead_radar_once(trigger="test")
        self.assertFalse(result["enabled"])
        self.assertEqual(result["created_drafts"], 0)

    async def test_run_lead_radar_dry_run_covers_three_rules(self) -> None:
        service = self._service(self._settings())
        conn = db.get_connection(self.db_path)
        try:
            user_id = db.get_or_create_user(conn, channel="telegram", external_id="u-radar")
            thread_id = f"tg:{user_id}"
            db.log_message(conn, user_id=user_id, direction="inbound", text="нужна помощь", meta={})
            conn.execute("UPDATE messages SET created_at = datetime('now', '-2 hours') WHERE user_id = ?", (user_id,))

            db.create_approval_action(
                conn,
                draft_id=None,
                user_id=user_id,
                thread_id=thread_id,
                action="manual_action",
                actor="test",
                payload={"note": "call happened"},
            )
            conn.execute(
                """
                UPDATE approval_actions
                SET created_at = datetime('now', '-2 hours')
                WHERE user_id = ? AND action = 'manual_action'
                """,
                (user_id,),
            )
            db.create_lead_score(
                conn,
                user_id=user_id,
                thread_id=thread_id,
                score=72.0,
                temperature="warm",
                confidence=0.7,
                factors={"src": "test"},
            )
            conn.execute("UPDATE lead_scores SET created_at = datetime('now', '-2 days') WHERE user_id = ?", (user_id,))
            conn.commit()
        finally:
            conn.close()

        result = await service.run_lead_radar_once(trigger="dry-run", dry_run=True)
        self.assertTrue(result["ok"])
        self.assertEqual(result["rules"]["radar:no_reply"]["created"], 1)
        self.assertEqual(result["rules"]["radar:call_no_next_step"]["created"], 1)
        self.assertEqual(result["rules"]["radar:stale_warm"]["created"], 1)

    async def test_run_lead_radar_handles_skip_branches(self) -> None:
        service = self._service(self._settings())

        # Force branches for invalid/missing user and guard skip reasons.
        service._collect_no_reply_candidates = lambda conn, no_reply_hours, limit: [  # type: ignore[assignment]
            {"user_id": 0, "thread_id": "", "inbound_message_id": 1, "inbound_text": "x"},
        ]
        service._collect_business_no_reply_candidates = lambda conn, no_reply_hours, limit: []  # type: ignore[assignment]
        service._collect_call_no_next_step_candidates = lambda conn, call_no_next_step_hours, limit: [  # type: ignore[assignment]
            {"user_id": 1, "thread_id": "tg:1", "action_id": 1, "created_at": "x"},
        ]
        service._collect_stale_warm_candidates = lambda conn, stale_warm_days, limit: [  # type: ignore[assignment]
            {"user_id": 1, "thread_id": "tg:1", "score_id": 1, "score": 50, "created_at": "x"},
        ]
        service._lead_radar_guard = (  # type: ignore[assignment]
            lambda conn, thread_id, rule_key, source_token: (False, "daily_cap")
        )

        result = await service.run_lead_radar_once(trigger="skip-test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["rules"]["radar:call_no_next_step"]["skipped_daily_cap"], 1)
        self.assertEqual(result["rules"]["radar:stale_warm"]["skipped_daily_cap"], 1)

    async def test_run_lead_radar_skips_duplicate_trigger(self) -> None:
        service = self._service(self._settings(lead_radar_thread_cooldown_hours=0, lead_radar_daily_cap_per_thread=10))
        conn = db.get_connection(self.db_path)
        try:
            user_id = db.get_or_create_user(conn, channel="telegram", external_id="u-radar-dup")
            thread_id = f"tg:{user_id}"
            db.log_message(conn, user_id=user_id, direction="inbound", text="есть вопрос", meta={})
            inbound_id_row = conn.execute(
                "SELECT MAX(id) AS inbound_id FROM messages WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            inbound_id = int(inbound_id_row["inbound_id"]) if inbound_id_row and inbound_id_row["inbound_id"] else 0
            conn.execute(
                "UPDATE messages SET created_at = datetime('now', '-3 hours') WHERE user_id = ?",
                (user_id,),
            )
            db.create_followup_task(
                conn,
                user_id=user_id,
                thread_id=thread_id,
                priority="warm",
                reason=f"radar:no_reply [source=msg:{inbound_id}] :: уже был follow-up",
                status="done",
                assigned_to="lead_radar:auto",
            )
            conn.commit()
        finally:
            conn.close()

        result = await service.run_lead_radar_once(trigger="dup-test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["created_followups"], 0)
        self.assertGreaterEqual(result["rules"]["radar:no_reply"]["skipped_duplicate_trigger"], 1)

    async def test_process_manual_call_upload_validation_and_error_path(self) -> None:
        service = self._service(self._settings())
        with self.assertRaises(HTTPException) as empty_exc:
            await service.process_manual_call_upload(
                user_id=None,
                thread_id=None,
                recording_url=None,
                transcript_hint=None,
                audio_file=None,
            )
        self.assertEqual(empty_exc.exception.status_code, 400)

        upload = _FakeUpload("a.wav", b"")
        with self.assertRaises(HTTPException) as file_exc:
            await service.process_manual_call_upload(
                user_id=None,
                thread_id=None,
                recording_url=None,
                transcript_hint=None,
                audio_file=upload,
            )
        self.assertEqual(file_exc.exception.status_code, 400)
        self.assertTrue(upload.closed)

        with patch("sales_agent.sales_api.services.revenue_ops.build_call_insights", side_effect=RuntimeError("boom")):
            with self.assertRaises(HTTPException) as processing_exc:
                await service.process_manual_call_upload(
                    user_id=None,
                    thread_id=None,
                    recording_url="https://example.com/r.mp3",
                    transcript_hint="hint",
                    audio_file=None,
                )
        self.assertEqual(processing_exc.exception.status_code, 500)

    async def test_ingest_mango_event_branches(self) -> None:
        service = self._service(self._settings())
        event = MangoCallEvent(
            event_id="evt-1",
            call_id="call-1",
            phone="",
            recording_url="",
            transcript_hint="",
            occurred_at="",
            payload={},
        )

        with self.assertRaises(ValueError):
            await service.ingest_mango_event(event=event, source="test", existing_event_row_id=999999)

        with patch.object(service, "process_manual_call_upload", new=AsyncMock(side_effect=RuntimeError("fail"))):
            with self.assertRaises(RuntimeError):
                await service.ingest_mango_event(event=event, source="webhook")

        # duplicate branch
        duplicate_event = MangoCallEvent(
            event_id="evt-2",
            call_id="call-2",
            phone="",
            recording_url="",
            transcript_hint="",
            occurred_at="",
            payload={},
        )
        with patch.object(
            service,
            "process_manual_call_upload",
            new=AsyncMock(return_value={"item": {"id": 1}}),
        ):
            first = await service.ingest_mango_event(event=duplicate_event, source="webhook")
            second = await service.ingest_mango_event(event=duplicate_event, source="webhook")
        self.assertFalse(first.get("duplicate"))
        self.assertTrue(second.get("duplicate"))

    async def test_run_mango_poll_once_and_retry_branches(self) -> None:
        # disabled
        disabled_service = self._service(self._settings(enable_mango_auto_ingest=False))
        disabled = await disabled_service.run_mango_poll_once(trigger="x")
        self.assertFalse(disabled["enabled"])

        # config error
        missing_cfg_service = self._service(self._settings(mango_api_base_url="", mango_api_token=""))
        missing_cfg = await missing_cfg_service.run_mango_poll_once(trigger="x")
        self.assertIn("error", missing_cfg)

        service = self._service(self._settings())
        event = MangoCallEvent(
            event_id="evt-p-1",
            call_id="call-p-1",
            phone="",
            recording_url="https://example.com/a.mp3",
            transcript_hint="",
            occurred_at="",
            payload={},
        )

        with patch(
            "sales_agent.sales_api.services.revenue_ops.fetch_mango_poll_events_with_retries",
            side_effect=MangoClientError("temporary"),
        ):
            failed_fetch = await service.run_mango_poll_once(trigger="x")
        self.assertIn("error", failed_fetch)

        with patch(
            "sales_agent.sales_api.services.revenue_ops.fetch_mango_poll_events_with_retries",
            return_value=([event], 1),
        ), patch.object(service, "ingest_mango_event", side_effect=RuntimeError("ingest failed")):
            processed = await service.run_mango_poll_once(trigger="x")
        self.assertEqual(processed["failed"], 1)

        # retry disabled
        retry_disabled = await disabled_service.run_mango_retry_failed_once(trigger="x")
        self.assertFalse(retry_disabled["enabled"])

        # retry parse failed + exception path
        conn = db.get_connection(self.db_path)
        try:
            created = db.create_or_get_mango_event(
                conn,
                event_id="evt-f-1",
                call_external_id=None,
                source="test",
                payload={"no": "event"},
            )
            db.update_mango_event_status(conn, event_row_id=int(created["id"]), status="failed", error_text="x")
            created2 = db.create_or_get_mango_event(
                conn,
                event_id="evt-f-2",
                call_external_id="call-f-2",
                source="test",
                payload={"event": "call_record", "call": {"id": "call-f-2"}},
            )
            db.update_mango_event_status(conn, event_row_id=int(created2["id"]), status="failed", error_text="x")
        finally:
            conn.close()

        with patch.object(service, "ingest_mango_event", side_effect=RuntimeError("retry failed")):
            retry = await service.run_mango_retry_failed_once(trigger="x")
        self.assertGreaterEqual(retry["failed"], 1)

    async def test_run_mango_retry_failed_skips_not_failed_claims(self) -> None:
        service = self._service(self._settings())
        conn = db.get_connection(self.db_path)
        try:
            created = db.create_or_get_mango_event(
                conn,
                event_id="evt-claim-skip-1",
                call_external_id="call-claim-skip-1",
                source="test",
                payload={"event": "call_record", "call": {"id": "call-claim-skip-1"}},
            )
            db.update_mango_event_status(conn, event_row_id=int(created["id"]), status="failed", error_text="x")
        finally:
            conn.close()

        with patch("sales_agent.sales_api.services.revenue_ops.claim_failed_mango_event_for_retry", return_value="not_failed"):
            retry = await service.run_mango_retry_failed_once(trigger="x")
        self.assertTrue(retry["ok"])
        self.assertEqual(retry["processed"], 0)
        self.assertEqual(retry["retried"], 0)
        self.assertEqual(retry["failed"], 0)
        self.assertGreaterEqual(retry["skipped_not_failed"], 1)

    async def test_run_call_retry_failed_once_branches(self) -> None:
        service = self._service(self._settings())

        disabled = await self._service(self._settings(enable_call_copilot=False)).run_call_retry_failed_once(trigger="x")
        self.assertFalse(disabled["enabled"])

        conn = db.get_connection(self.db_path)
        try:
            user_id = db.get_or_create_user(conn, channel="telegram", external_id="retry-call-user")
            call_id = db.create_call_record(
                conn,
                user_id=user_id,
                thread_id=f"tg:{user_id}",
                source_type="url",
                source_ref="https://example.com/retry.wav",
                status="failed",
                error_text="transient",
                created_by="test",
            )
            call_id_materialized = db.create_call_record(
                conn,
                user_id=user_id,
                thread_id=f"tg:{user_id}",
                source_type="upload",
                source_ref="retry-materialized",
                status="failed",
                error_text="old",
                created_by="test",
            )
            db.create_approval_action(
                conn,
                draft_id=None,
                user_id=user_id,
                thread_id=f"tg:{user_id}",
                action="manual_action",
                actor="test",
                payload={"call_id": call_id_materialized, "source": "legacy"},
            )
        finally:
            conn.close()

        result = await service.run_call_retry_failed_once(trigger="manual-test", limit_override=20)
        self.assertTrue(result["ok"])
        self.assertEqual(result["processed"], 2)
        self.assertGreaterEqual(result["retried"], 1)
        self.assertGreaterEqual(result["skipped_already_materialized"], 1)

        conn_check = db.get_connection(self.db_path)
        try:
            first = db.get_call_record(conn_check, call_id=call_id)
            second = db.get_call_record(conn_check, call_id=call_id_materialized)
            self.assertIsNotNone(first)
            self.assertIsNotNone(second)
            assert first is not None
            assert second is not None
            self.assertEqual(first["status"], "done")
            self.assertEqual(second["status"], "done")
        finally:
            conn_check.close()

    async def test_run_call_retry_failed_once_skips_not_failed_claim(self) -> None:
        service = self._service(self._settings())
        conn = db.get_connection(self.db_path)
        try:
            user_id = db.get_or_create_user(conn, channel="telegram", external_id="retry-claim-user")
            db.create_call_record(
                conn,
                user_id=user_id,
                thread_id=f"tg:{user_id}",
                source_type="url",
                source_ref="https://example.com/retry2.wav",
                status="failed",
                error_text="x",
                created_by="test",
            )
        finally:
            conn.close()

        with patch("sales_agent.sales_api.services.revenue_ops.claim_failed_call_record_for_retry", return_value="not_failed"):
            retry = await service.run_call_retry_failed_once(trigger="x")
        self.assertTrue(retry["ok"])
        self.assertEqual(retry["processed"], 0)
        self.assertGreaterEqual(retry["skipped_not_failed"], 1)


if __name__ == "__main__":
    unittest.main()
