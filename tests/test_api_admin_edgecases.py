import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

try:
    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core import db
    from sales_agent.sales_core.config import Settings
    from tests.test_client_compat import build_test_client

    HAS_ADMIN_DEPS = True
except ModuleNotFoundError:
    HAS_ADMIN_DEPS = False


@unittest.skipUnless(HAS_ADMIN_DEPS, "fastapi dependencies are not installed")
class ApiAdminEdgeCasesTests(unittest.TestCase):
    def _settings(self, db_path: Path, **overrides) -> Settings:
        values = dict(
            telegram_bot_token="",
            openai_api_key="",
            openai_model="gpt-5.1",
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
            enable_business_inbox=True,
            enable_call_copilot=True,
            enable_tallanto_enrichment=True,
            enable_director_agent=True,
            enable_lead_radar=False,
            lead_radar_scheduler_enabled=False,
            lead_radar_no_reply_hours=1,
            lead_radar_call_no_next_step_hours=1,
            lead_radar_stale_warm_days=1,
            lead_radar_max_items_per_run=20,
            mango_api_base_url="https://mango.example/api",
            mango_api_token="mango-token",
            mango_webhook_secret="secret",
            mango_polling_enabled=False,
            mango_poll_retry_attempts=1,
            mango_poll_retry_backoff_seconds=0,
            mango_poll_limit_per_run=10,
            mango_retry_failed_limit_per_run=10,
            mango_call_recording_ttl_hours=1,
            admin_ui_csrf_enabled=True,
        )
        values.update(overrides)
        return Settings(**values)

    def test_admin_calls_disabled_paths_and_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin_calls_disabled.db"
            app = create_app(self._settings(db_path, enable_call_copilot=False, enable_mango_auto_ingest=False))
            client = build_test_client(app)
            auth = ("admin", "secret")
            origin = {"Origin": "http://testserver"}

            self.assertEqual(client.get("/admin/calls", auth=auth).status_code, 503)
            self.assertEqual(client.get("/admin/calls/1", auth=auth).status_code, 503)
            self.assertEqual(client.post("/admin/calls/upload", auth=auth, data={"transcript_hint": "x"}).status_code, 503)
            self.assertEqual(client.post("/admin/calls/cleanup", auth=auth).status_code, 503)
            self.assertEqual(client.post("/admin/calls/mango/poll", auth=auth).status_code, 503)
            self.assertEqual(client.post("/admin/calls/mango/retry-failed", auth=auth).status_code, 503)
            self.assertEqual(client.get("/admin/ui/calls/1", auth=auth).status_code, 503)

            ui_calls = client.get("/admin/ui/calls", auth=auth)
            self.assertEqual(ui_calls.status_code, 200)
            self.assertIn("Call Copilot disabled", ui_calls.text)

            self.assertEqual(
                client.post("/admin/ui/calls/cleanup", auth=auth, headers=origin, follow_redirects=False).status_code,
                503,
            )
            self.assertEqual(
                client.post(
                    "/admin/ui/calls/upload",
                    auth=auth,
                    headers=origin,
                    data={"transcript_hint": "x"},
                    follow_redirects=False,
                ).status_code,
                503,
            )
            self.assertEqual(
                client.post("/admin/ui/calls/mango/poll", auth=auth, headers=origin, data={}, follow_redirects=False).status_code,
                503,
            )
            self.assertEqual(
                client.post(
                    "/admin/ui/calls/mango/retry-failed",
                    auth=auth,
                    headers=origin,
                    data={},
                    follow_redirects=False,
                ).status_code,
                503,
            )

    def test_admin_calls_mango_events_and_ui_upload_missing_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin_calls_ui.db"
            app = create_app(self._settings(db_path, enable_mango_auto_ingest=True))
            client = build_test_client(app)
            auth = ("admin", "secret")
            origin = {"Origin": "http://testserver"}

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="edge-call-u1")
                long_summary = "S" * 300
                call_id = db.create_call_record(
                    conn,
                    user_id=user_id,
                    thread_id=f"tg:{user_id}",
                    source_type="upload",
                    source_ref="edge-source",
                    file_path=None,
                    status="done",
                    created_by="test",
                )
                db.upsert_call_summary(
                    conn,
                    call_id=call_id,
                    summary_text=long_summary,
                    interests=["math"],
                    objections=["time"],
                    next_best_action="call back",
                    warmth="warm",
                    confidence=0.6,
                    model_name="test",
                )
            finally:
                conn.close()

            # 404 path in API detail
            self.assertEqual(client.get("/admin/calls/999999", auth=auth).status_code, 404)
            self.assertEqual(client.get("/admin/ui/calls/999999", auth=auth).status_code, 404)

            # Mango events list branch
            events_response = client.get("/admin/calls/mango/events", auth=auth, params={"status": "failed", "limit": 1})
            self.assertEqual(events_response.status_code, 200)
            self.assertTrue(events_response.json()["ok"])

            # API manual mango poll/retry-failed happy paths
            poll_api = client.post("/admin/calls/mango/poll?limit=1", auth=auth)
            self.assertEqual(poll_api.status_code, 200)
            retry_api = client.post("/admin/calls/mango/retry-failed?limit=1", auth=auth)
            self.assertEqual(retry_api.status_code, 200)

            # UI listing (includes summary truncation branch)
            ui_calls = client.get("/admin/ui/calls", auth=auth)
            self.assertEqual(ui_calls.status_code, 200)
            self.assertIn("...", ui_calls.text)

            # UI poll/retry/cleanup success redirects
            poll_resp = client.post(
                "/admin/ui/calls/mango/poll",
                auth=auth,
                headers=origin,
                data={"limit": "1"},
                follow_redirects=False,
            )
            self.assertEqual(poll_resp.status_code, 303)
            retry_resp = client.post(
                "/admin/ui/calls/mango/retry-failed",
                auth=auth,
                headers=origin,
                data={"limit": "1"},
                follow_redirects=False,
            )
            self.assertEqual(retry_resp.status_code, 303)
            cleanup_resp = client.post(
                "/admin/ui/calls/cleanup",
                auth=auth,
                headers=origin,
                data={},
                follow_redirects=False,
            )
            self.assertEqual(cleanup_resp.status_code, 303)

            with patch(
                "sales_agent.sales_api.services.revenue_ops.RevenueOpsService.process_manual_call_upload",
                new=AsyncMock(return_value={"ok": True}),
            ):
                bad_upload = client.post(
                    "/admin/ui/calls/upload",
                    auth=auth,
                    headers=origin,
                    data={"transcript_hint": "hello"},
                    follow_redirects=False,
                )
            self.assertEqual(bad_upload.status_code, 500)

    def test_admin_inbox_api_not_found_and_conflict_branches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin_inbox_api_edge.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")

            # inbox detail not found
            self.assertEqual(client.get("/admin/inbox/123456", auth=auth).status_code, 404)

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="edge-inbox-user")
                thread_id = f"tg:{user_id}"

                sent_draft_id = db.create_reply_draft(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    draft_text="sent draft",
                    created_by="test",
                    model_name="x",
                )
                db.update_reply_draft_status(conn, draft_id=sent_draft_id, status="sent", actor="test")

                approved_draft_id = db.create_reply_draft(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    draft_text="approved draft",
                    created_by="test",
                    model_name="x",
                )
                db.update_reply_draft_status(conn, draft_id=approved_draft_id, status="approved", actor="test")

                editable_draft_id = db.create_reply_draft(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    draft_text="editable",
                    created_by="test",
                    model_name="x",
                )
            finally:
                conn.close()

            # API approve/reject not found
            self.assertEqual(client.post("/admin/inbox/drafts/999999/approve", auth=auth).status_code, 404)
            self.assertEqual(client.post("/admin/inbox/drafts/999999/reject", auth=auth).status_code, 404)

            # sent conflict paths
            self.assertEqual(client.post(f"/admin/inbox/drafts/{sent_draft_id}/approve", auth=auth).status_code, 409)
            self.assertEqual(client.post(f"/admin/inbox/drafts/{sent_draft_id}/reject", auth=auth).status_code, 409)

            # already approved branch
            already = client.post(f"/admin/inbox/drafts/{approved_draft_id}/approve", auth=auth)
            self.assertEqual(already.status_code, 200)
            self.assertTrue(already.json()["already_approved"])

            # reject success branch
            reject_ok = client.post(f"/admin/inbox/drafts/{editable_draft_id}/reject", auth=auth)
            self.assertEqual(reject_ok.status_code, 200)
            self.assertEqual(reject_ok.json()["draft"]["status"], "rejected")

            # patch not found first branch
            self.assertEqual(
                client.patch(
                    "/admin/inbox/drafts/999998",
                    auth=auth,
                    json={"draft_text": "x", "model_name": "y"},
                ).status_code,
                404,
            )

            # patch update false branch
            with patch("sales_agent.sales_api.routers.admin_inbox.update_reply_draft_text", return_value=False):
                patch_resp = client.patch(
                    f"/admin/inbox/drafts/{editable_draft_id}",
                    auth=auth,
                    json={"draft_text": "new", "model_name": "z"},
                )
            self.assertEqual(patch_resp.status_code, 404)

    def test_admin_inbox_ui_post_actions_and_status_branches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin_inbox_ui_edge.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")
            origin = {"Origin": "http://testserver"}

            conn = db.get_connection(db_path)
            try:
                # user for direct UI post actions
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="ui-post-user")
                thread_id = f"tg:{user_id}"
                draft_id = db.create_reply_draft(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    draft_text="ui draft",
                    created_by="test",
                    model_name="x",
                )
                rejectable_ui_draft_id = db.create_reply_draft(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    draft_text="ui rejectable",
                    created_by="test",
                    model_name="x",
                )

                # user for sending-status branch in thread UI
                sending_user = db.get_or_create_user(conn, channel="telegram", external_id="ui-sending-user")
                sending_draft = db.create_reply_draft(
                    conn,
                    user_id=sending_user,
                    thread_id=f"tg:{sending_user}",
                    draft_text="sending draft",
                    created_by="test",
                    model_name="x",
                )
                db.update_reply_draft_status(conn, draft_id=sending_draft, status="sending", actor="test")

                # user for created-status branch
                created_user = db.get_or_create_user(conn, channel="telegram", external_id="ui-created-user")
                db.create_reply_draft(
                    conn,
                    user_id=created_user,
                    thread_id=f"tg:{created_user}",
                    draft_text="created draft",
                    created_by="test",
                    model_name="x",
                )

                # user for rejected-status branch
                rejected_user = db.get_or_create_user(conn, channel="telegram", external_id="ui-rejected-user")
                rej_draft = db.create_reply_draft(
                    conn,
                    user_id=rejected_user,
                    thread_id=f"tg:{rejected_user}",
                    draft_text="rejected draft",
                    created_by="test",
                    model_name="x",
                )
                db.update_reply_draft_status(conn, draft_id=rej_draft, status="rejected", actor="test")

                # user for followups-only branch + call insights branch
                followup_user = db.get_or_create_user(conn, channel="telegram", external_id="ui-followup-user")
                no_match_user = db.get_or_create_user(conn, channel="telegram", external_id="ui-no-match-user")
                followup_thread = f"tg:{followup_user}"
                db.create_followup_task(
                    conn,
                    user_id=followup_user,
                    thread_id=followup_thread,
                    priority="warm",
                    reason="manual followup",
                    status="pending",
                    assigned_to="sales",
                )
                db.create_followup_task(
                    conn,
                    user_id=followup_user,
                    thread_id="biz:bc-edge:777",
                    priority="hot",
                    reason="biz followup",
                    status="pending",
                    assigned_to="sales",
                )
                call_id = db.create_call_record(
                    conn,
                    user_id=followup_user,
                    thread_id=followup_thread,
                    source_type="upload",
                    source_ref="src",
                    file_path=None,
                    status="done",
                    created_by="test",
                )
                db.upsert_call_summary(
                    conn,
                    call_id=call_id,
                    summary_text="summary",
                    interests=["math", "physics"],
                    objections=["price"],
                    next_best_action="book consult",
                    warmth="hot",
                    confidence=0.8,
                    model_name="test",
                )
                empty_lookup_user = conn.execute(
                    """
                    INSERT INTO users (channel, external_id, created_at)
                    VALUES (?, ?, datetime('now'))
                    """,
                    ("telegram", ""),
                ).lastrowid
                conn.commit()
            finally:
                conn.close()

            # draft edit/approve/send/reject UI
            self.assertEqual(
                client.post(
                    f"/admin/ui/inbox/drafts/{draft_id}/edit",
                    auth=auth,
                    headers=origin,
                    data={"draft_text": "edited", "model_name": "m1"},
                    follow_redirects=False,
                ).status_code,
                303,
            )
            self.assertEqual(
                client.post(
                    f"/admin/ui/inbox/drafts/{draft_id}/approve",
                    auth=auth,
                    headers=origin,
                    data={},
                    follow_redirects=False,
                ).status_code,
                303,
            )
            self.assertEqual(
                client.post(
                    f"/admin/ui/inbox/drafts/{draft_id}/send",
                    auth=auth,
                    headers=origin,
                    data={"sent_message_id": "manual-1"},
                    follow_redirects=False,
                ).status_code,
                303,
            )
            self.assertEqual(
                client.post(
                    f"/admin/ui/inbox/drafts/{rejectable_ui_draft_id}/reject",
                    auth=auth,
                    headers=origin,
                    data={},
                    follow_redirects=False,
                ).status_code,
                303,
            )

            # not found branches for UI draft actions
            self.assertEqual(
                client.post(
                    "/admin/ui/inbox/drafts/999997/edit",
                    auth=auth,
                    headers=origin,
                    data={"draft_text": "x", "model_name": "m"},
                    follow_redirects=False,
                ).status_code,
                404,
            )
            self.assertEqual(
                client.post(
                    "/admin/ui/inbox/drafts/999997/approve",
                    auth=auth,
                    headers=origin,
                    data={},
                    follow_redirects=False,
                ).status_code,
                404,
            )
            self.assertEqual(
                client.post(
                    "/admin/ui/inbox/drafts/999997/reject",
                    auth=auth,
                    headers=origin,
                    data={},
                    follow_redirects=False,
                ).status_code,
                404,
            )

            self.assertEqual(client.get("/admin/ui/inbox/999997", auth=auth).status_code, 404)

            # outcome/followup/lead-score UI
            self.assertEqual(
                client.post(
                    f"/admin/ui/inbox/{user_id}/outcome",
                    auth=auth,
                    headers=origin,
                    data={"outcome": "won", "note": "note"},
                    follow_redirects=False,
                ).status_code,
                303,
            )
            self.assertEqual(
                client.post(
                    f"/admin/ui/inbox/{user_id}/followups",
                    auth=auth,
                    headers=origin,
                    data={"priority": "INVALID", "reason": "check", "assigned_to": "sales"},
                    follow_redirects=False,
                ).status_code,
                303,
            )
            self.assertEqual(
                client.post(
                    f"/admin/ui/inbox/{user_id}/lead-score",
                    auth=auth,
                    headers=origin,
                    data={"score": "77", "temperature": "INVALID", "confidence": "bad"},
                    follow_redirects=False,
                ).status_code,
                303,
            )

            # Cover workflow status branches and call-insights/crm blocks in UI thread page
            sending_ui = client.get(f"/admin/ui/inbox/{sending_user}", auth=auth)
            self.assertEqual(sending_ui.status_code, 200)
            self.assertIn("Отправляется", sending_ui.text)

            created_ui = client.get(f"/admin/ui/inbox/{created_user}", auth=auth)
            self.assertEqual(created_ui.status_code, 200)
            self.assertIn("Нужен approve", created_ui.text)

            rejected_ui = client.get(f"/admin/ui/inbox/{rejected_user}", auth=auth)
            self.assertEqual(rejected_ui.status_code, 200)
            self.assertIn("Отклонён", rejected_ui.text)

            followup_ui = client.get(f"/admin/ui/inbox/{followup_user}", auth=auth)
            self.assertEqual(followup_ui.status_code, 200)
            self.assertIn("Нужен ручной шаг", followup_ui.text)
            self.assertIn("Tallanto read-only mode отключен", followup_ui.text)
            self.assertIn("Interests:", followup_ui.text)

            followups_table_ui = client.get("/admin/ui/followups", auth=auth)
            self.assertEqual(followups_table_ui.status_code, 200)
            self.assertIn("/admin/ui/business-inbox/thread?thread_key=", followups_table_ui.text)

            app_crm_disabled = create_app(self._settings(db_path, enable_tallanto_enrichment=False))
            client_crm_disabled = build_test_client(app_crm_disabled)
            followup_ui_disabled = client_crm_disabled.get(f"/admin/ui/inbox/{followup_user}", auth=auth)
            self.assertEqual(followup_ui_disabled.status_code, 200)
            self.assertIn("CRM enrichment выключен", followup_ui_disabled.text)

            app_crm_not_configured = create_app(
                self._settings(db_path, tallanto_read_only=True, tallanto_api_url="", tallanto_api_token="")
            )
            client_crm_not_configured = build_test_client(app_crm_not_configured)
            followup_ui_not_configured = client_crm_not_configured.get(f"/admin/ui/inbox/{followup_user}", auth=auth)
            self.assertEqual(followup_ui_not_configured.status_code, 200)
            self.assertIn("Tallanto не настроен", followup_ui_not_configured.text)

            app_crm_found = create_app(
                self._settings(
                    db_path,
                    tallanto_read_only=True,
                    tallanto_api_url="https://crm.example/api",
                    tallanto_api_token="token",
                    tallanto_default_contact_module="contacts",
                )
            )
            client_crm_found = build_test_client(app_crm_found)
            with patch(
                "sales_agent.sales_api.main.TallantoReadOnlyClient.call",
                return_value={
                    "result": [
                        {
                            "tags": "vip,parents",
                            "interests": ["ege", "math"],
                            "updated_at": "2026-03-01T10:00:00Z",
                        }
                    ]
                },
            ):
                followup_ui_found = client_crm_found.get(f"/admin/ui/inbox/{followup_user}", auth=auth)
            self.assertEqual(followup_ui_found.status_code, 200)
            self.assertIn("<b>Match:</b> yes", followup_ui_found.text)

            app_crm_lookup_candidates_empty = create_app(
                self._settings(
                    db_path,
                    tallanto_read_only=True,
                    tallanto_api_url="https://crm.example/api",
                    tallanto_api_token="token",
                    tallanto_default_contact_module="contacts",
                )
            )
            client_crm_lookup_candidates_empty = build_test_client(app_crm_lookup_candidates_empty)
            followup_ui_lookup_candidates_empty = client_crm_lookup_candidates_empty.get(
                f"/admin/ui/inbox/{empty_lookup_user}",
                auth=auth,
            )
            self.assertEqual(followup_ui_lookup_candidates_empty.status_code, 200)
            self.assertIn("Недостаточно данных для поиска контакта", followup_ui_lookup_candidates_empty.text)

            app_crm_no_match = create_app(
                self._settings(
                    db_path,
                    tallanto_read_only=True,
                    tallanto_api_url="https://crm.example/api",
                    tallanto_api_token="token",
                    tallanto_default_contact_module="contacts",
                )
            )
            client_crm_no_match = build_test_client(app_crm_no_match)
            with patch("sales_agent.sales_api.main.TallantoReadOnlyClient.call", return_value={"result": []}):
                followup_ui_no_match = client_crm_no_match.get(f"/admin/ui/inbox/{no_match_user}", auth=auth)
            self.assertEqual(followup_ui_no_match.status_code, 200)
            self.assertIn("<b>Match:</b> no", followup_ui_no_match.text)
            self.assertIn("Контакт не найден в CRM", followup_ui_no_match.text)

    def test_admin_business_thread_ui_and_api_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin_business_not_found.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")
            self.assertEqual(
                client.get("/admin/business/inbox/thread", auth=auth, params={"thread_key": "biz:missing:1"}).status_code,
                404,
            )
            self.assertEqual(
                client.get(
                    "/admin/ui/business-inbox/thread",
                    auth=auth,
                    params={"thread_key": "biz:missing:1"},
                ).status_code,
                404,
            )


if __name__ == "__main__":
    unittest.main()
