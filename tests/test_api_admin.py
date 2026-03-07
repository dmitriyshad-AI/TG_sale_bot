import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core import db
    from sales_agent.sales_core.config import Settings
    from sales_agent.sales_core.telegram_business_sender import TelegramBusinessSendError
    from tests.test_client_compat import build_test_client

    HAS_ADMIN_DEPS = True
except ModuleNotFoundError:
    HAS_ADMIN_DEPS = False


@unittest.skipUnless(HAS_ADMIN_DEPS, "fastapi dependencies are not installed")
class ApiAdminTests(unittest.TestCase):
    def _settings(
        self,
        db_path: Path,
        admin_user: str = "admin",
        admin_pass: str = "secret",
        *,
        telegram_bot_token: str = "",
        enable_lead_radar: bool = False,
        lead_radar_scheduler_enabled: bool = True,
    ) -> Settings:
        return Settings(
            telegram_bot_token=telegram_bot_token,
            openai_api_key="",
            openai_model="gpt-4.1",
            tallanto_api_url="",
            tallanto_api_key="",
            brand_default="kmipt",
            database_path=db_path,
            catalog_path=Path("catalog/products.yaml"),
            knowledge_path=Path("knowledge"),
            vector_store_meta_path=Path("data/vector_store.json"),
            openai_vector_store_id="",
            admin_user=admin_user,
            admin_pass=admin_pass,
            enable_business_inbox=True,
            enable_call_copilot=True,
            enable_tallanto_enrichment=True,
            enable_director_agent=True,
            enable_lead_radar=enable_lead_radar,
            lead_radar_scheduler_enabled=lead_radar_scheduler_enabled,
            lead_radar_interval_seconds=86400,
            lead_radar_no_reply_hours=1,
            lead_radar_call_no_next_step_hours=1,
            lead_radar_stale_warm_days=1,
            lead_radar_max_items_per_run=100,
        )

    def test_admin_endpoints_require_auth(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)

            response = client.get("/admin/leads")
            self.assertEqual(response.status_code, 401)
            response_ui = client.get("/admin")
            self.assertEqual(response_ui.status_code, 401)

    def test_admin_returns_503_when_not_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path, admin_user="", admin_pass=""))
            client = build_test_client(app)

            response = client.get("/admin/leads", auth=("x", "y"))
            self.assertEqual(response.status_code, 503)

    def test_admin_leads_and_conversations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="user-1",
                    username="alice",
                    first_name="Alice",
                    last_name="Doe",
                )
                db.create_lead_record(
                    conn=conn,
                    user_id=user_id,
                    status="created",
                    tallanto_entry_id="lead-1",
                    contact={"phone": "+79990000001"},
                )
                db.log_message(conn, user_id=user_id, direction="inbound", text="hi", meta={"k": 1})
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")

            leads_response = client.get("/admin/leads", auth=auth)
            self.assertEqual(leads_response.status_code, 200)
            leads_items = leads_response.json()["items"]
            self.assertEqual(len(leads_items), 1)
            self.assertEqual(leads_items[0]["contact"]["phone"], "+79990000001")

            conv_response = client.get("/admin/conversations", auth=auth)
            self.assertEqual(conv_response.status_code, 200)
            conv_items = conv_response.json()["items"]
            self.assertEqual(len(conv_items), 1)
            self.assertEqual(conv_items[0]["messages_count"], 1)

            history_response = client.get(f"/admin/conversations/{conv_items[0]['user_id']}", auth=auth)
            self.assertEqual(history_response.status_code, 200)
            history = history_response.json()["messages"]
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["text"], "hi")

            dashboard_ui = client.get("/admin", auth=auth)
            self.assertEqual(dashboard_ui.status_code, 200)
            self.assertIn("Sales Agent Admin", dashboard_ui.text)

            leads_ui = client.get("/admin/ui/leads", auth=auth)
            self.assertEqual(leads_ui.status_code, 200)
            self.assertIn("Leads", leads_ui.text)
            self.assertIn("+79990000001", leads_ui.text)

            conv_ui = client.get("/admin/ui/conversations", auth=auth)
            self.assertEqual(conv_ui.status_code, 200)
            self.assertIn("Conversations", conv_ui.text)

            conv_detail_ui = client.get(f"/admin/ui/conversations/{conv_items[0]['user_id']}", auth=auth)
            self.assertEqual(conv_detail_ui.status_code, 200)
            self.assertIn("Conversation #", conv_detail_ui.text)
            self.assertIn("hi", conv_detail_ui.text)

    def test_admin_revenue_metrics_and_inbox_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="user-2",
                    username="boris",
                    first_name="Boris",
                    last_name="Ivanov",
                )
                db.log_message(conn, user_id=user_id, direction="inbound", text="Нужна стратегия ЕГЭ", meta={})
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")

            metrics_response = client.get("/admin/revenue-metrics", auth=auth)
            self.assertEqual(metrics_response.status_code, 200)
            metrics_payload = metrics_response.json()
            self.assertTrue(metrics_payload["ok"])
            self.assertIn("drafts_created_today", metrics_payload["metrics"])
            self.assertTrue(metrics_payload["feature_flags"]["enable_business_inbox"])

            inbox_response = client.get("/admin/inbox", auth=auth)
            self.assertEqual(inbox_response.status_code, 200)
            inbox_items = inbox_response.json()["items"]
            self.assertEqual(len(inbox_items), 1)
            self.assertEqual(inbox_items[0]["user_id"], user_id)

            create_draft_response = client.post(
                f"/admin/inbox/{user_id}/drafts",
                auth=auth,
                json={"draft_text": "Предлагаю консультацию", "model_name": "gpt-test"},
            )
            self.assertEqual(create_draft_response.status_code, 200)
            draft_id = int(create_draft_response.json()["draft"]["id"])

            patch_draft_response = client.patch(
                f"/admin/inbox/drafts/{draft_id}",
                auth=auth,
                json={"draft_text": "Предлагаю консультацию и план", "model_name": "gpt-test-2"},
            )
            self.assertEqual(patch_draft_response.status_code, 200)
            self.assertEqual(
                patch_draft_response.json()["draft"]["draft_text"],
                "Предлагаю консультацию и план",
            )

            send_without_approve_response = client.post(
                f"/admin/inbox/drafts/{draft_id}/send",
                auth=auth,
                json={},
            )
            self.assertEqual(send_without_approve_response.status_code, 409)

            approve_response = client.post(f"/admin/inbox/drafts/{draft_id}/approve", auth=auth)
            self.assertEqual(approve_response.status_code, 200)
            self.assertEqual(approve_response.json()["draft"]["status"], "approved")

            send_response = client.post(
                f"/admin/inbox/drafts/{draft_id}/send",
                auth=auth,
                json={"sent_message_id": "tg-msg-1"},
            )
            self.assertEqual(send_response.status_code, 200)
            self.assertEqual(send_response.json()["draft"]["status"], "sent")

            outcome_response = client.post(
                f"/admin/inbox/{user_id}/outcome",
                auth=auth,
                json={"outcome": "consultation_booked", "note": "Клиент просит звонок вечером"},
            )
            self.assertEqual(outcome_response.status_code, 200)
            self.assertEqual(outcome_response.json()["outcome"]["outcome"], "consultation_booked")

            followup_response = client.post(
                f"/admin/inbox/{user_id}/followups",
                auth=auth,
                json={"priority": "hot", "reason": "Контрольный созвон", "assigned_to": "sales-1"},
            )
            self.assertEqual(followup_response.status_code, 200)
            self.assertEqual(followup_response.json()["task"]["priority"], "hot")

            lead_score_response = client.post(
                f"/admin/inbox/{user_id}/lead-score",
                auth=auth,
                json={"score": 87.4, "temperature": "hot", "confidence": 0.76},
            )
            self.assertEqual(lead_score_response.status_code, 200)
            self.assertEqual(lead_score_response.json()["lead_score"]["temperature"], "hot")

            event_response = client.post(
                f"/admin/inbox/{user_id}/events",
                auth=auth,
                json={"action": "manual_action", "payload": {"note": "checked"}},
            )
            self.assertEqual(event_response.status_code, 200)
            self.assertGreater(int(event_response.json()["action_id"]), 0)

            detail_response = client.get(f"/admin/inbox/{user_id}", auth=auth)
            self.assertEqual(detail_response.status_code, 200)
            detail_payload = detail_response.json()
            self.assertEqual(detail_payload["outcome"]["outcome"], "consultation_booked")
            self.assertEqual(detail_payload["lead_score"]["temperature"], "hot")
            self.assertGreaterEqual(len(detail_payload["approval_actions"]), 1)

            inbox_ui = client.get("/admin/ui/inbox", auth=auth)
            self.assertEqual(inbox_ui.status_code, 200)
            self.assertIn("Inbox", inbox_ui.text)

            inbox_detail_ui = client.get(f"/admin/ui/inbox/{user_id}", auth=auth)
            self.assertEqual(inbox_detail_ui.status_code, 200)
            self.assertIn("Create Draft", inbox_detail_ui.text)
            self.assertIn("Approval Actions", inbox_detail_ui.text)

            metrics_ui = client.get("/admin/ui/revenue-metrics", auth=auth)
            self.assertEqual(metrics_ui.status_code, 200)
            self.assertIn("Revenue Metrics", metrics_ui.text)

    def test_admin_inbox_detail_includes_sanitized_crm_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            settings = self._settings(db_path)
            settings.tallanto_read_only = True
            settings.tallanto_api_url = "https://crm.example/api"
            settings.tallanto_api_token = "token"
            settings.tallanto_default_contact_module = "contacts"
            app = create_app(settings)

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="123456",
                    username="alice",
                    first_name="Alice",
                )
                db.log_message(conn, user_id=user_id, direction="inbound", text="Привет", meta={})
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            with patch(
                "sales_agent.sales_api.main.TallantoReadOnlyClient.call",
                return_value={
                    "result": [
                        {
                            "tags": "vip,parents",
                            "interests": ["ege", "math"],
                            "phone": "+79990000000",
                            "updated_at": "2026-03-01T10:00:00Z",
                        }
                    ]
                },
            ):
                response = client.get(f"/admin/inbox/{user_id}", auth=auth)
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            crm_context = payload.get("crm_context") or {}
            self.assertTrue(crm_context.get("enabled"))
            self.assertTrue(crm_context.get("found"))
            self.assertEqual(crm_context.get("tags"), ["vip", "parents"])
            self.assertEqual(crm_context.get("interests"), ["ege", "math"])
            self.assertNotIn("phone", crm_context)

    def test_admin_send_non_business_draft_requires_manual_sent_message_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin_manual_send.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="manual-send-user")
                thread_id = f"tg:{user_id}"
                draft_id = db.create_reply_draft(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    draft_text="Проверка ручной отправки",
                    model_name="gpt-test",
                    created_by="manager",
                )
                db.update_reply_draft_status(conn, draft_id=draft_id, status="approved", actor="manager")
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            response = client.post(
                f"/admin/inbox/drafts/{draft_id}/send",
                auth=auth,
                json={},
            )
            self.assertEqual(response.status_code, 409)
            self.assertIn("sent_message_id", response.json()["detail"])

            conn = db.get_connection(db_path)
            try:
                draft = db.get_reply_draft(conn, draft_id)
            finally:
                conn.close()
            self.assertIsNotNone(draft)
            self.assertEqual(draft["status"], "approved")
            self.assertEqual(draft["last_error"], "manual_confirmation_required")

            inbox_detail_ui = client.get(f"/admin/ui/inbox/{user_id}", auth=auth)
            self.assertEqual(inbox_detail_ui.status_code, 200)
            self.assertIn("Retry Send", inbox_detail_ui.text)

    def test_admin_send_returns_409_for_draft_in_sending_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin_send_conflict.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="send-conflict-user")
                thread_id = f"tg:{user_id}"
                draft_id = db.create_reply_draft(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    draft_text="Конфликт отправки",
                    model_name="gpt-test",
                    created_by="manager",
                )
                db.update_reply_draft_status(conn, draft_id=draft_id, status="approved", actor="manager")
                db.update_reply_draft_status(conn, draft_id=draft_id, status="sending", actor="manager")
            finally:
                conn.close()

            client = build_test_client(app)
            response = client.post(
                f"/admin/inbox/drafts/{draft_id}/send",
                auth=("admin", "secret"),
                json={"sent_message_id": "manual-1"},
            )
            self.assertEqual(response.status_code, 409)
            self.assertIn("already being sent", str(response.json()["detail"]))

    def test_admin_business_inbox_api_and_ui(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                business_user_id = db.get_or_create_user(
                    conn,
                    channel="telegram_business",
                    external_id="client-42",
                    username="client42",
                    first_name="Client",
                    last_name="Business",
                )
                db.upsert_business_connection(
                    conn,
                    business_connection_id="bc-test",
                    telegram_user_id=501,
                    user_chat_id=7001,
                    can_reply=True,
                    is_enabled=True,
                    connected_at="2026-03-06T12:00:00+00:00",
                    meta={"source": "test"},
                )
                thread_key = db.upsert_business_thread(
                    conn,
                    business_connection_id="bc-test",
                    chat_id=88001,
                    user_id=business_user_id,
                    direction="inbound",
                    occurred_at="2026-03-06T12:05:00+00:00",
                    meta={"topic": "ege"},
                )
                db.log_business_message(
                    conn,
                    business_connection_id="bc-test",
                    chat_id=88001,
                    telegram_message_id=101,
                    user_id=business_user_id,
                    direction="inbound",
                    text="Здравствуйте, нужна подготовка к ЕГЭ",
                    payload={"event_type": "business_message"},
                    created_at="2026-03-06T12:05:00+00:00",
                )
                draft_id = db.create_reply_draft(
                    conn,
                    user_id=business_user_id,
                    thread_id=thread_key,
                    draft_text="Черновик для business диалога",
                    model_name="business_placeholder_v1",
                    created_by="business_inbox:auto",
                )
                db.create_approval_action(
                    conn,
                    draft_id=draft_id,
                    user_id=business_user_id,
                    thread_id=thread_key,
                    action="draft_created",
                    actor="business_inbox:auto",
                    payload={"source": "test"},
                )
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")

            inbox_response = client.get("/admin/business/inbox", auth=auth)
            self.assertEqual(inbox_response.status_code, 200)
            items = inbox_response.json()["items"]
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["thread_key"], "biz:bc-test:88001")
            self.assertEqual(items[0]["messages_count"], 1)

            detail_response = client.get(
                "/admin/business/inbox/thread",
                auth=auth,
                params={"thread_key": "biz:bc-test:88001"},
            )
            self.assertEqual(detail_response.status_code, 200)
            detail = detail_response.json()
            self.assertEqual(detail["thread"]["thread_key"], "biz:bc-test:88001")
            self.assertEqual(len(detail["messages"]), 1)
            self.assertEqual(detail["messages"][0]["text"], "Здравствуйте, нужна подготовка к ЕГЭ")
            self.assertEqual(len(detail["drafts"]), 1)
            self.assertGreaterEqual(len(detail["approval_actions"]), 1)

            list_ui = client.get("/admin/ui/business-inbox", auth=auth)
            self.assertEqual(list_ui.status_code, 200)
            self.assertIn("Business Inbox", list_ui.text)
            self.assertIn("biz:bc-test:88001", list_ui.text)

            detail_ui = client.get(
                "/admin/ui/business-inbox/thread",
                auth=auth,
                params={"thread_key": "biz:bc-test:88001"},
            )
            self.assertEqual(detail_ui.status_code, 200)
            self.assertIn("Business Thread", detail_ui.text)
            self.assertIn("нужна подготовка к ЕГЭ", detail_ui.text)

    def test_admin_business_draft_send_dispatches_via_business_connection(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path, telegram_bot_token="token-123"))
            conn = db.get_connection(db_path)
            try:
                business_user_id = db.get_or_create_user(
                    conn,
                    channel="telegram_business",
                    external_id="client-send-1",
                    username="sendclient",
                    first_name="Send",
                    last_name="Client",
                )
                db.upsert_business_connection(
                    conn,
                    business_connection_id="bc-send",
                    telegram_user_id=801,
                    user_chat_id=9001,
                    can_reply=True,
                    is_enabled=True,
                    connected_at="2026-03-06T12:00:00+00:00",
                    meta={},
                )
                thread_key = db.upsert_business_thread(
                    conn,
                    business_connection_id="bc-send",
                    chat_id=99901,
                    user_id=business_user_id,
                    direction="inbound",
                    occurred_at="2026-03-06T12:05:00+00:00",
                    meta={"topic": "ege"},
                )
                draft_id = db.create_reply_draft(
                    conn,
                    user_id=business_user_id,
                    thread_id=thread_key,
                    draft_text="Готовы обсудить стратегию поступления.",
                    model_name="business_placeholder_v1",
                    created_by="manager",
                )
                db.update_reply_draft_status(conn, draft_id=draft_id, status="approved", actor="manager")
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            with patch(
                "sales_agent.sales_api.main.send_business_message",
                return_value={"message_id": 777, "date": 1700000000},
            ) as mocked_send:
                send_response = client.post(
                    f"/admin/inbox/drafts/{draft_id}/send",
                    auth=auth,
                    json={},
                )
            self.assertEqual(send_response.status_code, 200)
            send_payload = send_response.json()
            self.assertEqual(send_payload["draft"]["status"], "sent")
            self.assertEqual(send_payload["draft"]["sent_message_id"], "777")
            self.assertEqual(send_payload["delivery"]["transport"], "telegram_business")
            self.assertEqual(send_payload["delivery"]["message_ids"], [777])
            self.assertEqual(mocked_send.call_count, 1)

            repeat_response = client.post(
                f"/admin/inbox/drafts/{draft_id}/send",
                auth=auth,
                json={},
            )
            self.assertEqual(repeat_response.status_code, 200)
            self.assertTrue(repeat_response.json()["already_sent"])

            conn = db.get_connection(db_path)
            try:
                messages = db.list_business_messages(conn, thread_key="biz:bc-send:99901", limit=20)
            finally:
                conn.close()
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0]["direction"], "outbound")
            self.assertEqual(messages[0]["telegram_message_id"], 777)

    def test_admin_business_draft_send_failure_keeps_draft_approved(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path, telegram_bot_token="token-123"))
            conn = db.get_connection(db_path)
            try:
                business_user_id = db.get_or_create_user(
                    conn,
                    channel="telegram_business",
                    external_id="client-send-fail",
                )
                db.upsert_business_connection(
                    conn,
                    business_connection_id="bc-fail",
                    can_reply=True,
                    is_enabled=True,
                )
                thread_key = db.upsert_business_thread(
                    conn,
                    business_connection_id="bc-fail",
                    chat_id=99111,
                    user_id=business_user_id,
                    direction="inbound",
                )
                draft_id = db.create_reply_draft(
                    conn,
                    user_id=business_user_id,
                    thread_id=thread_key,
                    draft_text="Черновик с ошибкой отправки",
                    created_by="manager",
                )
                db.update_reply_draft_status(conn, draft_id=draft_id, status="approved", actor="manager")
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            with patch(
                "sales_agent.sales_api.main.send_business_message",
                side_effect=TelegramBusinessSendError("mock send failure"),
            ):
                send_response = client.post(
                    f"/admin/inbox/drafts/{draft_id}/send",
                    auth=auth,
                    json={},
                )
            self.assertEqual(send_response.status_code, 502)

            conn = db.get_connection(db_path)
            try:
                draft = db.get_reply_draft(conn, draft_id)
                actions = db.list_approval_actions_for_thread(conn, thread_id="biz:bc-fail:99111", limit=20)
            finally:
                conn.close()
            self.assertIsNotNone(draft)
            self.assertEqual(draft["status"], "approved")
            self.assertIn("mock send failure", str(draft.get("last_error") or ""))
            self.assertTrue(any(item.get("action") == "draft_send_failed" for item in actions))

    def test_admin_business_draft_partial_delivery_requires_manual_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path, telegram_bot_token="token-123"))
            conn = db.get_connection(db_path)
            try:
                business_user_id = db.get_or_create_user(
                    conn,
                    channel="telegram_business",
                    external_id="client-send-partial",
                )
                db.upsert_business_connection(
                    conn,
                    business_connection_id="bc-partial",
                    can_reply=True,
                    is_enabled=True,
                )
                thread_key = db.upsert_business_thread(
                    conn,
                    business_connection_id="bc-partial",
                    chat_id=99222,
                    user_id=business_user_id,
                    direction="inbound",
                )
                draft_id = db.create_reply_draft(
                    conn,
                    user_id=business_user_id,
                    thread_id=thread_key,
                    draft_text=("x " * 2505).strip(),
                    created_by="manager",
                )
                db.update_reply_draft_status(conn, draft_id=draft_id, status="approved", actor="manager")
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            with patch(
                "sales_agent.sales_api.main.send_business_message",
                side_effect=[{"message_id": 911, "date": 1700000000}, TelegramBusinessSendError("chunk-2 failed")],
            ):
                send_response = client.post(
                    f"/admin/inbox/drafts/{draft_id}/send",
                    auth=auth,
                    json={},
                )
            self.assertEqual(send_response.status_code, 409)
            self.assertIn("Partial Telegram Business delivery", str(send_response.json()["detail"]))

            conn = db.get_connection(db_path)
            try:
                failed_draft = db.get_reply_draft(conn, draft_id)
                actions_after_fail = db.list_approval_actions_for_thread(conn, thread_id=thread_key, limit=30)
            finally:
                conn.close()
            self.assertIsNotNone(failed_draft)
            self.assertEqual(failed_draft["status"], "approved")
            self.assertIn("partial_delivery|sent_message_ids=911|error=chunk-2 failed", str(failed_draft["last_error"]))
            self.assertTrue(any(item.get("action") == "draft_send_partial" for item in actions_after_fail))

            with patch("sales_agent.sales_api.main.send_business_message") as mocked_send:
                recover_response = client.post(
                    f"/admin/inbox/drafts/{draft_id}/send",
                    auth=auth,
                    json={"sent_message_id": "manual-911"},
                )
            self.assertEqual(recover_response.status_code, 200)
            recover_payload = recover_response.json()
            self.assertEqual(recover_payload["draft"]["status"], "sent")
            self.assertEqual(recover_payload["draft"]["sent_message_id"], "manual-911")
            self.assertEqual(recover_payload["delivery"]["transport"], "manual_partial_recovery")
            self.assertEqual(mocked_send.call_count, 0)

            conn = db.get_connection(db_path)
            try:
                actions_after_recover = db.list_approval_actions_for_thread(conn, thread_id=thread_key, limit=50)
            finally:
                conn.close()
            self.assertTrue(any(item.get("action") == "draft_partial_recovery_confirmed" for item in actions_after_recover))
            self.assertTrue(any(item.get("action") == "draft_sent" for item in actions_after_recover))

    def test_admin_followups_and_lead_radar_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(
                self._settings(
                    db_path,
                    enable_lead_radar=True,
                    lead_radar_scheduler_enabled=False,
                )
            )
            client = build_test_client(app)
            auth = ("admin", "secret")

            conn = db.get_connection(db_path)
            try:
                no_reply_user = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="radar-u1",
                    username="u1",
                    first_name="NoReply",
                )
                db.log_message(conn, user_id=no_reply_user, direction="inbound", text="Подберите курс", meta={})
                conn.execute(
                    "UPDATE messages SET created_at = datetime('now', '-5 hours') WHERE user_id = ?",
                    (no_reply_user,),
                )

                call_user = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="radar-u2",
                    username="u2",
                    first_name="CallUser",
                )
                call_thread_id = f"tg:{call_user}"
                db.create_approval_action(
                    conn,
                    draft_id=None,
                    user_id=call_user,
                    thread_id=call_thread_id,
                    action="manual_action",
                    actor="manager",
                    payload={"source": "call_summary", "note": "call completed"},
                )
                conn.execute(
                    "UPDATE approval_actions SET created_at = datetime('now', '-4 hours') WHERE thread_id = ?",
                    (call_thread_id,),
                )

                warm_user = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="radar-u3",
                    username="u3",
                    first_name="WarmUser",
                )
                warm_thread_id = f"tg:{warm_user}"
                db.create_lead_score(
                    conn,
                    user_id=warm_user,
                    thread_id=warm_thread_id,
                    score=62.0,
                    temperature="warm",
                    confidence=0.7,
                    factors={"reason": "needs followup"},
                )
                conn.execute(
                    "UPDATE lead_scores SET created_at = datetime('now', '-3 days') WHERE thread_id = ?",
                    (warm_thread_id,),
                )
                conn.commit()
            finally:
                conn.close()

            run_response = client.post("/admin/followups/run", auth=auth, json={"dry_run": False})
            self.assertEqual(run_response.status_code, 200)
            run_payload = run_response.json()
            self.assertTrue(run_payload["ok"])
            self.assertEqual(run_payload["created_followups"], 3)
            self.assertEqual(run_payload["created_drafts"], 3)
            self.assertEqual(run_payload["rules"]["radar:no_reply"]["created"], 1)
            self.assertEqual(run_payload["rules"]["radar:call_no_next_step"]["created"], 1)
            self.assertEqual(run_payload["rules"]["radar:stale_warm"]["created"], 1)

            run_again = client.post("/admin/followups/run", auth=auth, json={"dry_run": False})
            self.assertEqual(run_again.status_code, 200)
            self.assertEqual(run_again.json()["created_followups"], 0)

            followups_all = client.get("/admin/followups", auth=auth)
            self.assertEqual(followups_all.status_code, 200)
            self.assertEqual(len(followups_all.json()["items"]), 3)

            followups_hot = client.get("/admin/followups?priority=hot", auth=auth)
            self.assertEqual(followups_hot.status_code, 200)
            self.assertEqual(len(followups_hot.json()["items"]), 2)

            followups_radar = client.get("/admin/followups?radar_only=true", auth=auth)
            self.assertEqual(followups_radar.status_code, 200)
            reasons = [str(item.get("reason") or "") for item in followups_radar.json()["items"]]
            self.assertEqual(len(reasons), 3)
            self.assertTrue(all(reason.startswith("radar:") for reason in reasons))

            followups_ui = client.get("/admin/ui/followups?radar_only=true", auth=auth)
            self.assertEqual(followups_ui.status_code, 200)
            self.assertIn("Followups", followups_ui.text)
            self.assertIn("radar:no_reply", followups_ui.text)

    def test_admin_followups_run_returns_503_when_lead_radar_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path, enable_lead_radar=False))
            client = build_test_client(app)
            auth = ("admin", "secret")

            response = client.post("/admin/followups/run", auth=auth, json={})
            self.assertEqual(response.status_code, 503)

    def test_admin_followups_run_covers_business_no_reply_threads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(
                self._settings(
                    db_path,
                    enable_lead_radar=True,
                    lead_radar_scheduler_enabled=False,
                )
            )
            client = build_test_client(app)
            auth = ("admin", "secret")

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(
                    conn,
                    channel="telegram_business",
                    external_id="biz-radar-u1",
                )
                db.upsert_business_connection(
                    conn,
                    business_connection_id="bc-radar",
                    can_reply=True,
                    is_enabled=True,
                )
                db.log_business_message(
                    conn,
                    business_connection_id="bc-radar",
                    chat_id=88077,
                    telegram_message_id=900,
                    user_id=user_id,
                    direction="inbound",
                    text="Хочу консультацию по ЕГЭ",
                    payload={},
                    created_at="2026-03-06T12:05:00+00:00",
                )
                conn.execute(
                    "UPDATE business_messages SET created_at = datetime('now', '-5 hours') WHERE thread_key = ?",
                    ("biz:bc-radar:88077",),
                )
                conn.execute(
                    "UPDATE business_threads SET last_message_at = datetime('now', '-5 hours') WHERE thread_key = ?",
                    ("biz:bc-radar:88077",),
                )
                conn.commit()
            finally:
                conn.close()

            run_response = client.post("/admin/followups/run", auth=auth, json={"dry_run": False})
            self.assertEqual(run_response.status_code, 200)
            payload = run_response.json()
            self.assertEqual(payload["rules"]["radar:no_reply"]["created"], 1)

            followups = client.get("/admin/followups?radar_only=true", auth=auth)
            self.assertEqual(followups.status_code, 200)
            items = followups.json()["items"]
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["thread_id"], "biz:bc-radar:88077")

    def test_lead_radar_respects_cooldown_and_daily_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin_radar_limits.db"
            settings = self._settings(
                db_path,
                enable_lead_radar=True,
                lead_radar_scheduler_enabled=False,
            )
            settings.lead_radar_no_reply_hours = 1
            settings.lead_radar_thread_cooldown_hours = 24
            settings.lead_radar_daily_cap_per_thread = 2
            app = create_app(settings)
            client = build_test_client(app)
            auth = ("admin", "secret")

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="radar-limit-u1")
                thread_id = f"tg:{user_id}"
                db.log_message(conn, user_id=user_id, direction="inbound", text="Подскажите программу", meta={})
                conn.execute(
                    "UPDATE messages SET created_at = datetime('now', '-3 hours') WHERE user_id = ?",
                    (user_id,),
                )
                db.create_followup_task(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    priority="warm",
                    reason="radar:stale_warm: уже был контакт",
                    status="done",
                    assigned_to="sales-1",
                )
                conn.commit()
            finally:
                conn.close()

            run_response = client.post("/admin/followups/run", auth=auth, json={"dry_run": False})
            self.assertEqual(run_response.status_code, 200)
            payload = run_response.json()
            self.assertEqual(payload["created_followups"], 0)
            self.assertGreaterEqual(payload["rules"]["radar:no_reply"]["skipped_cooldown"], 1)

            # Disable cooldown to verify daily cap guard branch.
            settings_no_cooldown = self._settings(
                db_path,
                enable_lead_radar=True,
                lead_radar_scheduler_enabled=False,
            )
            settings_no_cooldown.lead_radar_no_reply_hours = 1
            settings_no_cooldown.lead_radar_thread_cooldown_hours = 0
            settings_no_cooldown.lead_radar_daily_cap_per_thread = 1
            app_daily_cap = create_app(settings_no_cooldown)
            client_daily_cap = build_test_client(app_daily_cap)

            run_daily_cap = client_daily_cap.post("/admin/followups/run", auth=auth, json={"dry_run": False})
            self.assertEqual(run_daily_cap.status_code, 200)
            daily_payload = run_daily_cap.json()
            self.assertEqual(daily_payload["created_followups"], 0)
            self.assertGreaterEqual(daily_payload["rules"]["radar:no_reply"]["skipped_daily_cap"], 1)

    def test_admin_filters_for_inbox_and_followups(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin_filters.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="filter-u1",
                    username="filter_user",
                    first_name="Filter",
                    last_name="Case",
                )
                thread_id = f"tg:{user_id}"
                db.log_message(conn, user_id=user_id, direction="inbound", text="Нужен созвон", meta={})
                draft_id = db.create_reply_draft(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    draft_text="Черновик",
                    model_name="gpt-test",
                    created_by="admin",
                )
                db.update_reply_draft_status(conn, draft_id=draft_id, status="approved", actor="admin")
                db.create_followup_task(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    priority="hot",
                    reason="radar:no_reply: followup для поиска",
                    status="pending",
                    assigned_to="sales-1",
                )
            finally:
                conn.close()

            inbox_response = client.get(
                "/admin/inbox",
                auth=auth,
                params={"status": "ready_to_send", "search": "filter_user"},
            )
            self.assertEqual(inbox_response.status_code, 200)
            inbox_items = inbox_response.json()["items"]
            self.assertEqual(len(inbox_items), 1)
            self.assertEqual(inbox_items[0]["workflow_status"], "ready_to_send")

            followups_response = client.get(
                "/admin/followups",
                auth=auth,
                params={"status": "pending", "priority": "hot", "radar_only": "true", "search": "поиска"},
            )
            self.assertEqual(followups_response.status_code, 200)
            followups = followups_response.json()["items"]
            self.assertEqual(len(followups), 1)
            self.assertIn("radar:no_reply", followups[0]["reason"])

            inbox_ui = client.get(
                "/admin/ui/inbox",
                auth=auth,
                params={"status": "ready_to_send", "search": "filter_user"},
            )
            self.assertEqual(inbox_ui.status_code, 200)
            self.assertIn("Применить фильтры", inbox_ui.text)

            followups_ui = client.get(
                "/admin/ui/followups",
                auth=auth,
                params={"priority": "hot", "radar_only": "true", "search": "поиска"},
            )
            self.assertEqual(followups_ui.status_code, 200)
            self.assertIn("Применить фильтры", followups_ui.text)

    def test_admin_ui_post_requires_origin_when_csrf_enabled_for_inbox_and_calls(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin_csrf.db"
            settings = self._settings(db_path)
            settings.admin_ui_csrf_enabled = True
            app = create_app(settings)
            client = build_test_client(app)
            auth = ("admin", "secret")
            origin_headers = {"Origin": "http://testserver"}

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="csrf-u1")
                audio_file = Path(tmpdir) / "expired_csrf_audio.raw"
                audio_file.write_bytes(b"csrf-audio")
                call_id = db.create_call_record(
                    conn,
                    user_id=user_id,
                    thread_id=f"tg:{user_id}",
                    source_type="upload",
                    source_ref="csrf-test",
                    file_path=str(audio_file),
                    status="done",
                    created_by="test",
                )
                conn.execute(
                    "UPDATE call_records SET created_at = datetime('now', '-72 hours'), updated_at = datetime('now', '-72 hours') WHERE id = ?",
                    (call_id,),
                )
                conn.commit()
            finally:
                conn.close()

            missing_origin = client.post(
                f"/admin/ui/inbox/{user_id}/drafts",
                auth=auth,
                data={"draft_text": "CSRF draft", "model_name": "gpt-test"},
                follow_redirects=False,
            )
            self.assertEqual(missing_origin.status_code, 403)

            with_origin = client.post(
                f"/admin/ui/inbox/{user_id}/drafts",
                auth=auth,
                headers=origin_headers,
                data={"draft_text": "CSRF draft", "model_name": "gpt-test"},
                follow_redirects=False,
            )
            self.assertEqual(with_origin.status_code, 303)
            self.assertEqual(with_origin.headers.get("location"), f"/admin/ui/inbox/{user_id}")

            calls_cleanup_missing_origin = client.post(
                "/admin/ui/calls/cleanup",
                auth=auth,
                follow_redirects=False,
            )
            self.assertEqual(calls_cleanup_missing_origin.status_code, 403)

            calls_cleanup_with_origin = client.post(
                "/admin/ui/calls/cleanup",
                auth=auth,
                headers=origin_headers,
                follow_redirects=False,
            )
            self.assertEqual(calls_cleanup_with_origin.status_code, 303)
            self.assertEqual(calls_cleanup_with_origin.headers.get("location"), "/admin/ui/calls")

    def test_admin_calls_upload_and_inbox_enrichment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="call-test-user",
                    username="calluser",
                    first_name="Call",
                    last_name="User",
                )
                db.log_message(
                    conn,
                    user_id=user_id,
                    direction="inbound",
                    text="Нужна консультация по ЕГЭ",
                    meta={},
                )
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            transcript = (
                "Здравствуйте. Хотим записаться на курс ЕГЭ по математике. "
                "Интересует стоимость и формат занятий."
            )
            upload_response = client.post(
                "/admin/calls/upload",
                auth=auth,
                data={"user_id": str(user_id)},
                files={"audio_file": ("call.txt", transcript.encode("utf-8"), "text/plain")},
            )
            self.assertEqual(upload_response.status_code, 200)
            upload_payload = upload_response.json()
            self.assertTrue(upload_payload["ok"])
            item = upload_payload["item"]
            self.assertEqual(item["status"], "done")
            self.assertIn(item["warmth"], {"hot", "warm", "cold"})
            self.assertIn("ЕГЭ", item["transcript_text"])

            call_id = int(item["id"])
            list_response = client.get("/admin/calls", auth=auth)
            self.assertEqual(list_response.status_code, 200)
            self.assertEqual(len(list_response.json()["items"]), 1)
            self.assertEqual(int(list_response.json()["items"][0]["id"]), call_id)

            detail_response = client.get(f"/admin/calls/{call_id}", auth=auth)
            self.assertEqual(detail_response.status_code, 200)
            detail_item = detail_response.json()["item"]
            self.assertEqual(detail_item["id"], call_id)
            self.assertIn("summary_text", detail_item)
            self.assertIn("next_best_action", detail_item)

            inbox_detail_response = client.get(f"/admin/inbox/{user_id}", auth=auth)
            self.assertEqual(inbox_detail_response.status_code, 200)
            inbox_detail = inbox_detail_response.json()
            self.assertIsNotNone(inbox_detail.get("latest_call_insights"))
            self.assertGreaterEqual(len(inbox_detail.get("followups") or []), 1)
            self.assertIsNotNone(inbox_detail.get("lead_score"))

            calls_ui = client.get("/admin/ui/calls", auth=auth)
            self.assertEqual(calls_ui.status_code, 200)
            self.assertIn("Calls", calls_ui.text)
            self.assertIn("Загрузить и обработать звонок", calls_ui.text)

            call_detail_ui = client.get(f"/admin/ui/calls/{call_id}", auth=auth)
            self.assertEqual(call_detail_ui.status_code, 200)
            self.assertIn("Call Detail", call_detail_ui.text)
            self.assertIn("Transcript", call_detail_ui.text)

    def test_admin_calls_upload_uses_existing_business_thread_user(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(
                    conn,
                    channel="telegram_business",
                    external_id="biz-call-user",
                    username="bizcall",
                )
                db.upsert_business_connection(
                    conn,
                    business_connection_id="bc-call",
                    can_reply=True,
                    is_enabled=True,
                )
                db.upsert_business_thread(
                    conn,
                    business_connection_id="bc-call",
                    chat_id=77101,
                    user_id=user_id,
                    direction="inbound",
                )
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            upload_response = client.post(
                "/admin/calls/upload",
                auth=auth,
                data={
                    "thread_id": "biz:bc-call:77101",
                    "transcript_hint": "Клиент интересуется олимпиадной подготовкой и консультацией.",
                },
            )
            self.assertEqual(upload_response.status_code, 200)
            item = upload_response.json()["item"]
            self.assertEqual(int(item["user_id"]), user_id)
            self.assertEqual(item["thread_id"], "biz:bc-call:77101")

            followups = client.get("/admin/followups", auth=auth)
            self.assertEqual(followups.status_code, 200)
            self.assertTrue(any(task.get("thread_id") == "biz:bc-call:77101" for task in followups.json()["items"]))

    def test_admin_calls_ui_upload_redirects_to_call_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="call-u2")
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            response = client.post(
                "/admin/ui/calls/upload",
                auth=auth,
                data={
                    "user_id": str(user_id),
                    "recording_url": "https://example.com/rec/1",
                    "transcript_hint": "Клиенту нужен план поступления в МФТИ.",
                },
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)
            location = response.headers.get("location", "")
            self.assertTrue(location.startswith("/admin/ui/calls/"))

    def test_admin_calls_cleanup_endpoints_remove_old_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            audio_file = Path(tmpdir) / "expired_audio.raw"
            audio_file.write_bytes(b"old-audio")

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="cleanup-u1")
                call_id = db.create_call_record(
                    conn,
                    user_id=user_id,
                    thread_id=f"tg:{user_id}",
                    source_type="upload",
                    source_ref="cleanup-test",
                    file_path=str(audio_file),
                    status="done",
                    created_by="test",
                )
                conn.execute(
                    "UPDATE call_records SET created_at = datetime('now', '-72 hours'), updated_at = datetime('now', '-72 hours') WHERE id = ?",
                    (call_id,),
                )
                conn.commit()
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")

            cleanup_response = client.post("/admin/calls/cleanup", auth=auth)
            self.assertEqual(cleanup_response.status_code, 200)
            cleanup_payload = cleanup_response.json()
            self.assertTrue(cleanup_payload["ok"])
            self.assertEqual(cleanup_payload["cleaned"], 1)
            self.assertFalse(audio_file.exists())

            conn_verify = db.get_connection(db_path)
            try:
                item = db.get_call_record(conn_verify, call_id=call_id)
            finally:
                conn_verify.close()
            self.assertIsNotNone(item)
            assert item is not None
            self.assertIsNone(item["file_path"])

            ui_cleanup_response = client.post("/admin/ui/calls/cleanup", auth=auth, follow_redirects=False)
            self.assertEqual(ui_cleanup_response.status_code, 303)
            self.assertEqual(ui_cleanup_response.headers.get("location"), "/admin/ui/calls")

    def test_admin_calls_endpoints_return_503_when_feature_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            settings = self._settings(db_path)
            settings.enable_call_copilot = False
            app = create_app(settings)
            client = build_test_client(app)
            auth = ("admin", "secret")

            calls_response = client.get("/admin/calls", auth=auth)
            self.assertEqual(calls_response.status_code, 503)

            upload_response = client.post(
                "/admin/calls/upload",
                auth=auth,
                data={"recording_url": "https://example.com/x"},
            )
            self.assertEqual(upload_response.status_code, 503)

            cleanup_response = client.post("/admin/calls/cleanup", auth=auth)
            self.assertEqual(cleanup_response.status_code, 503)

            retry_failed_response = client.post("/admin/calls/retry-failed", auth=auth)
            self.assertEqual(retry_failed_response.status_code, 503)

            ui_cleanup_response = client.post("/admin/ui/calls/cleanup", auth=auth)
            self.assertEqual(ui_cleanup_response.status_code, 503)

            ui_retry_failed_response = client.post("/admin/ui/calls/retry-failed", auth=auth, data={"limit": 10})
            self.assertEqual(ui_retry_failed_response.status_code, 503)

    def test_admin_copilot_import_returns_summary_and_draft(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")

            payload = (
                "12/02/2026, 10:00 - Клиент: 10 класс, ЕГЭ по математике\n"
                "12/02/2026, 10:05 - Менеджер: Добрый день\n"
            )
            response = client.post(
                "/admin/copilot/import",
                auth=auth,
                files={"file": ("dialog.txt", payload.encode("utf-8"), "text/plain")},
            )
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("summary", data)
            self.assertIn("draft_reply", data)
            self.assertFalse(data["auto_send"])

            form_response = client.get("/admin/ui/copilot", auth=auth)
            self.assertEqual(form_response.status_code, 200)
            self.assertIn("Copilot Import", form_response.text)

            ui_response = client.post(
                "/admin/ui/copilot/import",
                auth=auth,
                files={"file": ("dialog.txt", payload.encode("utf-8"), "text/plain")},
            )
            self.assertEqual(ui_response.status_code, 200)
            self.assertIn("Copilot Result", ui_response.text)
            self.assertIn("Summary", ui_response.text)

    def test_admin_copilot_import_rejects_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")

            response = client.post(
                "/admin/copilot/import",
                auth=auth,
                files={"file": ("dialog.json", b"{not-valid-json", "application/json")},
            )
            self.assertEqual(response.status_code, 400)
            self.assertIn("Invalid Telegram JSON", response.json()["detail"])

    def test_admin_copilot_import_rejects_empty_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")

            response = client.post(
                "/admin/copilot/import",
                auth=auth,
                files={"file": ("dialog.txt", b"", "text/plain")},
            )
            self.assertEqual(response.status_code, 400)
            self.assertIn("empty", response.json()["detail"].lower())

    @patch("sales_agent.sales_api.main.build_crm_client")
    def test_admin_copilot_import_with_create_task(self, mock_build_crm_client) -> None:
        class _MockCRMClient:
            def create_copilot_task(self, summary, draft_reply, contact=None):
                return type(
                    "Result",
                    (),
                    {"success": True, "entry_id": "task-1", "error": None},
                )()

        mock_client = _MockCRMClient()
        mock_build_crm_client.return_value = mock_client
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")

            payload = (
                "12/02/2026, 10:00 - Клиент: 10 класс, ЕГЭ по математике\n"
                "12/02/2026, 10:05 - Менеджер: Добрый день\n"
            )
            response = client.post(
                "/admin/copilot/import?create_task=true",
                auth=auth,
                files={"file": ("dialog.txt", payload.encode("utf-8"), "text/plain")},
            )
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("task", data)
            self.assertTrue(data["task"]["success"])
            self.assertEqual(data["task"]["entry_id"], "task-1")


if __name__ == "__main__":
    unittest.main()
