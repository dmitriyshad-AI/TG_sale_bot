import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from sales_agent.sales_core import db


class DatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "test_sales_agent.db"
        db.init_db(self.db_path)
        self.conn = db.get_connection(self.db_path)

    def tearDown(self) -> None:
        self.conn.close()
        self.tempdir.cleanup()

    def test_init_db_creates_required_tables(self) -> None:
        cursor = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = {row["name"] for row in cursor.fetchall()}
        self.assertTrue(
            {
                "users",
                "sessions",
                "messages",
                "leads",
                "conversation_contexts",
                "crm_cache",
                "webhook_updates",
                "conversation_outcomes",
                "reply_drafts",
                "approval_actions",
                "followup_tasks",
                "lead_scores",
                "business_connections",
                "business_threads",
                "business_messages",
                "call_records",
                "call_transcripts",
                "call_summaries",
                "mango_events",
            }.issubset(table_names)
        )

    def test_get_or_create_user_is_idempotent(self) -> None:
        first = db.get_or_create_user(
            conn=self.conn,
            channel="telegram",
            external_id="42",
            username="alice",
            first_name="Alice",
            last_name="Doe",
        )
        second = db.get_or_create_user(
            conn=self.conn,
            channel="telegram",
            external_id="42",
            username="alice",
            first_name="Alice",
            last_name="Doe",
        )
        self.assertEqual(first, second)

        cursor = self.conn.execute("SELECT COUNT(*) AS cnt FROM users")
        self.assertEqual(cursor.fetchone()["cnt"], 1)

    def test_log_message_persists_meta_json(self) -> None:
        user_id = db.get_or_create_user(self.conn, "telegram", "99")
        db.log_message(
            conn=self.conn,
            user_id=user_id,
            direction="inbound",
            text="Привет",
            meta={"source": "test", "message_id": 101},
        )

        cursor = self.conn.execute(
            "SELECT direction, text, meta_json FROM messages WHERE user_id = ?",
            (user_id,),
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["direction"], "inbound")
        self.assertEqual(row["text"], "Привет")
        self.assertEqual(json.loads(row["meta_json"]), {"source": "test", "message_id": 101})

    def test_upsert_session_state_insert_and_update(self) -> None:
        user_id = db.get_or_create_user(self.conn, "telegram", "77")

        db.upsert_session_state(
            conn=self.conn,
            user_id=user_id,
            state={"step": "ask_grade"},
            meta={"source": "site"},
        )
        db.upsert_session_state(
            conn=self.conn,
            user_id=user_id,
            state={"step": "ask_goal", "grade": 8},
            meta={"source": "site", "utm": "cpc"},
        )

        cursor = self.conn.execute(
            "SELECT state_json, meta_json FROM sessions WHERE user_id = ?",
            (user_id,),
        )
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 1)

        row = rows[0]
        self.assertEqual(json.loads(row["state_json"]), {"step": "ask_goal", "grade": 8})
        self.assertEqual(json.loads(row["meta_json"]), {"source": "site", "utm": "cpc"})

    def test_upsert_session_state_keeps_meta_when_not_passed(self) -> None:
        user_id = db.get_or_create_user(self.conn, "telegram", "78")
        db.upsert_session_state(
            conn=self.conn,
            user_id=user_id,
            state={"step": "ask_grade"},
            meta={"source": "site"},
        )
        db.upsert_session_state(
            conn=self.conn,
            user_id=user_id,
            state={"step": "ask_goal"},
            meta=None,
        )

        session = db.get_session(self.conn, user_id)
        self.assertEqual(session["state"], {"step": "ask_goal"})
        self.assertEqual(session["meta"], {"source": "site"})

    def test_db_connection_uses_row_factory(self) -> None:
        cursor = self.conn.execute("SELECT 1 AS one")
        row = cursor.fetchone()
        self.assertIsInstance(row, sqlite3.Row)
        self.assertEqual(row["one"], 1)

    def test_db_connection_enables_foreign_keys(self) -> None:
        row = self.conn.execute("PRAGMA foreign_keys").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 1)

    def test_db_connection_sets_busy_timeout(self) -> None:
        row = self.conn.execute("PRAGMA busy_timeout").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 5000)

    def test_db_connection_uses_wal_journal_mode(self) -> None:
        row = self.conn.execute("PRAGMA journal_mode").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row[0]).lower(), "wal")

    def test_create_lead_record_persists_contact_json(self) -> None:
        user_id = db.get_or_create_user(self.conn, "telegram", "555")
        lead_row_id = db.create_lead_record(
            conn=self.conn,
            user_id=user_id,
            status="created",
            tallanto_entry_id="lead-1001",
            contact={"phone": "+79991234567", "brand": "kmipt"},
        )

        self.assertGreater(lead_row_id, 0)
        cursor = self.conn.execute(
            "SELECT status, tallanto_entry_id, contact_json FROM leads WHERE lead_id = ?",
            (lead_row_id,),
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "created")
        self.assertEqual(row["tallanto_entry_id"], "lead-1001")
        self.assertEqual(
            json.loads(row["contact_json"]),
            {"phone": "+79991234567", "brand": "kmipt"},
        )

    def test_get_session_returns_empty_when_absent(self) -> None:
        session = db.get_session(self.conn, user_id=999999)
        self.assertEqual(session, {"state": {}, "meta": {}})

    def test_list_recent_leads_returns_joined_user_and_contact(self) -> None:
        user_id = db.get_or_create_user(
            self.conn,
            channel="telegram",
            external_id="701",
            username="lead_user",
            first_name="Lead",
            last_name="User",
        )
        db.create_lead_record(
            conn=self.conn,
            user_id=user_id,
            status="created",
            tallanto_entry_id="tl-1",
            contact={"phone": "+79990000001", "source": "test"},
        )

        leads = db.list_recent_leads(self.conn, limit=10)
        self.assertGreaterEqual(len(leads), 1)
        first = leads[0]
        self.assertEqual(first["user_id"], user_id)
        self.assertEqual(first["contact"]["phone"], "+79990000001")
        self.assertEqual(first["username"], "lead_user")

    def test_list_recent_conversations_and_messages(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="702")
        db.log_message(
            conn=self.conn,
            user_id=user_id,
            direction="inbound",
            text="Привет",
            meta={"m": 1},
        )
        db.log_message(
            conn=self.conn,
            user_id=user_id,
            direction="outbound",
            text="Здравствуйте",
            meta={"m": 2},
        )

        conversations = db.list_recent_conversations(self.conn, limit=10)
        self.assertGreaterEqual(len(conversations), 1)
        self.assertEqual(conversations[0]["user_id"], user_id)
        self.assertEqual(conversations[0]["messages_count"], 2)

        messages = db.list_conversation_messages(self.conn, user_id=user_id, limit=10)
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]["direction"], "inbound")
        self.assertEqual(messages[1]["direction"], "outbound")
        self.assertEqual(messages[0]["meta"]["m"], 1)

    def test_list_recent_messages_returns_last_rows_in_chronological_order(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="703")
        db.log_message(self.conn, user_id, "inbound", "msg-1", {"n": 1})
        db.log_message(self.conn, user_id, "outbound", "msg-2", {"n": 2})
        db.log_message(self.conn, user_id, "inbound", "msg-3", {"n": 3})

        recent = db.list_recent_messages(self.conn, user_id=user_id, limit=2)

        self.assertEqual(len(recent), 2)
        self.assertEqual(recent[0]["text"], "msg-2")
        self.assertEqual(recent[1]["text"], "msg-3")
        self.assertEqual(recent[0]["meta"]["n"], 2)

    def test_call_records_lifecycle_and_lookup(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="call-user-1")
        thread_id = f"tg:{user_id}"

        call_id = db.create_call_record(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            source_type="upload",
            source_ref="recording.m4a",
            file_path="/tmp/recording.m4a",
            status="queued",
            created_by="test",
        )
        self.assertGreater(call_id, 0)

        updated = db.update_call_record_status(self.conn, call_id=call_id, status="processing")
        self.assertTrue(updated)
        updated = db.update_call_record_status(self.conn, call_id=call_id, status="done", duration_seconds=123.4)
        self.assertTrue(updated)

        transcript_id = db.upsert_call_transcript(
            self.conn,
            call_id=call_id,
            provider="heuristic",
            transcript_text="Клиент интересуется ЕГЭ по математике.",
            language="ru",
            confidence=0.61,
        )
        self.assertGreater(transcript_id, 0)

        summary_id = db.upsert_call_summary(
            self.conn,
            call_id=call_id,
            summary_text="Клиент готов обсудить программу и формат.",
            interests=["ЕГЭ", "математика"],
            objections=["цена"],
            next_best_action="Назначить консультацию",
            warmth="hot",
            confidence=0.79,
            model_name="call_copilot_v1",
        )
        self.assertGreater(summary_id, 0)

        item = db.get_call_record(self.conn, call_id=call_id)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["status"], "done")
        self.assertEqual(item["warmth"], "hot")
        self.assertIn("ЕГЭ", item["interests"])
        self.assertEqual(item["transcript_provider"], "heuristic")

        records = db.list_call_records(self.conn, status="done", limit=10)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], call_id)

        latest = db.get_latest_call_summary_for_thread(self.conn, thread_id=thread_id)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest["call_id"], call_id)
        self.assertEqual(latest["warmth"], "hot")
        self.assertIn("цена", latest["objections"])

    def test_update_call_record_status_returns_false_for_missing_call(self) -> None:
        updated = db.update_call_record_status(self.conn, call_id=99999, status="done")
        self.assertFalse(updated)

    def test_mango_events_and_phone_lookup_and_cleanup(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="mango-user-1")
        db.create_lead_record(
            conn=self.conn,
            user_id=user_id,
            status="created",
            contact={"phone": "+7 (999) 111-22-33"},
            tallanto_entry_id=None,
        )
        matched_user = db.find_user_by_phone(self.conn, phone="89991112233")
        self.assertEqual(matched_user, user_id)

        event_state = db.create_or_get_mango_event(
            self.conn,
            event_id="evt-1",
            call_external_id="call-1",
            source="webhook",
            payload={"event_id": "evt-1"},
        )
        self.assertTrue(event_state["is_new"])
        duplicate = db.create_or_get_mango_event(
            self.conn,
            event_id="evt-1",
            call_external_id="call-1",
            source="webhook",
            payload={"event_id": "evt-1"},
        )
        self.assertFalse(duplicate["is_new"])

        updated = db.update_mango_event_status(
            self.conn,
            event_row_id=int(event_state["id"]),
            status="done",
        )
        self.assertTrue(updated)
        events = db.list_mango_events(self.conn, status="done", limit=10)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_id"], "evt-1")
        self.assertTrue(db.get_latest_mango_event_created_at(self.conn))

        call_id = db.create_call_record(
            self.conn,
            user_id=user_id,
            thread_id=f"tg:{user_id}",
            source_type="upload",
            source_ref="tmp-file",
            file_path="/tmp/fake-call-audio.raw",
            status="done",
            created_by="test",
        )
        self.conn.execute(
            "UPDATE call_records SET created_at = datetime('now', '-5 hours') WHERE id = ?",
            (call_id,),
        )
        self.conn.commit()

        cleanup_rows = db.list_call_records_with_files_for_cleanup(
            self.conn,
            older_than_hours=2,
            limit=10,
        )
        self.assertEqual(len(cleanup_rows), 1)
        self.assertEqual(int(cleanup_rows[0]["id"]), call_id)

        self.assertTrue(db.clear_call_record_file_path(self.conn, call_id=call_id))
        item = db.get_call_record(self.conn, call_id=call_id)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertIsNone(item["file_path"])

    def test_get_and_upsert_conversation_context(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="704")
        self.assertEqual(db.get_conversation_context(self.conn, user_id), {})

        db.upsert_conversation_context(
            self.conn,
            user_id=user_id,
            summary={
                "profile": {"grade": 10, "goal": "ege"},
                "summary_text": "Ученик 10 класса, цель ЕГЭ.",
            },
        )

        context = db.get_conversation_context(self.conn, user_id)
        self.assertEqual(context.get("profile", {}).get("grade"), 10)
        self.assertIn("ЕГЭ", context.get("summary_text", ""))

    def test_init_db_migrates_duplicate_sessions_and_enforces_unique_index(self) -> None:
        legacy_path = Path(self.tempdir.name) / "legacy_sessions.db"
        with sqlite3.connect(legacy_path) as conn:
            conn.execute(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT NOT NULL,
                    external_id TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    state_json TEXT DEFAULT '{}',
                    meta_json TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                "INSERT INTO users (channel, external_id) VALUES ('telegram', 'legacy-user')"
            )
            conn.execute(
                "INSERT INTO sessions (user_id, state_json) VALUES (1, '{\"step\":\"ask_grade\"}')"
            )
            conn.execute(
                "INSERT INTO sessions (user_id, state_json) VALUES (1, '{\"step\":\"ask_goal\"}')"
            )
            conn.commit()

        db.init_db(legacy_path)
        conn = db.get_connection(legacy_path)
        try:
            count = conn.execute("SELECT COUNT(*) AS cnt FROM sessions WHERE user_id = 1").fetchone()["cnt"]
            self.assertEqual(count, 1)

            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO sessions (user_id, state_json) VALUES (?, ?)",
                    (1, '{"step":"duplicate"}'),
                )
        finally:
            conn.close()

    def test_enqueue_webhook_update_deduplicates_by_update_id(self) -> None:
        first = db.enqueue_webhook_update(
            self.conn,
            payload={"update_id": 100, "message": {"text": "hello"}},
            update_id=100,
        )
        second = db.enqueue_webhook_update(
            self.conn,
            payload={"update_id": 100, "message": {"text": "hello-again"}},
            update_id=100,
        )

        self.assertTrue(first["is_new"])
        self.assertFalse(second["is_new"])
        self.assertEqual(first["id"], second["id"])

    def test_claim_and_mark_webhook_update_done(self) -> None:
        queued = db.enqueue_webhook_update(
            self.conn,
            payload={"update_id": 101, "message": {"text": "payload"}},
            update_id=101,
        )
        self.assertTrue(queued["is_new"])

        claimed = db.claim_webhook_update(self.conn)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], queued["id"])
        self.assertEqual(claimed["attempts"], 1)
        self.assertEqual(claimed["payload"]["update_id"], 101)

        db.mark_webhook_update_done(self.conn, queue_id=queued["id"])
        done_count = db.count_webhook_updates_by_status(self.conn, "done")
        self.assertEqual(done_count, 1)

    def test_mark_webhook_update_retry_then_failed(self) -> None:
        queued = db.enqueue_webhook_update(
            self.conn,
            payload={"update_id": 102},
            update_id=102,
        )
        self.assertTrue(queued["is_new"])

        first_claim = db.claim_webhook_update(self.conn)
        self.assertIsNotNone(first_claim)
        first_status = db.mark_webhook_update_retry(
            self.conn,
            queue_id=queued["id"],
            error="temporary-error",
            retry_delay_seconds=1,
            max_attempts=2,
        )
        self.assertEqual(first_status, "retry")

        # Force immediate retry in test environment.
        self.conn.execute(
            "UPDATE webhook_updates SET next_attempt_at = CURRENT_TIMESTAMP WHERE id = ?",
            (queued["id"],),
        )
        self.conn.commit()

        second_claim = db.claim_webhook_update(self.conn)
        self.assertIsNotNone(second_claim)
        self.assertEqual(second_claim["attempts"], 2)
        second_status = db.mark_webhook_update_retry(
            self.conn,
            queue_id=queued["id"],
            error="permanent-error",
            retry_delay_seconds=1,
            max_attempts=2,
        )
        self.assertEqual(second_status, "failed")
        failed_count = db.count_webhook_updates_by_status(self.conn, "failed")
        self.assertEqual(failed_count, 1)

    def test_requeue_stuck_webhook_updates(self) -> None:
        queued = db.enqueue_webhook_update(
            self.conn,
            payload={"update_id": 103},
            update_id=103,
        )
        self.assertTrue(queued["is_new"])
        claimed = db.claim_webhook_update(self.conn)
        self.assertIsNotNone(claimed)

        self.conn.execute(
            "UPDATE webhook_updates SET updated_at = datetime('now', '-10 minutes') WHERE id = ?",
            (queued["id"],),
        )
        self.conn.commit()

        moved = db.requeue_stuck_webhook_updates(self.conn, stale_after_seconds=60)
        self.assertEqual(moved, 1)
        retry_count = db.count_webhook_updates_by_status(self.conn, "retry")
        self.assertEqual(retry_count, 1)

    def test_crm_cache_roundtrip_and_ttl(self) -> None:
        db.upsert_crm_cache(
            self.conn,
            key="crm:modules",
            value={"items": ["contacts", "leads"]},
        )

        cached = db.get_crm_cache(self.conn, key="crm:modules", max_age_seconds=3600)
        self.assertIsNotNone(cached)
        self.assertEqual(cached["items"], ["contacts", "leads"])

        self.conn.execute(
            "UPDATE crm_cache SET updated_at = datetime('now', '-2 hours') WHERE key = ?",
            ("crm:modules",),
        )
        self.conn.commit()
        expired = db.get_crm_cache(self.conn, key="crm:modules", max_age_seconds=10)
        self.assertIsNone(expired)

    def test_revenue_entities_roundtrip_and_metrics(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="rev-1", first_name="Dmitriy")
        thread_id = f"tg:{user_id}"
        db.log_message(self.conn, user_id=user_id, direction="inbound", text="Здравствуйте", meta={})

        draft_id = db.create_reply_draft(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            draft_text="Черновик ответа",
            model_name="gpt-test",
            created_by="admin",
            idempotency_key="rev-1-draft",
        )
        draft = db.get_reply_draft(self.conn, draft_id)
        self.assertIsNotNone(draft)
        self.assertEqual(draft["status"], "created")

        updated = db.update_reply_draft_text(
            self.conn,
            draft_id=draft_id,
            draft_text="Черновик ответа 2",
            model_name="gpt-test-2",
            quality={"quality": "ok"},
            actor="admin",
        )
        self.assertTrue(updated)
        approved = db.update_reply_draft_status(self.conn, draft_id=draft_id, status="approved", actor="admin")
        self.assertTrue(approved)

        action_id = db.create_approval_action(
            self.conn,
            draft_id=draft_id,
            user_id=user_id,
            thread_id=thread_id,
            action="draft_approved",
            actor="admin",
            payload={"source": "test"},
        )
        self.assertGreater(action_id, 0)

        followup_id = db.create_followup_task(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            priority="hot",
            reason="Нужен созвон",
            status="pending",
            due_at=None,
            assigned_to="manager",
        )
        self.assertGreater(followup_id, 0)

        score_id = db.create_lead_score(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            score=92.5,
            temperature="hot",
            confidence=0.8,
            factors={"reason": "high_intent"},
        )
        self.assertGreater(score_id, 0)

        outcome_id = db.upsert_conversation_outcome(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            outcome="consultation_booked",
            note="Перезвонить вечером",
            created_by="admin",
        )
        self.assertGreater(outcome_id, 0)

        threads = db.list_inbox_threads(self.conn, limit=10)
        self.assertEqual(len(threads), 1)
        self.assertEqual(threads[0]["thread_id"], thread_id)
        self.assertEqual(threads[0]["status"], "approved")
        self.assertEqual(threads[0]["pending_followups"], 1)
        self.assertEqual(threads[0]["lead_score"]["temperature"], "hot")

        detail = db.get_inbox_thread_detail(self.conn, user_id=user_id, limit_messages=50)
        self.assertEqual(detail["thread_id"], thread_id)
        self.assertGreaterEqual(len(detail["messages"]), 1)
        self.assertEqual(detail["outcome"]["outcome"], "consultation_booked")
        self.assertEqual(detail["followups"][0]["priority"], "hot")
        self.assertEqual(detail["lead_score"]["score"], 92.5)
        self.assertEqual(detail["approval_actions"][0]["action"], "draft_approved")

        metrics = db.get_revenue_metrics_snapshot(self.conn)
        self.assertGreaterEqual(metrics["drafts_created_today"], 1)
        self.assertEqual(metrics["followups_pending"], 1)
        self.assertEqual(metrics["lead_temperature"]["hot"], 1)

    def test_create_reply_draft_idempotency_returns_existing_draft(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="rev-2")
        thread_id = f"tg:{user_id}"
        first_id = db.create_reply_draft(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            draft_text="Первый вариант",
            idempotency_key="same-key",
        )
        second_id = db.create_reply_draft(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            draft_text="Второй вариант",
            idempotency_key="same-key",
        )
        self.assertEqual(first_id, second_id)

        drafts = db.list_reply_drafts_for_thread(self.conn, thread_id=thread_id, limit=10)
        self.assertEqual(len(drafts), 1)
        self.assertEqual(drafts[0]["draft_text"], "Первый вариант")

    def test_business_tables_roundtrip_with_delete_marker(self) -> None:
        owner_id = db.get_or_create_user(self.conn, channel="telegram", external_id="owner-1")
        lead_user_id = db.get_or_create_user(
            self.conn,
            channel="telegram_business",
            external_id="client-1",
            username="client",
            first_name="Client",
        )
        connection_row_id = db.upsert_business_connection(
            self.conn,
            business_connection_id="bc-1",
            telegram_user_id=owner_id,
            user_chat_id=90001,
            can_reply=True,
            is_enabled=True,
            connected_at="2026-03-06T10:00:00+00:00",
            meta={"source": "test"},
        )
        self.assertGreater(connection_row_id, 0)

        thread_key = db.upsert_business_thread(
            self.conn,
            business_connection_id="bc-1",
            chat_id=70001,
            user_id=lead_user_id,
            direction="inbound",
            occurred_at="2026-03-06T10:05:00+00:00",
            meta={"topic": "math"},
        )
        self.assertEqual(thread_key, "biz:bc-1:70001")

        first_message_id = db.log_business_message(
            self.conn,
            business_connection_id="bc-1",
            chat_id=70001,
            telegram_message_id=501,
            user_id=lead_user_id,
            direction="inbound",
            text="Здравствуйте, нужен план подготовки",
            payload={"event_type": "business_message"},
            created_at="2026-03-06T10:05:00+00:00",
        )
        self.assertGreater(first_message_id, 0)

        duplicate_message_id = db.log_business_message(
            self.conn,
            business_connection_id="bc-1",
            chat_id=70001,
            telegram_message_id=501,
            user_id=lead_user_id,
            direction="inbound",
            text="dup",
            payload={"event_type": "business_message"},
        )
        self.assertEqual(first_message_id, duplicate_message_id)

        connection = db.get_business_connection(self.conn, business_connection_id="bc-1")
        self.assertIsNotNone(connection)
        self.assertTrue(connection["can_reply"])
        self.assertTrue(connection["is_enabled"])

        threads = db.list_recent_business_threads(self.conn, limit=10)
        self.assertEqual(len(threads), 1)
        self.assertEqual(threads[0]["thread_key"], thread_key)
        self.assertEqual(threads[0]["messages_count"], 1)

        deleted_count = db.mark_business_messages_deleted(
            self.conn,
            business_connection_id="bc-1",
            chat_id=70001,
            message_ids=[501],
        )
        self.assertEqual(deleted_count, 1)

        messages = db.list_business_messages(self.conn, thread_key=thread_key, limit=10)
        self.assertEqual(len(messages), 1)
        self.assertTrue(messages[0]["is_deleted"])
        self.assertEqual(messages[0]["payload"]["event_type"], "business_message")


if __name__ == "__main__":
    unittest.main()
