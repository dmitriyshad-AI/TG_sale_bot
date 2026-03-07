import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sales_agent.sales_core import db
from sales_agent.sales_core import faq_lab


class FaqLabTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "faq_lab.db"
        db.init_db(self.db_path)
        self.conn = db.get_connection(self.db_path)

    def tearDown(self) -> None:
        self.conn.close()
        self.tempdir.cleanup()

    def test_normalize_question_key_and_detection(self) -> None:
        self.assertEqual(faq_lab.normalize_question_key(123), "")
        self.assertEqual(
            faq_lab.normalize_question_key("  Как поступить в МФТИ?!  "),
            "как поступить в мфти",
        )
        self.assertTrue(len(faq_lab.normalize_question_key("а" * 260)) <= 180)
        self.assertEqual(faq_lab.compact_text(None), "")
        self.assertTrue(faq_lab.compact_text("x" * 300).endswith("..."))
        self.assertTrue(faq_lab.looks_like_question("Как поступить в МФТИ"))
        self.assertTrue(faq_lab.looks_like_question("Можно ли подтянуть математику?"))
        self.assertFalse(faq_lab.looks_like_question("Ок"))

    def test_chunk_and_empty_helpers(self) -> None:
        chunks = list(faq_lab._chunk(["a", "b", "c"], size=0))
        self.assertEqual(chunks, [["a", "b", "c"]])
        self.assertEqual(faq_lab._fetch_thread_counts(self.conn, thread_ids=[], action="draft_sent"), {})
        self.assertEqual(faq_lab._fetch_outcome_threads(self.conn, thread_ids=[]), set())

    def test_build_faq_candidates_aggregates_questions(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="faq-user-1")
        thread_id = f"tg:{user_id}"

        db.log_message(self.conn, user_id, "inbound", "Как поступить в МФТИ?", {})
        db.log_message(self.conn, user_id, "inbound", "Как поступить в МФТИ?", {})
        db.log_message(self.conn, user_id, "inbound", "Спасибо", {})

        draft_id = db.create_reply_draft(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            draft_text="Уточним класс и профиль.",
            model_name="faq_lab_v1",
        )
        db.create_approval_action(
            self.conn,
            draft_id=draft_id,
            user_id=user_id,
            thread_id=thread_id,
            action="draft_approved",
            actor="moderator",
            payload={"source": "test"},
        )
        db.create_approval_action(
            self.conn,
            draft_id=draft_id,
            user_id=user_id,
            thread_id=thread_id,
            action="draft_sent",
            actor="moderator",
            payload={"source": "test"},
        )
        db.upsert_conversation_outcome(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            outcome="consultation_booked",
            note="test",
        )

        candidates = faq_lab.build_faq_candidates(
            self.conn,
            window_days=30,
            min_question_count=1,
            limit=10,
        )
        self.assertEqual(len(candidates), 1)
        item = candidates[0]
        self.assertEqual(item["question_count"], 2)
        self.assertEqual(item["thread_count"], 1)
        self.assertEqual(item["approvals_count"], 1)
        self.assertEqual(item["sends_count"], 1)
        self.assertEqual(item["next_step_count"], 1)

    def test_refresh_faq_lab_persists_candidates_and_performance(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="faq-user-2")
        thread_id = f"tg:{user_id}"

        db.log_message(self.conn, user_id, "inbound", "Что делать если проседает математика?", {})
        draft_id = db.create_reply_draft(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            draft_text="Давайте начнем с диагностики уровня.",
            model_name="faq_lab_v1",
        )
        db.create_approval_action(
            self.conn,
            draft_id=draft_id,
            user_id=user_id,
            thread_id=thread_id,
            action="draft_approved",
            actor="moderator",
            payload={},
        )

        summary = faq_lab.refresh_faq_lab(
            self.conn,
            window_days=30,
            min_question_count=1,
            limit=20,
            trigger="test",
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["candidates_upserted"], 1)

        candidates = db.list_faq_candidates(self.conn, limit=10)
        self.assertEqual(len(candidates), 1)
        candidate_id = int(candidates[0]["id"])

        db.promote_faq_candidate_to_canonical(
            self.conn,
            candidate_id=candidate_id,
            answer_text="Сначала фиксируем цель и текущий уровень.",
            created_by="admin",
        )

        summary_2 = faq_lab.refresh_faq_lab(
            self.conn,
            window_days=30,
            min_question_count=1,
            limit=20,
            trigger="test-2",
        )
        self.assertTrue(summary_2["ok"])
        self.assertGreaterEqual(summary_2["canonical_synced"], 1)

        perf = db.list_answer_performance(self.conn, limit=20)
        kinds = {item["answer_kind"] for item in perf}
        self.assertIn("candidate", kinds)
        self.assertIn("canonical", kinds)

    def test_refresh_faq_lab_handles_edge_branches(self) -> None:
        summary = faq_lab.refresh_faq_lab(
            self.conn,
            window_days=30,
            min_question_count=1,
            limit=20,
            trigger="edge-empty",
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["candidates_scanned"], 0)

        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="faq-edge-user")
        self.conn.execute(
            """
            INSERT INTO business_messages (
                business_connection_id, thread_key, telegram_message_id, user_id, direction, text, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("bc-edge", "", 77, user_id, "inbound", "Какой формат лучше?", "{}"),
        )
        self.conn.commit()
        rows = faq_lab._fetch_source_messages(self.conn, window_days=30)
        self.assertEqual(rows, [])

        with patch("sales_agent.sales_core.faq_lab.db.list_canonical_answers", return_value=[{"id": 1, "candidate_id": 0}]):
            patched = faq_lab.refresh_faq_lab(
                self.conn,
                window_days=30,
                min_question_count=1,
                limit=20,
                trigger="edge-canonical-0",
            )
            self.assertTrue(patched["ok"])

        with patch("sales_agent.sales_core.faq_lab.db.list_canonical_answers", return_value=[{"id": 2, "candidate_id": 99}]), patch(
            "sales_agent.sales_core.faq_lab.db.get_faq_candidate",
            return_value=None,
        ):
            patched_none = faq_lab.refresh_faq_lab(
                self.conn,
                window_days=30,
                min_question_count=1,
                limit=20,
                trigger="edge-canonical-none",
            )
            self.assertTrue(patched_none["ok"])

    def test_build_candidates_handles_empty_key_and_empty_thread_ids(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="faq-empty-key")
        db.log_message(self.conn, user_id, "inbound", "????????", {})
        candidates = faq_lab.build_faq_candidates(self.conn, window_days=30, min_question_count=1, limit=20)
        self.assertEqual(candidates, [])

        with patch(
            "sales_agent.sales_core.faq_lab._fetch_source_messages",
            return_value=[
                {
                    "thread_id": "",
                    "text": "Как подготовиться к ЕГЭ?",
                    "created_at": "2026-03-07T12:00:00+00:00",
                    "source": "telegram",
                }
            ],
        ):
            candidates_empty_thread = faq_lab.build_faq_candidates(
                self.conn,
                window_days=30,
                min_question_count=1,
                limit=20,
            )
        self.assertEqual(candidates_empty_thread, [])

    def test_refresh_handles_candidate_upsert_zero(self) -> None:
        with patch(
            "sales_agent.sales_core.faq_lab.build_faq_candidates",
            return_value=[
                {
                    "question_key": "k1",
                    "question_text": "Как подготовиться?",
                    "question_count": 3,
                    "thread_count": 2,
                    "approvals_count": 1,
                    "sends_count": 1,
                    "next_step_count": 0,
                    "reply_approved_rate": 0.5,
                    "next_step_rate": 0.0,
                    "first_seen_at": None,
                    "last_seen_at": None,
                    "sample_thread_id": "tg:1",
                    "source": {},
                    "suggested_answer": "test",
                }
            ],
        ), patch("sales_agent.sales_core.faq_lab.db.upsert_faq_candidate", return_value=0):
            summary = faq_lab.refresh_faq_lab(
                self.conn,
                window_days=30,
                min_question_count=1,
                limit=20,
                trigger="edge-upsert-zero",
            )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["candidates_upserted"], 0)

    def test_fetch_source_messages_skips_invalid_tg_user_id(self) -> None:
        class _Cursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        class _Conn:
            def __init__(self) -> None:
                self.calls = 0

            def execute(self, _query, _params):
                self.calls += 1
                if self.calls == 1:
                    return _Cursor([{"user_id": 0, "text": "Как?", "created_at": "2026-03-07T12:00:00+00:00"}])
                return _Cursor([])

        rows = faq_lab._fetch_source_messages(_Conn(), window_days=30)
        self.assertEqual(rows, [])

    def test_refresh_faq_lab_creates_run_and_audit_event(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="faq-run-user")
        db.log_message(self.conn, user_id, "inbound", "Как лучше подготовиться к ЕГЭ по математике?", {})

        summary = faq_lab.refresh_faq_lab(
            self.conn,
            window_days=30,
            min_question_count=1,
            limit=10,
            trigger="scheduler",
        )
        self.assertTrue(summary["ok"])
        self.assertGreater(int(summary.get("run_id") or 0), 0)
        self.assertGreaterEqual(int(summary.get("duration_ms") or 0), 0)

        runs = db.list_faq_lab_runs(self.conn, limit=10)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["status"], "success")

        events = db.list_faq_lab_events(self.conn, event_type="refresh_completed", limit=10)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["payload"]["run_id"], summary["run_id"])

    def test_refresh_faq_lab_failed_creates_failed_run_and_event(self) -> None:
        with patch("sales_agent.sales_core.faq_lab.build_faq_candidates", side_effect=RuntimeError("refresh boom")):
            with self.assertRaises(RuntimeError):
                faq_lab.refresh_faq_lab(
                    self.conn,
                    window_days=30,
                    min_question_count=1,
                    limit=10,
                    trigger="scheduler-fail",
                )

        runs = db.list_faq_lab_runs(self.conn, limit=10)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["status"], "failed")
        self.assertIn("refresh boom", str(runs[0]["error_text"] or ""))

        events = db.list_faq_lab_events(self.conn, event_type="refresh_failed", limit=10)
        self.assertEqual(len(events), 1)
        self.assertIn("refresh boom", str(events[0]["payload"].get("error") or ""))

    def test_promote_candidate_to_canonical_safe_records_event(self) -> None:
        candidate_id = db.upsert_faq_candidate(
            self.conn,
            question_key="как не выгорать при подготовке",
            question_text="Как не выгорать при подготовке?",
            question_count=1,
            thread_count=1,
            approvals_count=0,
            sends_count=0,
            next_step_count=0,
            reply_approved_rate=0.0,
            next_step_rate=0.0,
            status="candidate",
            suggested_answer="Делим учебную нагрузку на этапы и фиксируем реалистичный недельный ритм.",
        )
        promoted = faq_lab.promote_candidate_to_canonical_safe(
            self.conn,
            candidate_id=candidate_id,
            answer_text=None,
            created_by="admin",
        )
        self.assertEqual(promoted["canonical"]["candidate_id"], candidate_id)
        self.assertGreater(int(promoted["event_id"]), 0)
        self.assertGreaterEqual(len(promoted["warnings"]), 1)
        events = db.list_faq_lab_events(self.conn, event_type="candidate_promoted", limit=10)
        self.assertEqual(len(events), 1)
        self.assertEqual(int(events[0]["candidate_id"] or 0), candidate_id)

    def test_promote_candidate_to_canonical_safe_validation_errors(self) -> None:
        with self.assertRaises(faq_lab.FaqLabPromotionError):
            faq_lab.promote_candidate_to_canonical_safe(
                self.conn,
                candidate_id=9999,
                answer_text="test",
                created_by="admin",
            )

        candidate_id = db.upsert_faq_candidate(
            self.conn,
            question_key="архивный вопрос",
            question_text="Архивный вопрос?",
            question_count=3,
            thread_count=2,
            approvals_count=1,
            sends_count=1,
            next_step_count=1,
            reply_approved_rate=0.5,
            next_step_rate=0.5,
            status="archived",
            suggested_answer="Достаточно длинный безопасный ответ для проверки.",
        )
        with self.assertRaises(faq_lab.FaqLabPromotionError) as archived_ctx:
            faq_lab.promote_candidate_to_canonical_safe(
                self.conn,
                candidate_id=candidate_id,
                answer_text=None,
                created_by="admin",
            )
        self.assertEqual(getattr(archived_ctx.exception, "code", ""), "archived")

        candidate_ok = db.upsert_faq_candidate(
            self.conn,
            question_key="обычный вопрос",
            question_text="Обычный вопрос?",
            question_count=3,
            thread_count=2,
            approvals_count=1,
            sends_count=1,
            next_step_count=1,
            reply_approved_rate=0.5,
            next_step_rate=0.5,
            status="candidate",
            suggested_answer="Достаточно длинный безопасный ответ для проверки.",
        )
        with self.assertRaises(faq_lab.FaqLabPromotionError) as short_ctx:
            faq_lab.promote_candidate_to_canonical_safe(
                self.conn,
                candidate_id=candidate_ok,
                answer_text="коротко",
                created_by="admin",
            )
        self.assertEqual(getattr(short_ctx.exception, "code", ""), "answer_too_short")


if __name__ == "__main__":
    unittest.main()
