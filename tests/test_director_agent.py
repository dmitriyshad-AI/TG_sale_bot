import tempfile
import unittest
from pathlib import Path

from sales_agent.sales_core import db
from sales_agent.sales_core import director_agent


class DirectorAgentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "director_agent.db"
        db.init_db(self.db_path)
        self.conn = db.get_connection(self.db_path)

    def tearDown(self) -> None:
        self.conn.close()
        self.tempdir.cleanup()

    def test_extract_goal_tags(self) -> None:
        tags = director_agent.extract_goal_tags("Верни тёплых лидов по ЕГЭ информатика")
        self.assertIn("reactivation", tags)
        self.assertIn("ege", tags)
        self.assertIn("informatics", tags)

    def test_text_helpers_and_empty_goal_tags(self) -> None:
        self.assertEqual(director_agent._normalize_text(None), "")
        self.assertEqual(director_agent._compact_text(None), "")
        self.assertEqual(director_agent.extract_goal_tags("   "), [])
        compacted = director_agent._compact_text("a" * 400, max_len=25)
        self.assertTrue(compacted.endswith("..."))
        self.assertLessEqual(len(compacted), 25)

    def test_score_text_against_tags_handles_empty_values(self) -> None:
        self.assertEqual(director_agent._score_text_against_tags("", ["ege"]), 0)
        self.assertEqual(director_agent._score_text_against_tags("   ", ["ege"]), 0)
        self.assertGreaterEqual(
            director_agent._score_text_against_tags("Подготовка к ЕГЭ по математике", ["ege", "math"]),
            3,
        )

    def test_discover_thread_candidates_and_plan_build(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="u-1")
        db.log_message(self.conn, user_id, "inbound", "Нужна стратегия ЕГЭ по информатике", {})

        biz_user = db.get_or_create_user(self.conn, channel="telegram_business", external_id="u-biz")
        db.upsert_business_connection(
            self.conn,
            business_connection_id="bc-1",
            telegram_user_id=101,
            user_chat_id=202,
            can_reply=True,
            is_enabled=True,
            connected_at="2026-03-07T10:00:00+00:00",
            meta={},
        )
        db.log_business_message(
            self.conn,
            business_connection_id="bc-1",
            chat_id=9999,
            telegram_message_id=1,
            user_id=biz_user,
            direction="inbound",
            text="Подскажите план поступления в МФТИ",
            payload={},
        )

        candidates = director_agent.discover_thread_candidates(
            self.conn,
            goal_text="Верни теплых лидов по ЕГЭ информатика",
            max_candidates=20,
        )
        self.assertGreaterEqual(len(candidates), 2)

        plan = director_agent.build_campaign_plan(
            goal_text="Верни теплых лидов по ЕГЭ информатика",
            candidates=candidates,
            max_actions=3,
        )
        self.assertEqual(plan["model_name"], director_agent.DIRECTOR_MODEL_NAME)
        self.assertGreaterEqual(len(plan["actions"]), 1)
        self.assertIn("objective", plan)

    def test_build_campaign_plan_fallback_when_no_candidates(self) -> None:
        plan = director_agent.build_campaign_plan(
            goal_text="Подготовить кампанию без истории",
            candidates=[],
            max_actions=5,
        )
        self.assertEqual(len(plan["actions"]), 1)
        self.assertEqual(plan["actions"][0]["action_type"], "manual_review")

    def test_build_campaign_plan_requires_non_empty_goal(self) -> None:
        with self.assertRaises(ValueError):
            director_agent.build_campaign_plan(goal_text="   ", candidates=[], max_actions=5)

    def test_infer_user_id_from_thread_id(self) -> None:
        self.assertEqual(director_agent._infer_user_id_from_thread_id("tg:42"), 42)
        self.assertIsNone(director_agent._infer_user_id_from_thread_id("biz:bc:42"))
        self.assertIsNone(director_agent._infer_user_id_from_thread_id("tg:not-number"))
        self.assertIsNone(director_agent._infer_user_id_from_thread_id(None))

    def test_apply_campaign_plan_creates_artifacts(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="u-apply")
        thread_id = f"tg:{user_id}"

        goal_id = db.create_campaign_goal(
            self.conn,
            goal_text="Вернуть теплых лидов",
            created_by="admin",
        )
        plan = {
            "actions": [
                {
                    "action_type": "reactivation",
                    "thread_id": thread_id,
                    "user_id": user_id,
                    "priority": "hot",
                    "reason": "keyword_match",
                },
                {
                    "action_type": "reactivation",
                    "thread_id": None,
                    "user_id": None,
                    "priority": "warm",
                    "reason": "no-thread",
                },
            ]
        }
        plan_id = db.create_campaign_plan(
            self.conn,
            goal_id=goal_id,
            objective="Тестовый план",
            actions=plan["actions"],
            created_by="admin",
        )

        report = director_agent.apply_campaign_plan(
            self.conn,
            goal_id=goal_id,
            plan_id=plan_id,
            plan=plan,
            actor="director:auto",
        )
        self.assertEqual(report["created_actions"], 1)
        self.assertEqual(report["created_drafts"], 1)
        self.assertEqual(report["created_followups"], 1)
        self.assertEqual(report["skipped"], 1)

        actions = db.list_campaign_actions(self.conn, plan_id=plan_id, limit=20)
        self.assertEqual(len(actions), 2)
        statuses = {item["status"] for item in actions}
        self.assertIn("created", statuses)
        self.assertIn("skipped", statuses)

        reports = db.list_campaign_reports(self.conn, plan_id=plan_id, limit=5)
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]["report"]["created_actions"], 1)

    def test_apply_campaign_plan_handles_non_list_actions_and_non_dict_entries(self) -> None:
        goal_id = db.create_campaign_goal(
            self.conn,
            goal_text="Нормализация actions",
            created_by="admin",
        )
        plan_id = db.create_campaign_plan(
            self.conn,
            goal_id=goal_id,
            objective="Проверка edge cases",
            actions=[],
            created_by="admin",
        )

        report_from_non_list = director_agent.apply_campaign_plan(
            self.conn,
            goal_id=goal_id,
            plan_id=plan_id,
            plan={"actions": "bad-type"},
            actor="director:auto",
        )
        self.assertEqual(report_from_non_list["created_actions"], 0)
        self.assertEqual(report_from_non_list["skipped"], 0)

        report_from_non_dict = director_agent.apply_campaign_plan(
            self.conn,
            goal_id=goal_id,
            plan_id=plan_id,
            plan={"actions": ["bad-item"]},
            actor="director:auto",
        )
        self.assertEqual(report_from_non_dict["created_actions"], 0)
        self.assertEqual(report_from_non_dict["skipped"], 1)


if __name__ == "__main__":
    unittest.main()
