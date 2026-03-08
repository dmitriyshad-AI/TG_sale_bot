import tempfile
import unittest
from pathlib import Path

from sales_agent.sales_api.services.runtime_metrics import build_runtime_enrichment
from sales_agent.sales_core import db
from sales_agent.sales_core.config import Settings


class RuntimeMetricsServiceTests(unittest.TestCase):
    def _settings(self, db_path: Path) -> Settings:
        return Settings(
            telegram_bot_token="token",
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
            enable_faq_lab=True,
            enable_director_agent=True,
            enable_mango_auto_ingest=True,
            mango_calls_path="/vpbx/stats/calls/request",
            mango_polling_enabled=True,
            mango_poll_interval_seconds=600,
            mango_poll_limit_per_run=50,
            mango_poll_retry_attempts=2,
            mango_poll_retry_backoff_seconds=3,
            mango_retry_failed_limit_per_run=20,
            mango_call_recording_ttl_hours=24,
            faq_lab_scheduler_enabled=True,
            faq_lab_interval_seconds=3600,
            faq_lab_window_days=90,
            faq_lab_min_question_count=2,
            faq_lab_max_items_per_run=120,
        )

    def test_build_runtime_enrichment_returns_expected_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime_metrics.db"
            db.init_db(db_path)
            settings = self._settings(db_path)

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="runtime-metrics-user")
                thread_id = f"tg:{user_id}"
                db.create_call_record(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    source_type="url",
                    source_ref="https://example.com/failed.wav",
                    status="failed",
                    error_text="timeout",
                    created_by="test",
                )
                db.create_faq_lab_run(
                    conn,
                    trigger="manual",
                    status="failed",
                    window_days=30,
                    min_question_count=1,
                    requested_limit=10,
                )
                goal_id = db.create_campaign_goal(conn, goal_text="Goal", created_by="test")
                db.create_campaign_plan(
                    conn,
                    goal_id=goal_id,
                    objective="Objective",
                    actions=[{"action_type": "manual_review", "thread_id": thread_id}],
                    status="draft",
                    created_by="test",
                )
                mango_state = db.create_or_get_mango_event(
                    conn,
                    event_id="evt-runtime-1",
                    call_external_id="call-1",
                    source="webhook",
                    payload={"event": "call_record"},
                )
                db.update_mango_event_status(
                    conn,
                    event_row_id=int(mango_state["id"]),
                    status="failed",
                    error_text="bad payload",
                )
                db.enqueue_webhook_update(conn, update_id=123, payload={"update_id": 123})

                payload = build_runtime_enrichment(
                    conn=conn,
                    settings=settings,
                    mango_webhook_path="/api/mango/webhook",
                    mango_ingest_enabled=lambda: True,
                )
            finally:
                conn.close()

        self.assertIn("webhook_queue", payload)
        self.assertIn("mango", payload)
        self.assertIn("calls", payload)
        self.assertIn("faq_lab", payload)
        self.assertIn("director", payload)
        self.assertEqual(payload["webhook_queue"]["pending"], 1)
        self.assertEqual(payload["mango"]["events_failed"], 1)
        self.assertEqual(payload["calls"]["records_failed"], 1)
        self.assertEqual(payload["faq_lab"]["runs_failed"], 1)
        self.assertEqual(payload["director"]["plans_draft"], 1)


if __name__ == "__main__":
    unittest.main()
