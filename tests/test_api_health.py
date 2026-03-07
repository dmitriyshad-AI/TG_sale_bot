import unittest
import tempfile
from pathlib import Path

try:
    from tests.test_client_compat import build_test_client

    from sales_agent.sales_api.main import app, create_app
    from sales_agent.sales_core.config import Settings
    from sales_agent.sales_core import db

    HAS_FASTAPI = True
except ModuleNotFoundError:
    HAS_FASTAPI = False


@unittest.skipUnless(HAS_FASTAPI, "fastapi dependencies are not installed")
class ApiHealthTests(unittest.TestCase):
    def test_health_endpoint_returns_ok_payload(self) -> None:
        client = build_test_client(app)
        response = client.get("/api/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok", "service": "sales-agent"})

    def test_runtime_diagnostics_endpoint_returns_sanitized_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "catalog.yaml"
            catalog_path.write_text(
                """
products:
  - id: kmipt-ege-math
    brand: kmipt
    title: Подготовка к ЕГЭ по математике
    url: https://example.com/ege
    category: ege
    grade_min: 10
    grade_max: 11
    subjects: [math]
    format: online
    usp:
      - Мини-группа
      - Сильный преподаватель
      - Разбор домашних заданий
""".strip(),
                encoding="utf-8",
            )
            knowledge_path = root / "knowledge"
            knowledge_path.mkdir(parents=True, exist_ok=True)
            (knowledge_path / "faq_general.md").write_text("FAQ", encoding="utf-8")

            cfg = Settings(
                telegram_bot_token="token",
                openai_api_key="sk-test",
                openai_model="gpt-4.1",
                tallanto_api_url="",
                tallanto_api_key="",
                brand_default="kmipt",
                database_path=root / "data" / "sales_agent.db",
                catalog_path=catalog_path,
                knowledge_path=knowledge_path,
                vector_store_meta_path=root / "data" / "vector_store.json",
                openai_vector_store_id="vs_123",
                admin_user="",
                admin_pass="",
            )
            cfg.database_path.parent.mkdir(parents=True, exist_ok=True)
            client = build_test_client(create_app(cfg))
            response = client.get("/api/runtime/diagnostics")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn(payload.get("status"), {"ok", "warn", "fail"})
        self.assertIn("runtime", payload)
        runtime = payload.get("runtime") or {}
        self.assertIn("calls", runtime)
        self.assertIn("faq_lab", runtime)
        self.assertIn("director", runtime)
        self.assertNotIn("openai_api_key", str(payload).lower())
        self.assertNotIn("telegram_bot_token", str(payload).lower())

    def test_create_app_requires_webhook_secret_in_production(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = Settings(
                telegram_bot_token="token",
                openai_api_key="sk-test",
                openai_model="gpt-4.1",
                tallanto_api_url="",
                tallanto_api_key="",
                brand_default="kmipt",
                database_path=root / "data" / "sales_agent.db",
                catalog_path=Path("catalog/products.yaml"),
                knowledge_path=Path("knowledge"),
                vector_store_meta_path=root / "data" / "vector_store.json",
                openai_vector_store_id="",
                admin_user="admin",
                admin_pass="secret",
                app_env="production",
                telegram_mode="webhook",
                telegram_webhook_secret="",
            )
            cfg.database_path.parent.mkdir(parents=True, exist_ok=True)
            with self.assertRaises(RuntimeError):
                create_app(cfg)

    def test_runtime_diagnostics_exposes_revenue_runtime_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "catalog.yaml"
            catalog_path.write_text(
                """
products:
  - id: kmipt-ege-math
    brand: kmipt
    title: Подготовка к ЕГЭ по математике
    url: https://example.com/ege
    category: ege
    grade_min: 10
    grade_max: 11
    subjects: [math]
    format: online
    usp:
      - Мини-группа
      - Сильный преподаватель
      - Разбор домашних заданий
""".strip(),
                encoding="utf-8",
            )
            knowledge_path = root / "knowledge"
            knowledge_path.mkdir(parents=True, exist_ok=True)
            (knowledge_path / "faq_general.md").write_text("FAQ", encoding="utf-8")

            cfg = Settings(
                telegram_bot_token="token",
                openai_api_key="sk-test",
                openai_model="gpt-4.1",
                tallanto_api_url="",
                tallanto_api_key="",
                brand_default="kmipt",
                database_path=root / "data" / "sales_agent.db",
                catalog_path=catalog_path,
                knowledge_path=knowledge_path,
                vector_store_meta_path=root / "data" / "vector_store.json",
                openai_vector_store_id="vs_123",
                admin_user="admin",
                admin_pass="secret",
                enable_call_copilot=True,
                enable_faq_lab=True,
                enable_director_agent=True,
            )
            cfg.database_path.parent.mkdir(parents=True, exist_ok=True)
            with db.get_connection(cfg.database_path) as conn:
                db.init_db(cfg.database_path)
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="diag-user")
                thread_id = f"tg:{user_id}"
                db.create_call_record(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    source_type="url",
                    source_ref="https://example.com/call.wav",
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
                goal_id = db.create_campaign_goal(conn, goal_text="Test campaign", created_by="test")
                db.create_campaign_plan(
                    conn,
                    goal_id=goal_id,
                    objective="Test objective",
                    actions=[{"action_type": "manual_review", "thread_id": thread_id, "priority": "warm"}],
                    status="draft",
                    created_by="test",
                )

            client = build_test_client(create_app(cfg))
            response = client.get("/api/runtime/diagnostics")

        self.assertEqual(response.status_code, 200)
        runtime = response.json().get("runtime") or {}
        calls = runtime.get("calls") or {}
        self.assertEqual(calls.get("records_failed"), 1)
        faq = runtime.get("faq_lab") or {}
        self.assertEqual(faq.get("runs_failed"), 1)
        self.assertEqual((faq.get("latest_run_status") or "").lower(), "failed")
        director = runtime.get("director") or {}
        self.assertEqual(director.get("plans_draft"), 1)


if __name__ == "__main__":
    unittest.main()
