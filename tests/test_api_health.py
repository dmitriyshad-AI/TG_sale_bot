import unittest
import tempfile
from pathlib import Path

try:
    from fastapi.testclient import TestClient

    from sales_agent.sales_api.main import app, create_app
    from sales_agent.sales_core.config import Settings

    HAS_FASTAPI = True
except ModuleNotFoundError:
    HAS_FASTAPI = False


@unittest.skipUnless(HAS_FASTAPI, "fastapi dependencies are not installed")
class ApiHealthTests(unittest.TestCase):
    def test_health_endpoint_returns_ok_payload(self) -> None:
        client = TestClient(app)
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
            client = TestClient(create_app(cfg))
            response = client.get("/api/runtime/diagnostics")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn(payload.get("status"), {"ok", "warn", "fail"})
        self.assertIn("runtime", payload)
        self.assertNotIn("openai_api_key", str(payload).lower())
        self.assertNotIn("telegram_bot_token", str(payload).lower())


if __name__ == "__main__":
    unittest.main()
