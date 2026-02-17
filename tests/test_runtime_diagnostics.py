import tempfile
import unittest
from pathlib import Path

from sales_agent.sales_core.config import Settings
from sales_agent.sales_core.runtime_diagnostics import build_runtime_diagnostics


def _write_catalog(path: Path) -> None:
    path.write_text(
        """
products:
  - id: kmipt-ege-math
    brand: kmipt
    title: Подготовка к ЕГЭ по математике
    url: https://example.com/ege-math
    category: ege
    grade_min: 10
    grade_max: 11
    subjects: [math]
    format: online
    usp:
      - Мини-группа
      - Сильные преподаватели
      - Регулярный контроль прогресса
""".strip(),
        encoding="utf-8",
    )


class RuntimeDiagnosticsTests(unittest.TestCase):
    def test_diagnostics_reports_fail_when_critical_settings_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "catalog.yaml"
            _write_catalog(catalog_path)
            settings = Settings(
                telegram_bot_token="",
                openai_api_key="",
                openai_model="gpt-4.1",
                tallanto_api_url="",
                tallanto_api_key="",
                brand_default="kmipt",
                database_path=root / "data" / "sales_agent.db",
                catalog_path=catalog_path,
                knowledge_path=root / "knowledge",
                vector_store_meta_path=root / "data" / "vector_store.json",
                openai_vector_store_id="",
                admin_user="",
                admin_pass="",
            )
            settings.database_path.parent.mkdir(parents=True, exist_ok=True)

            diagnostics = build_runtime_diagnostics(settings)

        self.assertEqual(diagnostics["status"], "fail")
        issue_codes = {item["code"] for item in diagnostics["issues"]}
        self.assertIn("telegram_token_missing", issue_codes)
        self.assertIn("openai_key_missing", issue_codes)

    def test_diagnostics_reports_ok_when_runtime_is_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "catalog.yaml"
            _write_catalog(catalog_path)
            knowledge_path = root / "knowledge"
            knowledge_path.mkdir(parents=True, exist_ok=True)
            (knowledge_path / "faq_general.md").write_text("FAQ", encoding="utf-8")
            db_path = root / "data" / "sales_agent.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            settings = Settings(
                telegram_bot_token="tg-token",
                openai_api_key="sk-test",
                openai_model="gpt-4.1",
                tallanto_api_url="",
                tallanto_api_key="",
                brand_default="kmipt",
                database_path=db_path,
                catalog_path=catalog_path,
                knowledge_path=knowledge_path,
                vector_store_meta_path=root / "data" / "vector_store.json",
                openai_vector_store_id="vs_123",
                admin_user="",
                admin_pass="",
                telegram_mode="webhook",
                telegram_webhook_secret="secret",
            )

            diagnostics = build_runtime_diagnostics(settings)

        self.assertEqual(diagnostics["status"], "ok")
        runtime = diagnostics["runtime"]
        self.assertTrue(runtime["catalog_ok"])
        self.assertEqual(runtime["catalog_products_count"], 1)
        self.assertTrue(runtime["vector_store_id_set"])


if __name__ == "__main__":
    unittest.main()
