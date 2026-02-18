import tempfile
import unittest
from pathlib import Path
import json

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
        self.assertEqual(runtime["vector_store_id_source"], "env")
        self.assertIn("tallanto_read_only", runtime)
        self.assertIn("tallanto_token_set", runtime)

    def test_diagnostics_warns_when_vector_store_loaded_only_from_meta_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "catalog.yaml"
            _write_catalog(catalog_path)
            knowledge_path = root / "knowledge"
            knowledge_path.mkdir(parents=True, exist_ok=True)
            (knowledge_path / "faq_general.md").write_text("FAQ", encoding="utf-8")
            db_path = root / "data" / "sales_agent.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            vector_meta_path = root / "data" / "vector_store.json"
            vector_meta_path.parent.mkdir(parents=True, exist_ok=True)
            vector_meta_path.write_text(json.dumps({"vector_store_id": "vs_meta_123"}), encoding="utf-8")

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
                vector_store_meta_path=vector_meta_path,
                openai_vector_store_id="",
                admin_user="",
                admin_pass="",
            )
            diagnostics = build_runtime_diagnostics(settings)

        self.assertEqual(diagnostics["status"], "warn")
        runtime = diagnostics["runtime"]
        self.assertTrue(runtime["vector_store_id_set"])
        self.assertEqual(runtime["vector_store_id_source"], "meta_file")
        issue_codes = {item["code"] for item in diagnostics["issues"]}
        self.assertIn("vector_store_env_recommended", issue_codes)

    def test_diagnostics_warns_when_tallanto_readonly_missing_token(self) -> None:
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
                tallanto_read_only=True,
                tallanto_api_token="",
            )
            diagnostics = build_runtime_diagnostics(settings)

        self.assertEqual(diagnostics["status"], "warn")
        issue_codes = {item["code"] for item in diagnostics["issues"]}
        self.assertIn("tallanto_readonly_incomplete", issue_codes)


if __name__ == "__main__":
    unittest.main()
