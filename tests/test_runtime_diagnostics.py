import tempfile
import unittest
from pathlib import Path
import json
from unittest.mock import patch

from sales_agent.sales_core.config import Settings
from sales_agent.sales_core.runtime_diagnostics import (
    _can_write_parent,
    _summarize_issues,
    enforce_startup_preflight,
    _is_path_within,
    _safe_md_count,
    build_runtime_diagnostics,
    normalize_preflight_mode,
)


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
    def test_summarize_issues_handles_empty_and_missing_message(self) -> None:
        self.assertEqual(_summarize_issues([]), "no issues reported")
        summary = _summarize_issues([{"code": "only_code"}], limit=3)
        self.assertEqual(summary, "only_code")

    def test_normalize_preflight_mode(self) -> None:
        self.assertEqual(normalize_preflight_mode("off"), "off")
        self.assertEqual(normalize_preflight_mode("FAIL"), "fail")
        self.assertEqual(normalize_preflight_mode("strict"), "strict")
        self.assertEqual(normalize_preflight_mode("  unknown "), "off")
        self.assertEqual(normalize_preflight_mode(None), "off")

    def test_can_write_parent_returns_false_when_probe_write_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "data" / "sales_agent.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            with patch("pathlib.Path.write_text", side_effect=OSError("read only fs")):
                self.assertFalse(_can_write_parent(db_path))

    def test_is_path_within_handles_resolve_error(self) -> None:
        with patch("pathlib.Path.resolve", side_effect=RuntimeError("resolve failure")):
            self.assertFalse(_is_path_within(Path("/tmp/a"), Path("/tmp")))

    def test_is_path_within_fallback_for_legacy_paths(self) -> None:
        class LegacyResolved:
            def __init__(self, value: str) -> None:
                self.value = value

            def relative_to(self, other: "LegacyResolved"):
                if self.value == other.value or self.value.startswith(other.value.rstrip("/") + "/"):
                    return self.value
                raise ValueError

        class LegacyPath:
            def __init__(self, value: str) -> None:
                self.value = value

            def resolve(self) -> LegacyResolved:
                return LegacyResolved(self.value)

        self.assertTrue(_is_path_within(LegacyPath("/srv/app/data"), LegacyPath("/srv/app")))
        self.assertFalse(_is_path_within(LegacyPath("/srv/other"), LegacyPath("/srv/app")))

    def test_helper_path_within_and_md_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            docs = root / "docs"
            docs.mkdir(parents=True, exist_ok=True)
            (docs / "a.md").write_text("x", encoding="utf-8")
            (docs / "b.txt").write_text("x", encoding="utf-8")
            (docs / "c.pdf").write_text("x", encoding="utf-8")
            (docs / "d.json").write_text("x", encoding="utf-8")

            self.assertEqual(_safe_md_count(docs), 3)
            self.assertTrue(_is_path_within(docs / "a.md", docs))
            self.assertFalse(_is_path_within(root / "other.md", docs))

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

    def test_diagnostics_warns_for_render_non_persistent_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "catalog.yaml"
            _write_catalog(catalog_path)
            knowledge_path = root / "knowledge"
            knowledge_path.mkdir(parents=True, exist_ok=True)
            (knowledge_path / "faq_general.md").write_text("FAQ", encoding="utf-8")

            settings = Settings(
                telegram_bot_token="tg-token",
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
                running_on_render=True,
                persistent_data_root=Path("/var/data"),
            )
            settings.database_path.parent.mkdir(parents=True, exist_ok=True)
            diagnostics = build_runtime_diagnostics(settings)

        self.assertEqual(diagnostics["status"], "warn")
        issue_codes = {item["code"] for item in diagnostics["issues"]}
        self.assertIn("render_database_not_persistent", issue_codes)
        self.assertIn("render_vector_meta_not_persistent", issue_codes)

    def test_diagnostics_reports_render_paths_ok_when_under_persistent_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "catalog.yaml"
            _write_catalog(catalog_path)
            knowledge_path = root / "knowledge"
            knowledge_path.mkdir(parents=True, exist_ok=True)
            (knowledge_path / "faq_general.md").write_text("FAQ", encoding="utf-8")

            persistent = root / "persistent"
            persistent.mkdir(parents=True, exist_ok=True)
            settings = Settings(
                telegram_bot_token="tg-token",
                openai_api_key="sk-test",
                openai_model="gpt-4.1",
                tallanto_api_url="",
                tallanto_api_key="",
                brand_default="kmipt",
                database_path=persistent / "sales_agent.db",
                catalog_path=catalog_path,
                knowledge_path=knowledge_path,
                vector_store_meta_path=persistent / "vector_store.json",
                openai_vector_store_id="vs_123",
                admin_user="",
                admin_pass="",
                running_on_render=True,
                persistent_data_root=persistent,
            )
            diagnostics = build_runtime_diagnostics(settings)

        self.assertEqual(diagnostics["status"], "ok")
        runtime = diagnostics["runtime"]
        self.assertTrue(runtime["database_on_persistent_storage"])
        self.assertTrue(runtime["vector_meta_on_persistent_storage"])

    def test_diagnostics_warns_when_render_uses_tmp_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "catalog.yaml"
            _write_catalog(catalog_path)
            knowledge_path = root / "knowledge"
            knowledge_path.mkdir(parents=True, exist_ok=True)
            (knowledge_path / "faq_general.md").write_text("FAQ", encoding="utf-8")

            db_path = Path("/tmp/sales_agent_test_runtime.db")
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
                vector_store_meta_path=Path("/tmp/vector_store_test_runtime.json"),
                openai_vector_store_id="vs_123",
                admin_user="",
                admin_pass="",
                running_on_render=True,
                persistent_data_root=Path("/tmp"),
            )
            diagnostics = build_runtime_diagnostics(settings)

        self.assertEqual(diagnostics["status"], "warn")
        issue_codes = {item["code"] for item in diagnostics["issues"]}
        self.assertIn("render_ephemeral_storage_fallback", issue_codes)
        runtime = diagnostics["runtime"]
        self.assertFalse(runtime["database_on_persistent_storage"])
        self.assertFalse(runtime["vector_meta_on_persistent_storage"])

    def test_diagnostics_warns_for_render_without_persistent_root(self) -> None:
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
                running_on_render=True,
                persistent_data_root=Path(),
            )
            diagnostics = build_runtime_diagnostics(settings)

        self.assertEqual(diagnostics["status"], "warn")
        issue_codes = {item["code"] for item in diagnostics["issues"]}
        self.assertIn("persistent_data_root_missing", issue_codes)

    def test_enforce_startup_preflight_blocks_fail_status(self) -> None:
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
                startup_preflight_mode="fail",
            )
            settings.database_path.parent.mkdir(parents=True, exist_ok=True)
            with self.assertRaises(RuntimeError):
                enforce_startup_preflight(settings)

    def test_enforce_startup_preflight_blocks_warn_in_strict_mode(self) -> None:
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
                openai_vector_store_id="",
                admin_user="",
                admin_pass="",
                startup_preflight_mode="strict",
            )
            with self.assertRaises(RuntimeError):
                enforce_startup_preflight(settings)

    def test_enforce_startup_preflight_returns_off_mode_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = Settings(
                telegram_bot_token="",
                openai_api_key="",
                openai_model="gpt-4.1",
                tallanto_api_url="",
                tallanto_api_key="",
                brand_default="kmipt",
                database_path=root / "data" / "sales_agent.db",
                catalog_path=root / "catalog.yaml",
                knowledge_path=root / "knowledge",
                vector_store_meta_path=root / "data" / "vector_store.json",
                openai_vector_store_id="",
                admin_user="",
                admin_pass="",
                startup_preflight_mode="off",
            )
            payload = enforce_startup_preflight(settings)

        self.assertEqual(payload["status"], "off")
        self.assertEqual(payload["issues"], [])

    def test_enforce_startup_preflight_allows_warn_in_fail_mode(self) -> None:
        diagnostics = {"status": "warn", "runtime": {"x": 1}, "issues": [{"code": "warn"}]}
        with patch(
            "sales_agent.sales_core.runtime_diagnostics.build_runtime_diagnostics",
            return_value=diagnostics,
        ):
            result = enforce_startup_preflight(Settings(
                telegram_bot_token="tg",
                openai_api_key="openai",
                openai_model="gpt-4.1",
                tallanto_api_url="",
                tallanto_api_key="",
                brand_default="kmipt",
                database_path=Path("/tmp/db.sqlite"),
                catalog_path=Path("/tmp/catalog.yaml"),
                knowledge_path=Path("/tmp/knowledge"),
                vector_store_meta_path=Path("/tmp/vector.json"),
                openai_vector_store_id="",
                admin_user="",
                admin_pass="",
                startup_preflight_mode="fail",
            ))

        self.assertEqual(result["status"], "warn")


if __name__ == "__main__":
    unittest.main()
