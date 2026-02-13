import os
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from sales_agent.sales_core.config import get_settings, project_root

    HAS_CONFIG_DEPS = True
except ModuleNotFoundError:
    HAS_CONFIG_DEPS = False


@unittest.skipUnless(HAS_CONFIG_DEPS, "config dependencies are not installed")
class ConfigTests(unittest.TestCase):
    def test_project_root_points_to_repository_root(self) -> None:
        root = project_root()
        self.assertTrue((root / "README.md").exists())
        self.assertTrue((root / "sales_agent").exists())

    @patch.dict(
        os.environ,
        {
            "TELEGRAM_BOT_TOKEN": "token-123",
            "OPENAI_API_KEY": "sk-test",
            "OPENAI_MODEL": "gpt-4.1-mini",
            "TALLANTO_API_URL": "https://crm.example/api",
            "TALLANTO_API_KEY": "crm-key",
            "BRAND_DEFAULT": "foton",
            "DATABASE_PATH": "/tmp/custom_sales_agent.db",
            "CATALOG_PATH": "/tmp/custom_products.yaml",
            "KNOWLEDGE_PATH": "/tmp/custom_knowledge",
            "VECTOR_STORE_META_PATH": "/tmp/custom_vector_store.json",
            "OPENAI_VECTOR_STORE_ID": "vs_test_123",
            "ADMIN_USER": "admin",
            "ADMIN_PASS": "secret",
        },
        clear=True,
    )
    def test_get_settings_reads_environment_values(self) -> None:
        settings = get_settings()
        self.assertEqual(settings.telegram_bot_token, "token-123")
        self.assertEqual(settings.openai_api_key, "sk-test")
        self.assertEqual(settings.openai_model, "gpt-4.1-mini")
        self.assertEqual(settings.tallanto_api_url, "https://crm.example/api")
        self.assertEqual(settings.tallanto_api_key, "crm-key")
        self.assertEqual(settings.brand_default, "foton")
        self.assertEqual(settings.database_path, Path("/tmp/custom_sales_agent.db"))
        self.assertEqual(settings.catalog_path, Path("/tmp/custom_products.yaml"))
        self.assertEqual(settings.knowledge_path, Path("/tmp/custom_knowledge"))
        self.assertEqual(settings.vector_store_meta_path, Path("/tmp/custom_vector_store.json"))
        self.assertEqual(settings.openai_vector_store_id, "vs_test_123")
        self.assertEqual(settings.admin_user, "admin")
        self.assertEqual(settings.admin_pass, "secret")

    @patch.dict(os.environ, {}, clear=True)
    def test_get_settings_uses_defaults(self) -> None:
        settings = get_settings()
        root = project_root()
        self.assertEqual(settings.brand_default, "kmipt")
        self.assertEqual(settings.openai_model, "gpt-4.1")
        self.assertEqual(settings.database_path, root / "data" / "sales_agent.db")
        self.assertEqual(settings.catalog_path, root / "catalog" / "products.yaml")
        self.assertEqual(settings.knowledge_path, root / "knowledge")
        self.assertEqual(settings.vector_store_meta_path, root / "data" / "vector_store.json")
        self.assertEqual(settings.openai_vector_store_id, "")
        self.assertEqual(settings.admin_user, "")
        self.assertEqual(settings.admin_pass, "")

    @patch.dict(
        os.environ,
        {"DATABASE_PATH": "", "CATALOG_PATH": "", "KNOWLEDGE_PATH": "", "VECTOR_STORE_META_PATH": ""},
        clear=True,
    )
    def test_empty_optional_paths_fallback_to_defaults(self) -> None:
        settings = get_settings()
        root = project_root()
        self.assertEqual(settings.database_path, root / "data" / "sales_agent.db")
        self.assertEqual(settings.catalog_path, root / "catalog" / "products.yaml")
        self.assertEqual(settings.knowledge_path, root / "knowledge")
        self.assertEqual(settings.vector_store_meta_path, root / "data" / "vector_store.json")


if __name__ == "__main__":
    unittest.main()
