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
            "TELEGRAM_MODE": "webhook",
            "TELEGRAM_WEBHOOK_SECRET": "wh-secret",
            "TELEGRAM_WEBHOOK_PATH": "tg/webhook",
            "ADMIN_MINIAPP_ENABLED": "true",
            "ADMIN_TELEGRAM_IDS": "101,  202,broken,",
            "ADMIN_WEBAPP_URL": "https://example.com/admin/miniapp",
            "OPENAI_API_KEY": "sk-test",
            "OPENAI_MODEL": "gpt-4.1-mini",
            "TALLANTO_API_URL": "https://crm.example/api",
            "TALLANTO_API_KEY": "crm-key",
            "TALLANTO_API_TOKEN": "crm-token",
            "TALLANTO_READ_ONLY": "1",
            "TALLANTO_DEFAULT_CONTACT_MODULE": "contacts",
            "OPENAI_WEB_FALLBACK_ENABLED": "false",
            "OPENAI_WEB_FALLBACK_DOMAIN": "example.edu",
            "BRAND_DEFAULT": "foton",
            "DATABASE_PATH": "/tmp/custom_sales_agent.db",
            "CATALOG_PATH": "/tmp/custom_products.yaml",
            "KNOWLEDGE_PATH": "/tmp/custom_knowledge",
            "VECTOR_STORE_META_PATH": "/tmp/custom_vector_store.json",
            "WEBAPP_DIST_PATH": "/tmp/custom_webapp_dist",
            "OPENAI_VECTOR_STORE_ID": "vs_test_123",
            "ADMIN_USER": "admin",
            "ADMIN_PASS": "secret",
            "CRM_PROVIDER": "amo",
            "AMO_API_URL": "https://amo.example/api",
            "AMO_ACCESS_TOKEN": "amo-token",
        },
        clear=True,
    )
    def test_get_settings_reads_environment_values(self) -> None:
        settings = get_settings()
        self.assertEqual(settings.telegram_bot_token, "token-123")
        self.assertEqual(settings.telegram_mode, "webhook")
        self.assertEqual(settings.telegram_webhook_secret, "wh-secret")
        self.assertEqual(settings.telegram_webhook_path, "/tg/webhook")
        self.assertTrue(settings.admin_miniapp_enabled)
        self.assertEqual(settings.admin_telegram_ids, (101, 202))
        self.assertEqual(settings.admin_webapp_url, "https://example.com/admin/miniapp")
        self.assertEqual(settings.openai_api_key, "sk-test")
        self.assertEqual(settings.openai_model, "gpt-4.1-mini")
        self.assertFalse(settings.openai_web_fallback_enabled)
        self.assertEqual(settings.openai_web_fallback_domain, "example.edu")
        self.assertEqual(settings.tallanto_api_url, "https://crm.example/api")
        self.assertEqual(settings.tallanto_api_key, "crm-key")
        self.assertEqual(settings.tallanto_api_token, "crm-token")
        self.assertTrue(settings.tallanto_read_only)
        self.assertEqual(settings.tallanto_default_contact_module, "contacts")
        self.assertEqual(settings.brand_default, "foton")
        self.assertEqual(settings.database_path, Path("/tmp/custom_sales_agent.db"))
        self.assertEqual(settings.catalog_path, Path("/tmp/custom_products.yaml"))
        self.assertEqual(settings.knowledge_path, Path("/tmp/custom_knowledge"))
        self.assertEqual(settings.vector_store_meta_path, Path("/tmp/custom_vector_store.json"))
        self.assertEqual(settings.webapp_dist_path, Path("/tmp/custom_webapp_dist"))
        self.assertEqual(settings.openai_vector_store_id, "vs_test_123")
        self.assertEqual(settings.admin_user, "admin")
        self.assertEqual(settings.admin_pass, "secret")
        self.assertEqual(settings.crm_provider, "amo")
        self.assertEqual(settings.amo_api_url, "https://amo.example/api")
        self.assertEqual(settings.amo_access_token, "amo-token")

    @patch.dict(os.environ, {}, clear=True)
    def test_get_settings_uses_defaults(self) -> None:
        settings = get_settings()
        root = project_root()
        self.assertEqual(settings.brand_default, "kmipt")
        self.assertEqual(settings.telegram_mode, "polling")
        self.assertEqual(settings.telegram_webhook_secret, "")
        self.assertEqual(settings.telegram_webhook_path, "/telegram/webhook")
        self.assertFalse(settings.admin_miniapp_enabled)
        self.assertEqual(settings.admin_telegram_ids, ())
        self.assertEqual(settings.admin_webapp_url, "")
        self.assertEqual(settings.openai_model, "gpt-4.1")
        self.assertTrue(settings.openai_web_fallback_enabled)
        self.assertEqual(settings.openai_web_fallback_domain, "kmipt.ru")
        self.assertEqual(settings.database_path, root / "data" / "sales_agent.db")
        self.assertEqual(settings.catalog_path, root / "catalog" / "products.yaml")
        self.assertEqual(settings.knowledge_path, root / "knowledge")
        self.assertEqual(settings.vector_store_meta_path, root / "data" / "vector_store.json")
        self.assertEqual(settings.webapp_dist_path, root / "webapp" / "dist")
        self.assertEqual(settings.openai_vector_store_id, "")
        self.assertEqual(settings.admin_user, "")
        self.assertEqual(settings.admin_pass, "")
        self.assertEqual(settings.crm_provider, "tallanto")
        self.assertEqual(settings.amo_api_url, "")
        self.assertEqual(settings.amo_access_token, "")
        self.assertEqual(settings.tallanto_api_token, "")
        self.assertFalse(settings.tallanto_read_only)
        self.assertEqual(settings.tallanto_default_contact_module, "")

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

    @patch.dict(os.environ, {"TELEGRAM_MODE": "unexpected"}, clear=True)
    def test_invalid_telegram_mode_falls_back_to_polling(self) -> None:
        settings = get_settings()
        self.assertEqual(settings.telegram_mode, "polling")

    @patch.dict(os.environ, {"ADMIN_MINIAPP_ENABLED": "yes", "ADMIN_TELEGRAM_IDS": "1, 2, bad,3"}, clear=True)
    def test_admin_miniapp_settings_parse_values(self) -> None:
        settings = get_settings()
        self.assertTrue(settings.admin_miniapp_enabled)
        self.assertEqual(settings.admin_telegram_ids, (1, 2, 3))

    @patch.dict(
        os.environ,
        {"TALLANTO_API_KEY": "legacy-key", "TALLANTO_READ_ONLY": "true"},
        clear=True,
    )
    def test_tallanto_token_falls_back_to_api_key_and_readonly_requires_literal_one(self) -> None:
        settings = get_settings()
        self.assertEqual(settings.tallanto_api_token, "legacy-key")
        self.assertFalse(settings.tallanto_read_only)


if __name__ == "__main__":
    unittest.main()
