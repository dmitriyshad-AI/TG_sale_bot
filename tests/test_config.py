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
            "MINIAPP_BRAND_NAME": "УНПК МФТИ",
            "MINIAPP_ADVISOR_NAME": "Гид",
            "SALES_MANAGER_LABEL": "Старший менеджер",
            "SALES_MANAGER_CHAT_URL": "https://t.me/kmipt_sales_manager",
            "APP_ENV": "production",
            "STARTUP_PREFLIGHT_MODE": "strict",
            "RATE_LIMIT_BACKEND": "redis",
            "REDIS_URL": "redis://localhost:6379/0",
            "ADMIN_UI_CSRF_ENABLED": "true",
            "ASSISTANT_API_TOKEN": "assist-token",
            "ASSISTANT_RATE_LIMIT_WINDOW_SECONDS": "90",
            "ASSISTANT_RATE_LIMIT_USER_REQUESTS": "33",
            "ASSISTANT_RATE_LIMIT_IP_REQUESTS": "88",
            "CRM_API_EXPOSED": "true",
            "CRM_RATE_LIMIT_WINDOW_SECONDS": "420",
            "CRM_RATE_LIMIT_IP_REQUESTS": "222",
            "ENABLE_BUSINESS_INBOX": "true",
            "ENABLE_CALL_COPILOT": "1",
            "ENABLE_TALLANTO_ENRICHMENT": "yes",
            "ENABLE_DIRECTOR_AGENT": "on",
            "ENABLE_LEAD_RADAR": "true",
            "ENABLE_FAQ_LAB": "true",
            "ENABLE_MANGO_AUTO_INGEST": "yes",
            "LEAD_RADAR_SCHEDULER_ENABLED": "false",
            "LEAD_RADAR_INTERVAL_SECONDS": "1200",
            "LEAD_RADAR_NO_REPLY_HOURS": "8",
            "LEAD_RADAR_CALL_NO_NEXT_STEP_HOURS": "30",
            "LEAD_RADAR_STALE_WARM_DAYS": "10",
            "LEAD_RADAR_MAX_ITEMS_PER_RUN": "77",
            "FAQ_LAB_SCHEDULER_ENABLED": "false",
            "FAQ_LAB_INTERVAL_SECONDS": "7200",
            "FAQ_LAB_WINDOW_DAYS": "45",
            "FAQ_LAB_MIN_QUESTION_COUNT": "3",
            "FAQ_LAB_MAX_ITEMS_PER_RUN": "222",
            "MANGO_API_BASE_URL": "https://mango.example/api",
            "MANGO_API_TOKEN": "mango-token",
            "MANGO_CALLS_PATH": "vpbx/calls",
            "MANGO_WEBHOOK_PATH": "hooks/mango",
            "MANGO_WEBHOOK_SECRET": "mango-secret",
            "MANGO_POLLING_ENABLED": "1",
            "MANGO_POLL_INTERVAL_SECONDS": "180",
            "MANGO_CALL_RECORDING_TTL_HOURS": "72",
            "MANGO_POLL_LIMIT_PER_RUN": "88",
            "MANGO_POLL_RETRY_ATTEMPTS": "4",
            "MANGO_POLL_RETRY_BACKOFF_SECONDS": "3",
            "MANGO_RETRY_FAILED_LIMIT_PER_RUN": "66",
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
        self.assertEqual(settings.miniapp_brand_name, "УНПК МФТИ")
        self.assertEqual(settings.miniapp_advisor_name, "Гид")
        self.assertEqual(settings.sales_manager_label, "Старший менеджер")
        self.assertEqual(settings.sales_manager_chat_url, "https://t.me/kmipt_sales_manager")
        self.assertEqual(settings.app_env, "production")
        self.assertEqual(settings.startup_preflight_mode, "strict")
        self.assertEqual(settings.rate_limit_backend, "redis")
        self.assertEqual(settings.redis_url, "redis://localhost:6379/0")
        self.assertTrue(settings.admin_ui_csrf_enabled)
        self.assertEqual(settings.assistant_api_token, "assist-token")
        self.assertEqual(settings.assistant_rate_limit_window_seconds, 90)
        self.assertEqual(settings.assistant_rate_limit_user_requests, 33)
        self.assertEqual(settings.assistant_rate_limit_ip_requests, 88)
        self.assertTrue(settings.crm_api_exposed)
        self.assertEqual(settings.crm_rate_limit_window_seconds, 420)
        self.assertEqual(settings.crm_rate_limit_ip_requests, 222)
        self.assertTrue(settings.enable_business_inbox)
        self.assertTrue(settings.enable_call_copilot)
        self.assertTrue(settings.enable_tallanto_enrichment)
        self.assertTrue(settings.enable_director_agent)
        self.assertTrue(settings.enable_lead_radar)
        self.assertTrue(settings.enable_faq_lab)
        self.assertTrue(settings.enable_mango_auto_ingest)
        self.assertFalse(settings.lead_radar_scheduler_enabled)
        self.assertEqual(settings.lead_radar_interval_seconds, 1200)
        self.assertEqual(settings.lead_radar_no_reply_hours, 8)
        self.assertEqual(settings.lead_radar_call_no_next_step_hours, 30)
        self.assertEqual(settings.lead_radar_stale_warm_days, 10)
        self.assertEqual(settings.lead_radar_max_items_per_run, 77)
        self.assertFalse(settings.faq_lab_scheduler_enabled)
        self.assertEqual(settings.faq_lab_interval_seconds, 7200)
        self.assertEqual(settings.faq_lab_window_days, 45)
        self.assertEqual(settings.faq_lab_min_question_count, 3)
        self.assertEqual(settings.faq_lab_max_items_per_run, 222)
        self.assertEqual(settings.mango_api_base_url, "https://mango.example/api")
        self.assertEqual(settings.mango_api_token, "mango-token")
        self.assertEqual(settings.mango_calls_path, "/vpbx/calls")
        self.assertEqual(settings.mango_webhook_path, "/hooks/mango")
        self.assertEqual(settings.mango_webhook_secret, "mango-secret")
        self.assertTrue(settings.mango_polling_enabled)
        self.assertEqual(settings.mango_poll_interval_seconds, 180)
        self.assertEqual(settings.mango_call_recording_ttl_hours, 72)
        self.assertEqual(settings.mango_poll_limit_per_run, 88)
        self.assertEqual(settings.mango_poll_retry_attempts, 4)
        self.assertEqual(settings.mango_poll_retry_backoff_seconds, 3)
        self.assertEqual(settings.mango_retry_failed_limit_per_run, 66)

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
        self.assertEqual(settings.crm_provider, "none")
        self.assertEqual(settings.amo_api_url, "")
        self.assertEqual(settings.amo_access_token, "")
        self.assertEqual(settings.miniapp_brand_name, "УНПК МФТИ")
        self.assertEqual(settings.miniapp_advisor_name, "Гид")
        self.assertEqual(settings.sales_manager_label, "Менеджер")
        self.assertEqual(settings.sales_manager_chat_url, "")
        self.assertEqual(settings.app_env, "development")
        self.assertEqual(settings.startup_preflight_mode, "fail")
        self.assertEqual(settings.rate_limit_backend, "memory")
        self.assertEqual(settings.redis_url, "")
        self.assertFalse(settings.admin_ui_csrf_enabled)
        self.assertEqual(settings.assistant_api_token, "")
        self.assertEqual(settings.assistant_rate_limit_window_seconds, 60)
        self.assertEqual(settings.assistant_rate_limit_user_requests, 24)
        self.assertEqual(settings.assistant_rate_limit_ip_requests, 72)
        self.assertFalse(settings.crm_api_exposed)
        self.assertEqual(settings.crm_rate_limit_window_seconds, 300)
        self.assertEqual(settings.crm_rate_limit_ip_requests, 180)
        self.assertEqual(settings.tallanto_api_token, "")
        self.assertFalse(settings.tallanto_read_only)
        self.assertEqual(settings.tallanto_default_contact_module, "")
        self.assertFalse(settings.running_on_render)
        self.assertEqual(settings.persistent_data_root, Path())
        self.assertFalse(settings.enable_business_inbox)
        self.assertFalse(settings.enable_call_copilot)
        self.assertFalse(settings.enable_tallanto_enrichment)
        self.assertFalse(settings.enable_director_agent)
        self.assertFalse(settings.enable_lead_radar)
        self.assertFalse(settings.enable_faq_lab)
        self.assertFalse(settings.enable_mango_auto_ingest)
        self.assertTrue(settings.lead_radar_scheduler_enabled)
        self.assertEqual(settings.lead_radar_interval_seconds, 3600)
        self.assertEqual(settings.lead_radar_no_reply_hours, 6)
        self.assertEqual(settings.lead_radar_call_no_next_step_hours, 24)
        self.assertEqual(settings.lead_radar_stale_warm_days, 7)
        self.assertEqual(settings.lead_radar_max_items_per_run, 50)
        self.assertTrue(settings.faq_lab_scheduler_enabled)
        self.assertEqual(settings.faq_lab_interval_seconds, 21600)
        self.assertEqual(settings.faq_lab_window_days, 90)
        self.assertEqual(settings.faq_lab_min_question_count, 2)
        self.assertEqual(settings.faq_lab_max_items_per_run, 120)
        self.assertEqual(settings.mango_api_base_url, "")
        self.assertEqual(settings.mango_api_token, "")
        self.assertEqual(settings.mango_calls_path, "/calls")
        self.assertEqual(settings.mango_webhook_path, "/integrations/mango/webhook")
        self.assertEqual(settings.mango_webhook_secret, "")
        self.assertFalse(settings.mango_polling_enabled)
        self.assertEqual(settings.mango_poll_interval_seconds, 300)
        self.assertEqual(settings.mango_call_recording_ttl_hours, 48)
        self.assertEqual(settings.mango_poll_limit_per_run, 50)
        self.assertEqual(settings.mango_poll_retry_attempts, 3)
        self.assertEqual(settings.mango_poll_retry_backoff_seconds, 2)
        self.assertEqual(settings.mango_retry_failed_limit_per_run, 25)

    @patch.dict(
        os.environ,
        {
            "ASSISTANT_RATE_LIMIT_WINDOW_SECONDS": "bad",
            "ASSISTANT_RATE_LIMIT_USER_REQUESTS": "-1",
            "ASSISTANT_RATE_LIMIT_IP_REQUESTS": "10000",
            "CRM_RATE_LIMIT_WINDOW_SECONDS": "5",
            "CRM_RATE_LIMIT_IP_REQUESTS": "0",
            "LEAD_RADAR_INTERVAL_SECONDS": "5",
            "LEAD_RADAR_NO_REPLY_HOURS": "999",
            "LEAD_RADAR_CALL_NO_NEXT_STEP_HOURS": "-2",
            "LEAD_RADAR_STALE_WARM_DAYS": "9999",
            "LEAD_RADAR_MAX_ITEMS_PER_RUN": "0",
            "FAQ_LAB_INTERVAL_SECONDS": "20",
            "FAQ_LAB_WINDOW_DAYS": "999",
            "FAQ_LAB_MIN_QUESTION_COUNT": "0",
            "FAQ_LAB_MAX_ITEMS_PER_RUN": "5000",
            "MANGO_POLL_INTERVAL_SECONDS": "-1",
            "MANGO_CALL_RECORDING_TTL_HOURS": "999999",
            "MANGO_POLL_LIMIT_PER_RUN": "0",
            "MANGO_POLL_RETRY_ATTEMPTS": "999",
            "MANGO_POLL_RETRY_BACKOFF_SECONDS": "-2",
            "MANGO_RETRY_FAILED_LIMIT_PER_RUN": "0",
        },
        clear=True,
    )
    def test_rate_limit_env_values_are_sanitized(self) -> None:
        settings = get_settings()
        self.assertEqual(settings.assistant_rate_limit_window_seconds, 60)
        self.assertEqual(settings.assistant_rate_limit_user_requests, 1)
        self.assertEqual(settings.assistant_rate_limit_ip_requests, 5000)
        self.assertEqual(settings.crm_rate_limit_window_seconds, 30)
        self.assertEqual(settings.crm_rate_limit_ip_requests, 1)
        self.assertEqual(settings.lead_radar_interval_seconds, 60)
        self.assertEqual(settings.lead_radar_no_reply_hours, 168)
        self.assertEqual(settings.lead_radar_call_no_next_step_hours, 1)
        self.assertEqual(settings.lead_radar_stale_warm_days, 180)
        self.assertEqual(settings.lead_radar_max_items_per_run, 1)
        self.assertEqual(settings.faq_lab_interval_seconds, 300)
        self.assertEqual(settings.faq_lab_window_days, 365)
        self.assertEqual(settings.faq_lab_min_question_count, 1)
        self.assertEqual(settings.faq_lab_max_items_per_run, 1000)
        self.assertEqual(settings.mango_poll_interval_seconds, 30)
        self.assertEqual(settings.mango_call_recording_ttl_hours, 2160)
        self.assertEqual(settings.mango_poll_limit_per_run, 1)
        self.assertEqual(settings.mango_poll_retry_attempts, 10)
        self.assertEqual(settings.mango_poll_retry_backoff_seconds, 0)
        self.assertEqual(settings.mango_retry_failed_limit_per_run, 1)

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

    @patch.dict(os.environ, {"STARTUP_PREFLIGHT_MODE": "invalid"}, clear=True)
    def test_invalid_preflight_mode_falls_back_to_fail(self) -> None:
        settings = get_settings()
        self.assertEqual(settings.startup_preflight_mode, "fail")

    @patch.dict(
        os.environ,
        {"APP_ENV": "invalid", "RATE_LIMIT_BACKEND": "bad-backend", "ADMIN_UI_CSRF_ENABLED": "0"},
        clear=True,
    )
    def test_invalid_app_env_and_rate_backend_fall_back_to_defaults(self) -> None:
        settings = get_settings()
        self.assertEqual(settings.app_env, "development")
        self.assertEqual(settings.rate_limit_backend, "memory")
        self.assertFalse(settings.admin_ui_csrf_enabled)

    @patch.dict(os.environ, {"APP_ENV": "production"}, clear=True)
    def test_admin_ui_csrf_enabled_by_default_in_production(self) -> None:
        settings = get_settings()
        self.assertEqual(settings.app_env, "production")
        self.assertTrue(settings.admin_ui_csrf_enabled)

    @patch.dict(os.environ, {"TELEGRAM_MODE": "webhook", "TELEGRAM_WEBHOOK_SECRET": ""}, clear=True)
    def test_webhook_mode_allows_empty_secret(self) -> None:
        settings = get_settings()
        self.assertEqual(settings.telegram_mode, "webhook")
        self.assertEqual(settings.telegram_webhook_secret, "")

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

    @patch.dict(os.environ, {"RENDER": "true"}, clear=True)
    @patch("sales_agent.sales_core.config._path_is_writable_directory", return_value=True)
    def test_render_defaults_use_var_data_when_writable(self, _mock_writable: object) -> None:
        settings = get_settings()
        self.assertTrue(settings.running_on_render)
        self.assertEqual(settings.persistent_data_root, Path("/var/data"))
        self.assertEqual(settings.database_path, Path("/var/data/sales_agent.db"))
        self.assertEqual(settings.vector_store_meta_path, Path("/var/data/vector_store.json"))

    @patch.dict(os.environ, {"RENDER": "true"}, clear=True)
    @patch("sales_agent.sales_core.config._path_is_writable_directory", return_value=False)
    def test_render_defaults_fallback_to_tmp_when_var_data_unavailable(self, _mock_writable: object) -> None:
        settings = get_settings()
        self.assertTrue(settings.running_on_render)
        self.assertEqual(settings.persistent_data_root, Path("/tmp"))
        self.assertEqual(settings.database_path, Path("/tmp/sales_agent.db"))
        self.assertEqual(settings.vector_store_meta_path, Path("/tmp/vector_store.json"))

    @patch.dict(os.environ, {"PERSISTENT_DATA_PATH": "/tmp/persistent-sales-data"}, clear=True)
    def test_explicit_persistent_data_path_overrides_defaults(self) -> None:
        settings = get_settings()
        self.assertFalse(settings.running_on_render)
        self.assertEqual(settings.persistent_data_root, Path("/tmp/persistent-sales-data"))
        self.assertEqual(settings.database_path, Path("/tmp/persistent-sales-data/sales_agent.db"))
        self.assertEqual(settings.vector_store_meta_path, Path("/tmp/persistent-sales-data/vector_store.json"))

    @patch.dict(
        os.environ,
        {"RENDER": "true", "RENDER_DISK_MOUNT_PATH": "/tmp/render-disk"},
        clear=True,
    )
    def test_render_disk_mount_path_is_used_when_provided(self) -> None:
        settings = get_settings()
        self.assertTrue(settings.running_on_render)
        self.assertEqual(settings.persistent_data_root, Path("/tmp/render-disk"))
        self.assertEqual(settings.database_path, Path("/tmp/render-disk/sales_agent.db"))
        self.assertEqual(settings.vector_store_meta_path, Path("/tmp/render-disk/vector_store.json"))


if __name__ == "__main__":
    unittest.main()
