import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from sales_agent.sales_core.config import Settings
from sales_agent.sales_core.crm import AmoCRMClient, NoopCRMClient, TallantoCRMClient, build_crm_client


class CRMFactoryTests(unittest.TestCase):
    def _settings(self, **overrides) -> Settings:
        payload = {
            "telegram_bot_token": "",
            "openai_api_key": "",
            "openai_model": "gpt-4.1",
            "tallanto_api_url": "",
            "tallanto_api_key": "",
            "brand_default": "kmipt",
            "database_path": Path("data/sales_agent.db"),
            "catalog_path": Path("catalog/products.yaml"),
            "knowledge_path": Path("knowledge"),
            "vector_store_meta_path": Path("data/vector_store.json"),
            "openai_vector_store_id": "",
            "admin_user": "",
            "admin_pass": "",
            "crm_provider": "tallanto",
            "amo_api_url": "",
            "amo_access_token": "",
        }
        payload.update(overrides)
        return Settings(**payload)

    def test_build_crm_client_tallanto_provider(self) -> None:
        settings = self._settings(crm_provider="tallanto")
        with patch("sales_agent.sales_core.crm.TallantoClient.from_settings", return_value=MagicMock()) as mock_from:
            client = build_crm_client(settings)
        self.assertIsInstance(client, TallantoCRMClient)
        mock_from.assert_called_once_with(settings)

    def test_build_crm_client_amo_provider(self) -> None:
        settings = self._settings(crm_provider="amo", amo_api_url="https://amo.example", amo_access_token="token")
        client = build_crm_client(settings)
        self.assertIsInstance(client, AmoCRMClient)
        self.assertEqual(client.base_url, "https://amo.example")

    def test_build_crm_client_none_provider(self) -> None:
        settings = self._settings(crm_provider="none")
        client = build_crm_client(settings)
        self.assertIsInstance(client, NoopCRMClient)

    def test_build_crm_client_unknown_provider_falls_back_to_noop(self) -> None:
        settings = self._settings(crm_provider="custom")
        client = build_crm_client(settings)
        self.assertIsInstance(client, NoopCRMClient)
        self.assertIn("Unsupported CRM_PROVIDER", client.reason)


class CRMClientBehaviorTests(unittest.IsolatedAsyncioTestCase):
    async def test_tallanto_adapter_create_lead_async_maps_result(self) -> None:
        tallanto = SimpleNamespace(
            create_lead_async=AsyncMock(
                return_value=SimpleNamespace(
                    success=True,
                    entry_id="lead-1",
                    raw={"id": "lead-1"},
                    error=None,
                )
            )
        )
        client = TallantoCRMClient(tallanto)
        result = await client.create_lead_async(phone="+79990000000", brand="kmipt")
        self.assertTrue(result.success)
        self.assertEqual(result.entry_id, "lead-1")
        self.assertEqual(result.raw, {"id": "lead-1"})

    def test_tallanto_adapter_create_copilot_task_maps_result(self) -> None:
        tallanto = SimpleNamespace(
            set_entry=MagicMock(
                return_value=SimpleNamespace(
                    success=True,
                    entry_id="task-7",
                    raw={"id": "task-7"},
                    error=None,
                )
            )
        )
        client = TallantoCRMClient(tallanto)
        result = client.create_copilot_task(summary="sum", draft_reply="draft")
        self.assertTrue(result.success)
        self.assertEqual(result.entry_id, "task-7")

    async def test_amo_client_returns_config_error_when_not_configured(self) -> None:
        client = AmoCRMClient(base_url="", access_token="")
        result = await client.create_lead_async(phone="+79990000000", brand="kmipt")
        self.assertFalse(result.success)
        self.assertIn("not configured", (result.error or "").lower())

    async def test_noop_client_returns_disabled_error(self) -> None:
        client = NoopCRMClient()
        result = await client.create_lead_async(phone="+79990000000", brand="kmipt")
        self.assertFalse(result.success)
        self.assertIn("disabled", (result.error or "").lower())


if __name__ == "__main__":
    unittest.main()
