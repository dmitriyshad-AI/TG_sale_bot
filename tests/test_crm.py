import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

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

    async def test_amo_client_create_lead_success(self) -> None:
        client = AmoCRMClient(base_url="https://amo.example", access_token="token")
        with patch.object(
            client,
            "_post_json_async",
            side_effect=[
                {"_embedded": {"leads": [{"id": 12345}]}, "_links": {}},
                {"_embedded": {"notes": [{"id": 777}]}, "_links": {}},
            ],
        ) as post_mock:
            result = await client.create_lead_async(
                phone="+79990000000",
                brand="kmipt",
                name="Alice",
                source="telegram",
                note="Перезвонить завтра",
            )
        self.assertTrue(result.success)
        self.assertEqual(result.entry_id, "12345")
        self.assertEqual(post_mock.call_count, 2)
        first_call_path = post_mock.call_args_list[0].args[0]
        second_call_path = post_mock.call_args_list[1].args[0]
        self.assertEqual(first_call_path, "/leads")
        self.assertEqual(second_call_path, "/leads/12345/notes")

    async def test_amo_client_create_lead_handles_http_error(self) -> None:
        client = AmoCRMClient(base_url="https://amo.example", access_token="token")
        request = httpx.Request("POST", "https://amo.example/api/v4/leads")
        response = httpx.Response(401, request=request, text='{"title":"Unauthorized"}')
        error = httpx.HTTPStatusError("Unauthorized", request=request, response=response)

        with patch.object(client, "_post_json_async", side_effect=error):
            result = await client.create_lead_async(phone="+79990000000", brand="kmipt")

        self.assertFalse(result.success)
        self.assertIn("http error", (result.error or "").lower())
        self.assertIn("401", (result.error or ""))

    def test_amo_client_create_copilot_task_success(self) -> None:
        client = AmoCRMClient(base_url="https://amo.example", access_token="token")
        with patch.object(
            client,
            "_post_json",
            side_effect=[
                {"_embedded": {"leads": [{"id": 54321}]}, "_links": {}},
                {"_embedded": {"notes": [{"id": 999}]}, "_links": {}},
            ],
        ) as post_mock:
            result = client.create_copilot_task(
                summary="Клиент интересуется лагерем",
                draft_reply="Предлагаю 2 смены на выбор.",
                contact="+79990000000",
            )
        self.assertTrue(result.success)
        self.assertEqual(result.entry_id, "54321")
        self.assertEqual(post_mock.call_count, 2)
        self.assertEqual(post_mock.call_args_list[0].args[0], "/leads")
        self.assertEqual(post_mock.call_args_list[1].args[0], "/leads/54321/notes")

    def test_amo_client_create_copilot_task_returns_error_when_not_configured(self) -> None:
        client = AmoCRMClient(base_url="", access_token="")
        result = client.create_copilot_task(summary="sum", draft_reply="draft")
        self.assertFalse(result.success)
        self.assertIn("not configured", (result.error or "").lower())

    def test_amo_client_create_copilot_task_handles_http_error(self) -> None:
        client = AmoCRMClient(base_url="https://amo.example", access_token="token")
        request = httpx.Request("POST", "https://amo.example/api/v4/leads")
        response = httpx.Response(500, request=request, text='{"title":"Server error"}')
        error = httpx.HTTPStatusError("Server error", request=request, response=response)

        with patch.object(client, "_post_json", side_effect=error):
            result = client.create_copilot_task(summary="sum", draft_reply="draft")

        self.assertFalse(result.success)
        self.assertIn("500", (result.error or ""))

    async def test_noop_client_returns_disabled_error(self) -> None:
        client = NoopCRMClient()
        result = await client.create_lead_async(phone="+79990000000", brand="kmipt")
        self.assertFalse(result.success)
        self.assertIn("disabled", (result.error or "").lower())


if __name__ == "__main__":
    unittest.main()
