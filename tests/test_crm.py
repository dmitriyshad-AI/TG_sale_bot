import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from sales_agent.sales_core.config import Settings
from sales_agent.sales_core.crm import AmoCRMClient, NoopCRMClient, TallantoCRMClient, build_crm_client


class _MockHttpxResponse:
    def __init__(self, status_code: int, payload, text: str, request_url: str = "https://amo.example/api/v4/leads") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.request = httpx.Request("POST", request_url)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            response = httpx.Response(self.status_code, request=self.request, text=self.text)
            raise httpx.HTTPStatusError("error", request=self.request, response=response)

    def json(self):
        return self._payload


class _MockAsyncHttpxClient:
    def __init__(self, response: _MockHttpxResponse) -> None:
        self._response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, *args, **kwargs):
        return self._response


class _MockHttpxClient:
    def __init__(self, response: _MockHttpxResponse) -> None:
        self._response = response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, *args, **kwargs):
        return self._response


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
    def test_amo_api_url_keeps_api_v4_prefix(self) -> None:
        client = AmoCRMClient(base_url="https://amo.example/api/v4", access_token="token")
        self.assertEqual(client._api_url("/leads"), "https://amo.example/api/v4/leads")

    def test_amo_extract_entity_id_falls_back_to_top_level_id(self) -> None:
        self.assertEqual(AmoCRMClient._extract_entity_id({"id": 7}, "leads"), "7")

    def test_amo_safe_error_message_for_request_error(self) -> None:
        req = httpx.Request("POST", "https://amo.example/api/v4/leads")
        error = httpx.RequestError("network", request=req)
        message = AmoCRMClient._safe_error_message(error)
        self.assertIn("connection error", message.lower())

    async def test_amo_post_json_async_returns_empty_dict_for_non_object_json(self) -> None:
        client = AmoCRMClient(base_url="https://amo.example", access_token="token")
        response = _MockHttpxResponse(200, [{"id": 1}], text="[]")
        with patch("sales_agent.sales_core.crm.httpx.AsyncClient", return_value=_MockAsyncHttpxClient(response)):
            payload = await client._post_json_async("/leads", [{"name": "x"}])
        self.assertEqual(payload, {})

    def test_amo_post_json_returns_empty_dict_for_non_object_json(self) -> None:
        client = AmoCRMClient(base_url="https://amo.example", access_token="token")
        response = _MockHttpxResponse(200, [{"id": 1}], text="[]")
        with patch("sales_agent.sales_core.crm.httpx.Client", return_value=_MockHttpxClient(response)):
            payload = client._post_json("/leads", [{"name": "x"}])
        self.assertEqual(payload, {})

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

    async def test_amo_client_create_lead_returns_error_when_no_lead_id(self) -> None:
        client = AmoCRMClient(base_url="https://amo.example", access_token="token")
        with patch.object(client, "_post_json_async", return_value={"_embedded": {"leads": [{}]}}):
            result = await client.create_lead_async(phone="+79990000000", brand="kmipt")
        self.assertFalse(result.success)
        self.assertIn("no lead id", (result.error or "").lower())

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

    def test_amo_client_create_copilot_task_returns_error_when_no_lead_id(self) -> None:
        client = AmoCRMClient(base_url="https://amo.example", access_token="token")
        with patch.object(client, "_post_json", return_value={"_embedded": {"leads": [{}]}}):
            result = client.create_copilot_task(summary="sum", draft_reply="draft")
        self.assertFalse(result.success)
        self.assertIn("no lead id", (result.error or "").lower())

    async def test_noop_client_returns_disabled_error(self) -> None:
        client = NoopCRMClient()
        result = await client.create_lead_async(phone="+79990000000", brand="kmipt")
        self.assertFalse(result.success)
        self.assertIn("disabled", (result.error or "").lower())


if __name__ == "__main__":
    unittest.main()
